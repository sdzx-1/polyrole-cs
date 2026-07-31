// 聊天室 demo 测试
//
// 覆盖：
//   1. Room 成员表纯逻辑（register/remove/broadcast，同步 drain）
//   2. Ctrl 协议 simulate（注册 → 欢迎 → 消息广播 → 退出广播）
//   3. 双客户端网络集成（欢迎/成员表/消息/加入离开通知/quit）
//   4. 客户端断开后服务器清理并广播离开通知
//
// 运行：zig build test

const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const chat = @import("protocol.zig");
const server_mod = @import("server.zig");

const Mux = polyrole.family_mux_channel.MultiplexChannel(&.{
    .{ .capacity = 1, .max_message_size = 4096, .overflow = .close_channel },
    .{ .capacity = 16, .max_message_size = 512, .overflow = .backpressure },
}, 4100);

const CtrlRunner = polyrole.runner.Runner(chat.Login);
const PushRunner = polyrole.runner.Runner(chat.Deliver);

const testing = std.testing;
const allocator = testing.allocator;

fn makeText(s: []const u8) [chat.MAX_TEXT]u8 {
    var t = [_]u8{0} ** chat.MAX_TEXT;
    @memcpy(t[0..@min(s.len, chat.MAX_TEXT - 1)], s[0..@min(s.len, chat.MAX_TEXT - 1)]);
    return t;
}

fn makeNick(s: []const u8) [chat.MAX_NICK]u8 {
    var t = [_]u8{0} ** chat.MAX_NICK;
    @memcpy(t[0..@min(s.len, chat.MAX_NICK - 1)], s[0..@min(s.len, chat.MAX_NICK - 1)]);
    return t;
}

// ─── 1. Room 纯逻辑（同步 drain，无需 runtime） ──────────────────────

test "room: register/remove/broadcast" {
    var room: chat.Room = undefined;
    room.init();

    var a_buf: [4]chat.PushPayload = undefined;
    var a_inbox: zio.Channel(chat.PushPayload) = .init(&a_buf);
    var b_buf: [4]chat.PushPayload = undefined;
    var b_inbox: zio.Channel(chat.PushPayload) = .init(&b_buf);

    // A 注册
    var ra_buf: [1]chat.WelcomePayload = undefined;
    var ra: zio.Channel(chat.WelcomePayload) = .init(&ra_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("alice"), .inbox = &a_inbox, .reply = &ra } });
    room.drain();
    const wa = try ra.receive();
    try testing.expectEqual(@as(u32, 0), wa.client_id);
    try testing.expectEqual(@as(u8, 0), wa.member_count);

    // B 注册：B 的成员表含 A；A 收到"bob 加入了房间"
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .inbox = &b_inbox, .reply = &rb } });
    room.drain();
    const wb = try rb.receive();
    try testing.expectEqual(@as(u32, 1), wb.client_id);
    try testing.expectEqual(@as(u8, 1), wb.member_count);
    try testing.expectEqualStrings("alice", wb.members[0][0..chat.cstrLen(&wb.members[0])]);

    const sys_join = try a_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.system)), sys_join.kind);
    try testing.expectEqualStrings("bob 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);

    // 广播：A 发消息给房间，B 收到；A 收不到自己的消息
    try room.ops.send(.{ .broadcast = .{
        .from_id = 0,
        .payload = .{
            .kind = @intFromEnum(chat.PushKind.chat),
            .seq = 1,
            .from_id = 0,
            .from_name = makeNick("alice"),
            .text = makeText("hello"),
            .ts_ms = 0,
        },
    } });
    room.drain();
    const msg = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);
    try testing.expectError(error.ChannelEmpty, a_inbox.tryReceive());

    // B 移除：A 收到"bob 离开了房间"，房间成员数归 1
    var rr_buf: [1]void = undefined;
    var rr: zio.Channel(void) = .init(&rr_buf);
    try room.ops.send(.{ .remove = .{ .client_id = 1, .reply = &rr } });
    room.drain();
    _ = try rr.receive();
    const sys_leave = try a_inbox.receive();
    try testing.expectEqualStrings("bob 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
    try testing.expectEqual(@as(u8, 1), room.count);
}

test "room: 慢消费者被断开" {
    var room: chat.Room = undefined;
    room.init();

    var a_buf: [1]chat.PushPayload = undefined;
    var a_inbox: zio.Channel(chat.PushPayload) = .init(&a_buf); // 容量 1
    var b_buf: [8]chat.PushPayload = undefined;
    var b_inbox: zio.Channel(chat.PushPayload) = .init(&b_buf);

    // A、B 注册
    var ra_buf: [1]chat.WelcomePayload = undefined;
    var ra: zio.Channel(chat.WelcomePayload) = .init(&ra_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("alice"), .inbox = &a_inbox, .reply = &ra } });
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .inbox = &b_inbox, .reply = &rb } });
    room.drain();
    _ = try ra.receive();
    _ = try rb.receive();
    _ = try a_inbox.receive(); // 消费"bob 加入了房间"

    // 向房间广播 3 条：A 的容量 1，第 2 条时 A 被断开
    const payload = chat.PushPayload{
        .kind = @intFromEnum(chat.PushKind.chat),
        .seq = 1,
        .from_id = 0,
        .from_name = makeNick("alice"),
        .text = makeText("x"),
        .ts_ms = 0,
    };
    for (0..3) |_| {
        try room.ops.send(.{ .broadcast = .{ .from_id = 99, .payload = payload } });
        room.drain();
    }
    // A 被断开：room 只剩 B；A 的 inbox 先排空缓冲的 1 条再返回 ChannelClosed
    try testing.expectEqual(@as(u8, 1), room.count);
    _ = try a_inbox.receive();
    try testing.expectError(error.ChannelClosed, a_inbox.receive());
    // B：chat(第1次广播) → sys(A断开) → chat(第2次) → chat(第3次)
    _ = try b_inbox.receive();
    const sys = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.system)), sys.kind);
    try testing.expectEqualStrings("alice 因接收过慢被断开", sys.text[0..chat.cstrLen(&sys.text)]);
    _ = try b_inbox.receive();
    _ = try b_inbox.receive();
    try testing.expectError(error.ChannelEmpty, b_inbox.tryReceive());
}

// ─── 2. Ctrl simulate（Room fiber 并行处理 ops） ──────────────────────

test "ctrl: simulate 注册→欢迎→消息→退出" {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var room: chat.Room = undefined;
    room.init();
    try room.spawn();
    defer room.stop();

    // 预注册 bob（服务器端代表另一条连接）
    var b_buf: [8]chat.PushPayload = undefined;
    var b_inbox: zio.Channel(chat.PushPayload) = .init(&b_buf);
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .inbox = &b_inbox, .reply = &rb } });
    const wb = try rb.receive();
    try testing.expectEqual(@as(u8, 0), wb.member_count);

    // 客户端：预置一条消息 + 退出
    var input_buf: [8]chat.UserInput = undefined;
    var input: zio.Channel(chat.UserInput) = .init(&input_buf);
    try input.send(.{ .msg = makeText("hello") });
    try input.send(.quit);

    var a_inbox_buf: [8]chat.PushPayload = undefined;
    var a_inbox: zio.Channel(chat.PushPayload) = .init(&a_inbox_buf);

    var client_ctx = chat.ClientContext.init(null, makeNick("alice"), &input);
    var server_ctx = chat.ServerContext.init(&room, &a_inbox);

    try CtrlRunner.simulate(&client_ctx, &server_ctx, chat.Login);

    // 客户端上下文：欢迎数据正确（bob 在线）
    try testing.expectEqual(@as(u32, 1), client_ctx.client_id);
    try testing.expectEqual(@as(u8, 1), client_ctx.member_count);
    try testing.expectEqualStrings("bob", client_ctx.members[0][0..chat.cstrLen(&client_ctx.members[0])]);
    // 服务器端 seq 递增
    try testing.expectEqual(@as(u64, 1), client_ctx.seq);

    // bob 的收件箱：alice 加入 → chat 消息 → alice 离开
    const sys_join = try b_inbox.receive();
    try testing.expectEqualStrings("alice 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);
    const msg = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);
    try testing.expectEqual(@as(u64, 1), msg.seq);
    const sys_leave = try b_inbox.receive();
    try testing.expectEqualStrings("alice 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
}

// ─── 3/4. 网络集成 ──────────────────────────────────────────────────

const ServerFn = struct {
    /// 接受 n 个连接并并发 serve（serveConnection 阻塞在各自的 Ctrl 循环，
    /// 串行 accept+serve 会饿死后面的连接）。
    fn run(ls: zio.net.Server, r: *chat.Room, n: usize) anyerror!void {
        var group: zio.Group = .init;
        defer group.cancel();
        for (0..n) |_| {
            const stream = try ls.accept(.{});
            try group.spawn(server_mod.serveConnection, .{ allocator, stream, r, 2000 });
        }
        try group.wait();
    }
};

/// 完整客户端：Ctrl 主循环 + Push 消费（输出到收件箱）。
fn clientRun(
    addr: zio.net.Address,
    nick: []const u8,
    input: *zio.Channel(chat.UserInput),
    push_inbox: *zio.Channel(chat.PushPayload),
) anyerror!void {
    var stream = try addr.connect(.{});
    defer stream.close();

    var sc: polyrole.channel.StreamChannel = undefined;
    try sc.init(allocator, stream, 4096, 4096, 4096);
    defer sc.deinit(allocator);

    var mux: Mux = undefined;
    try mux.initFromChannel(allocator, &sc);
    defer mux.deinit();

    var ctrl_ctx = chat.ClientContext.init(null, makeNick(nick), input);
    var push_ctx = chat.PushClientContext{ .inbox = push_inbox };

    const PushFn = struct {
        fn run(ctx: *chat.PushClientContext, ch: *Mux.SubChannel) anyerror!void {
            PushRunner.symmetric_run(.client, ctx, ch, chat.Deliver, null) catch {};
        }
    };
    var push_h = try zio.spawn(PushFn.run, .{ &push_ctx, mux.subChannel(1) });

    CtrlRunner.symmetric_run(.client, &ctrl_ctx, mux.subChannel(0), chat.Login, 5000) catch {};

    push_h.cancel();
    push_h.join() catch {};
}

/// 流氓客户端：手工发一条 Register，收到 Welcome 后立即断开（模拟崩溃）。
fn rogueRun(addr: zio.net.Address) anyerror!void {
    const stream = try addr.connect(.{});

    var sc: polyrole.channel.StreamChannel = undefined;
    try sc.init(allocator, stream, 4096, 4096, 4096);

    var mux: Mux = undefined;
    try mux.initFromChannel(allocator, &sc);

    const ch = mux.subChannel(0);
    try ch.send(
        CtrlRunner.idFromState(chat.Login),
        chat.Login,
        @as(chat.Login, .{ .register = .{ .data = .{ .nickname = makeNick("ccc") } } }),
    );
    // 等服务器确认注册（Welcome 到达）再断开
    _ = try ch.recv(CtrlRunner.idFromState(chat.Welcome), chat.Welcome);

    // Mux owns_stream：deinit 会关闭底层 stream；必须先关 Mux 再释放 sc 缓冲。
    mux.deinit();
    sc.deinit(allocator);
}

test "chat: 双客户端集成（欢迎/消息/加入离开/退出）" {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    const server_addr = listener.socket.address;

    var room: chat.Room = undefined;
    room.init();
    try room.spawn();
    defer room.stop();

    var server_group: zio.Group = .init;
    defer server_group.cancel();
    try server_group.spawn(ServerFn.run, .{ listener, &room, 2 });

    var a_input_buf: [16]chat.UserInput = undefined;
    var a_input: zio.Channel(chat.UserInput) = .init(&a_input_buf);
    var a_push_buf: [16]chat.PushPayload = undefined;
    var a_inbox: zio.Channel(chat.PushPayload) = .init(&a_push_buf);
    var b_input_buf: [16]chat.UserInput = undefined;
    var b_input: zio.Channel(chat.UserInput) = .init(&b_input_buf);
    var b_push_buf: [16]chat.PushPayload = undefined;
    var b_inbox: zio.Channel(chat.PushPayload) = .init(&b_push_buf);

    var client_group: zio.Group = .init;
    defer client_group.cancel();
    try client_group.spawn(clientRun, .{ server_addr, "alice", &a_input, &a_inbox });
    try client_group.spawn(clientRun, .{ server_addr, "bob", &b_input, &b_inbox });

    // 1. A 收到"bob 加入了房间" → 说明 A、B 都已注册完成
    const sys_join = try a_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.system)), sys_join.kind);
    try testing.expectEqualStrings("bob 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);

    // 2. A 发消息 → B 收到；A 收不到自己的消息
    try a_input.send(.{ .msg = makeText("hello") });
    const msg = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("alice", msg.from_name[0..chat.cstrLen(&msg.from_name)]);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);
    try testing.expectEqual(@as(u32, 0), msg.from_id); // alice 是第一个注册的
    try testing.expectError(error.ChannelEmpty, a_inbox.tryReceive());

    // 3. A 退出 → B 收到离开通知；A 的 clientRun 正常返回
    try a_input.send(.quit);
    const sys_leave = try b_inbox.receive();
    try testing.expectEqualStrings("alice 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
}

test "chat: 客户端断开后广播离开通知" {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    const server_addr = listener.socket.address;

    var room: chat.Room = undefined;
    room.init();
    try room.spawn();
    defer room.stop();

    var server_group: zio.Group = .init;
    defer server_group.cancel();
    try server_group.spawn(ServerFn.run, .{ listener, &room, 2 });

    var b_input_buf: [16]chat.UserInput = undefined;
    var b_input: zio.Channel(chat.UserInput) = .init(&b_input_buf);
    var b_push_buf: [16]chat.PushPayload = undefined;
    var b_inbox: zio.Channel(chat.PushPayload) = .init(&b_push_buf);

    var client_group: zio.Group = .init;
    defer client_group.cancel();
    // 先让 B 完成注册
    try client_group.spawn(clientRun, .{ server_addr, "bob", &b_input, &b_inbox });
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    // C 注册后立即断开 → 服务器清理并广播
    try client_group.spawn(rogueRun, .{ server_addr });

    const sys_join = try b_inbox.receive();
    try testing.expectEqualStrings("ccc 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);
    const sys_leave = try b_inbox.receive();
    try testing.expectEqualStrings("ccc 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
}
