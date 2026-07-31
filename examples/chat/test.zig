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
    .{ .capacity = 16, .max_message_size = 4096, .overflow = .backpressure },
}, 4100);

const CtrlRunner = polyrole.runner.Runner(chat.Login);
const PushRunner = polyrole.runner.Runner(chat.Poll);

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

test "room: register/remove/broadcast（消息板）" {
    var board: chat.SharedBoard = undefined;
    board.init(allocator, 1024);
    defer board.deinit(allocator);

    var room: chat.Room = undefined;
    room.init(allocator, &board);
    defer room.deinit();

    // A 注册 → 板追加 "alice 加入了房间"
    var ra_buf: [1]chat.WelcomePayload = undefined;
    var ra: zio.Channel(chat.WelcomePayload) = .init(&ra_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("alice"), .reply = &ra } });
    room.drain();
    const wa = try ra.receive();
    try testing.expectEqual(@as(u32, 0), wa.client_id);
    try testing.expectEqual(@as(u32, 0), wa.member_count);
    try testing.expectEqual(@as(usize, 1), board.len());

    // B 注册 → 板追加 "bob 加入了房间"；B 的 Welcome 带在线人数 1
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .reply = &rb } });
    room.drain();
    const wb = try rb.receive();
    try testing.expectEqual(@as(u32, 1), wb.client_id);
    try testing.expectEqual(@as(u32, 1), wb.member_count);
    try testing.expectEqual(@as(usize, 2), board.len());
    const sys_join = board.slice(1, 2)[0];
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.system)), sys_join.kind);
    try testing.expectEqualStrings("bob 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);

    // /who：A 查询成员列表 → count=2，名单含 bob（排除自己）
    var rw_buf: [1]chat.MemberListReply = undefined;
    var rw: zio.Channel(chat.MemberListReply) = .init(&rw_buf);
    try room.ops.send(.{ .who = .{ .client_id = 0, .reply = &rw } });
    room.drain();
    const wl = try rw.receive();
    try testing.expectEqual(@as(u32, 2), wl.count);
    try testing.expectEqualStrings("bob", wl.names[0][0..chat.cstrLen(&wl.names[0])]);
    try testing.expect(!wl.truncated);

    // 广播：A 发消息 → 板追加 chat 消息
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
    try testing.expectEqual(@as(usize, 3), board.len());
    const msg = board.slice(2, 3)[0];
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);

    // B 移除 → 板追加 "bob 离开了房间"；房间成员数归 1
    var rr_buf: [1]void = undefined;
    var rr: zio.Channel(void) = .init(&rr_buf);
    try room.ops.send(.{ .remove = .{ .client_id = 1, .reply = &rr } });
    room.drain();
    _ = try rr.receive();
    try testing.expectEqual(@as(usize, 4), board.len());
    const sys_leave = board.slice(3, 4)[0];
    try testing.expectEqualStrings("bob 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
    try testing.expectEqual(@as(usize, 1), room.count);

    // 槽位复用：B 移除后其 client_id 回到空闲列表，C 注册应复用它
    var rc_buf: [1]chat.WelcomePayload = undefined;
    var rc: zio.Channel(chat.WelcomePayload) = .init(&rc_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("carol"), .reply = &rc } });
    room.drain();
    const wc = try rc.receive();
    try testing.expectEqual(@as(u32, 1), wc.client_id); // 复用 bob 的槽位
    try testing.expectEqual(@as(usize, 2), room.count);
}

test "room: notify_joins 关闭时无加入/离开通知" {
    var board: chat.SharedBoard = undefined;
    board.init(allocator, 1024);
    defer board.deinit(allocator);

    var room: chat.Room = undefined;
    room.init(allocator, &board);
    room.notify_joins = false; // 静默注册（大群/压测）
    defer room.deinit();

    var ra_buf: [1]chat.WelcomePayload = undefined;
    var ra: zio.Channel(chat.WelcomePayload) = .init(&ra_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("alice"), .reply = &ra } });
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .reply = &rb } });
    var rr_buf: [1]void = undefined;
    var rr: zio.Channel(void) = .init(&rr_buf);
    try room.ops.send(.{ .remove = .{ .client_id = 0, .reply = &rr } });
    room.drain();
    _ = try ra.receive();
    _ = try rb.receive();
    _ = try rr.receive();
    // 板只有广播内容：注册/离开均不追加通知
    try testing.expectEqual(@as(usize, 0), board.len());
    try testing.expectEqual(@as(usize, 1), room.count); // alice 移除后只剩 bob
}

// ─── 2. Ctrl simulate（Room fiber 并行处理 ops） ──────────────────────

test "ctrl: simulate 注册→欢迎→消息→退出" {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var board: chat.SharedBoard = undefined;
    board.init(allocator, 1024);
    defer board.deinit(allocator);

    var room: chat.Room = undefined;
    room.init(allocator, &board);
    defer room.deinit();
    try room.spawn();
    defer room.stop();

    // 预注册 bob（服务器端代表另一条连接）→ 板追加 "bob 加入了房间"
    var rb_buf: [1]chat.WelcomePayload = undefined;
    var rb: zio.Channel(chat.WelcomePayload) = .init(&rb_buf);
    try room.ops.send(.{ .register = .{ .nickname = makeNick("bob"), .reply = &rb } });
    const wb = try rb.receive();
    try testing.expectEqual(@as(u32, 0), wb.member_count);
    try testing.expectEqual(@as(usize, 1), board.len());

    // 客户端：预置一条消息 + /who + 退出
    var input_buf: [8]chat.UserInput = undefined;
    var input: zio.Channel(chat.UserInput) = .init(&input_buf);
    try input.send(.{ .msg = makeText("hello") });
    try input.send(.who);
    try input.send(.quit);

    var client_ctx = chat.ClientContext.init(null, makeNick("alice"), &input);
    var server_ctx = chat.ServerContext.init(&room);

    try CtrlRunner.simulate(&client_ctx, &server_ctx, chat.Login);

    // 客户端上下文：欢迎数据正确（bob 在线）
    try testing.expectEqual(@as(u32, 1), client_ctx.client_id);
    try testing.expectEqual(@as(u32, 1), client_ctx.member_count);
    // 服务器端 seq 递增
    try testing.expectEqual(@as(u64, 1), client_ctx.seq);

    // 消息板：bob 加入 → alice 加入 → chat → /who → alice 离开（共 5 条）
    try testing.expectEqual(@as(usize, 5), board.len());
    const sys_join = board.slice(1, 2)[0];
    try testing.expectEqualStrings("alice 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);
    const msg = board.slice(2, 3)[0];
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);
    try testing.expectEqual(@as(u64, 1), msg.seq);
    const who_resp = board.slice(3, 4)[0];
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.member_list)), who_resp.kind);
    try testing.expectEqualStrings("在线 2 人：bob", who_resp.text[0..chat.cstrLen(&who_resp.text)]);
    const sys_leave = board.slice(4, 5)[0];
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
            PushRunner.symmetric_run(.client, ctx, ch, chat.Poll, null) catch {};
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

    var board: chat.SharedBoard = undefined;
    board.init(allocator, 1024);
    defer board.deinit(allocator);

    var room: chat.Room = undefined;
    room.init(allocator, &board);
    defer room.deinit();
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

    // 1. A 的收件箱：自己的加入通知 → "bob 加入了房间"（board 模式含自己，顺序按注册）
    const self_join = try a_inbox.receive();
    try testing.expectEqualStrings("alice 加入了房间", self_join.text[0..chat.cstrLen(&self_join.text)]);
    const sys_join = try a_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.system)), sys_join.kind);
    try testing.expectEqualStrings("bob 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);

    // 1.5 B 也消费注册期的历史（B 的 Push 从游标 0 拉，含 A 的加入通知）
    _ = try b_inbox.receive(); // alice 加入了房间
    _ = try b_inbox.receive(); // bob 加入了房间

    // 2. A 发消息 → B 收到；A（board 模式）也收到自己的消息
    try a_input.send(.{ .msg = makeText("hello") });
    const msg = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), msg.kind);
    try testing.expectEqualStrings("alice", msg.from_name[0..chat.cstrLen(&msg.from_name)]);
    try testing.expectEqualStrings("hello", msg.text[0..chat.cstrLen(&msg.text)]);
    try testing.expectEqual(@as(u32, 0), msg.from_id); // alice 是第一个注册的
    const self_msg = try a_inbox.receive(); // 发送者也会经板拉回自己的消息
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.chat)), self_msg.kind);
    try testing.expectEqualStrings("hello", self_msg.text[0..chat.cstrLen(&self_msg.text)]);

    // 2.5 B 发 /who → 收到成员列表（kind=member_list，含 alice）
    try b_input.send(.who);
    const who_resp = try b_inbox.receive();
    try testing.expectEqual(@as(u8, @intFromEnum(chat.PushKind.member_list)), who_resp.kind);
    try testing.expectEqualStrings("在线 2 人：alice", who_resp.text[0..chat.cstrLen(&who_resp.text)]);

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

    var board: chat.SharedBoard = undefined;
    board.init(allocator, 1024);
    defer board.deinit(allocator);

    var room: chat.Room = undefined;
    room.init(allocator, &board);
    defer room.deinit();
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

    // B 先收到自己的加入通知，再收到 ccc 加入/离开
    _ = try b_inbox.receive(); // "bob 加入了房间"
    const sys_join = try b_inbox.receive();
    try testing.expectEqualStrings("ccc 加入了房间", sys_join.text[0..chat.cstrLen(&sys_join.text)]);
    const sys_leave = try b_inbox.receive();
    try testing.expectEqualStrings("ccc 离开了房间", sys_leave.text[0..chat.cstrLen(&sys_leave.text)]);
}
