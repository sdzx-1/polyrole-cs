const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Mux = polyrole.family_mux_channel.MultiplexChannel;
const init = @import("init.zig");
const chat = @import("chat.zig");
const push = @import("push.zig");

test "chat: three users send and receive" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const SC = polyrole.channel.StreamChannel;
    const MUX = Mux(3, false, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var users_mu: zio.Mutex = .{};
    var users = std.StringHashMap(void).init(allocator);
    defer users.deinit();
    var board: std.ArrayList(chat.Message) = .empty;
    defer { for (board.items) |m| { allocator.free(m.from); allocator.free(m.text); } board.deinit(allocator); }
    var board_mu: zio.Mutex = .{};

    var bc: chat.BroadcastChannel = .{ .subs = .empty, .mu = .{}, .gpa = allocator };
    defer bc.subs.deinit(allocator);

    const Handler = struct {
        fn run(stream: zio.net.Stream, us: *std.StringHashMap(void), um: *zio.Mutex,
               bd: *std.ArrayList(chat.Message), bm: *zio.Mutex,
               brdcst: *chat.BroadcastChannel) !void {
            var sc: SC = undefined;
            try sc.init(allocator, stream, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: MUX = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var isrv = init.ServerContext{ .users = us, .mu = um };
            try polyrole.runner.Runner(init.Send).symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);
            const username = if (isrv.pending_name.len > 0) isrv.pending_name else "unknown";

            var push_buf: [8]chat.BcMsg = @splat(undefined);
            var push_ch = zio.Channel(chat.BcMsg).init(&push_buf);
            brdcst.subscribe(&push_ch);
            errdefer brdcst.unsubscribe(&push_ch);
            var psrv = push.ServerContext{ .board_ch = &push_ch };
            var hp = try zio.spawn(struct {
                fn run(mx2: *MUX, ps: *push.ServerContext) !void {
                    try polyrole.runner.Runner(push.Push).symmetric_run(.server, ps, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &psrv });

            try zio.sleep(zio.Duration.fromMilliseconds(50));

            var csrv = chat.ServerContext{ .board = bd, .mu = bm, .username = username, .gpa = allocator, .bc = brdcst };
            var hc = try zio.spawn(struct {
                fn run(mx2: *MUX, cs: *chat.ServerContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.server, cs, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &csrv });
            hc.join() catch {};

            push_ch.close(.graceful);
            hp.join() catch {};
            brdcst.unsubscribe(&push_ch);
        }
    };

    const Cli = struct {
        fn run(a: zio.net.Address, name: []const u8, text: []const u8, out_recv: *std.ArrayList(push.Message)) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: MUX = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var init_buf: [4][]const u8 = @splat(undefined);
            var init_ch = zio.Channel([]const u8).init(&init_buf);
            try init_ch.send(name);
            var ic = init.ClientContext{ .input_ch = &init_ch };
            try polyrole.runner.Runner(init.Send).symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            var chat_buf: [4][]const u8 = @splat(undefined);
            var cc = zio.Channel([]const u8).init(&chat_buf);
            var cctx = chat.ClientContext{ .input_ch = &cc };
            var hc = try zio.spawn(struct {
                fn run(mx2: *MUX, c2: *chat.ClientContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.client, c2, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &cctx });
            var pctx = push.ClientContext{ .recv = out_recv, .gpa = allocator };
            var hp = try zio.spawn(struct {
                fn run(mx2: *MUX, pc: *push.ClientContext) !void {
                    try polyrole.runner.Runner(push.Push).symmetric_run(.client, pc, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &pctx });
            cc.send(text) catch {};
            cc.close(.graceful);
            hc.join() catch {};
            hp.join() catch {};
        }
    };

    var recvs: [3]std.ArrayList(push.Message) = @splat(.empty);
    defer for (&recvs) |*r| {
        for (r.items) |m| { allocator.free(m.from); allocator.free(m.text); }
        r.deinit(allocator);
    };

    const names = [_][]const u8{ "alice", "bob", "charlie" };
    const texts = [_][]const u8{ "hello", "hi there", "hey" };

    var chs: [3]@TypeOf(try zio.spawn(Cli.run, .{ l.socket.address, "", "", &recvs[0] })) = undefined;
    for (names, texts, &recvs, &chs) |name, text, *recv, *h| {
        h.* = try zio.spawn(Cli.run, .{ l.socket.address, name, text, recv });
    }

    var sg: zio.Group = .init;
    defer sg.cancel();
    for (0..3) |_| {
        const stream = try l.accept(.{});
        try sg.spawn(Handler.run, .{ stream, &users, &users_mu, &board, &board_mu, &bc });
    }

    for (&chs) |*h| h.join() catch {};

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), board.items.len);
    try std.testing.expectEqual(@as(usize, 3), recvs[0].items.len);
    try std.testing.expectEqual(@as(usize, 3), recvs[1].items.len);
    try std.testing.expectEqual(@as(usize, 3), recvs[2].items.len);
    try std.testing.expectEqual(push.KIND_MSG, recvs[0].items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recvs[1].items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recvs[2].items[0].kind);
}
