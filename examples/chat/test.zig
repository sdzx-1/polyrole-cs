const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
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
    var board = push.SharedBoard.init(allocator, 1024);
    defer {
        for (board.items.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        board.deinit(allocator);
    }

    const Handler = struct {
        fn run(stream: zio.net.Stream, us: *std.StringHashMap(void), um: *zio.Mutex,
               bd: *push.SharedBoard) !void {
            var sc: SC = undefined;
            try sc.init(allocator, stream, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: MUX = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var isrv = init.ServerContext{ .users = us, .mu = um };
            try polyrole.runner.Runner(init.Send).symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);
            const username = if (isrv.pending_name.len > 0) isrv.pending_name else "unknown";

            var psrv = push.ServerContext{ .board = bd, .poll_ms = 10 };
            var hp = try zio.spawn(struct {
                fn run(mx2: *MUX, ps: *push.ServerContext) !void {
                    try polyrole.runner.Runner(push.Sync).symmetric_run(.server, ps, mx2.subChannel(2), push.Sync, null);
                }
            }.run, .{ &mx, &psrv });

            var csrv = chat.ServerContext{ .gpa = allocator, .board = bd, .username = username };
            var hc = try zio.spawn(struct {
                fn run(mx2: *MUX, cs: *chat.ServerContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.server, cs, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &csrv });
            hc.join() catch {};

            psrv.kick = true;
            hp.join() catch {};
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
                    try polyrole.runner.Runner(push.Sync).symmetric_run(.client, pc, mx2.subChannel(2), push.Sync, null);
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
        for (r.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        r.deinit(allocator);
    };

    const names = [_][]const u8{ "alice", "bob", "charlie" };
    const texts = [_][]const u8{ "hello", "hi there", "hey" };

    var chs: [3]@TypeOf(try zio.spawn(Cli.run, .{ l.socket.address, "", "", &recvs[0] })) = undefined;
    for (names, texts, &recvs, &chs) |name, text, *recv, *h| {
        h.* = try zio.spawn(Cli.run, .{ l.socket.address, name, text, recv });
    }

    const stream0 = try l.accept(.{});
    const stream1 = try l.accept(.{});
    const stream2 = try l.accept(.{});
    var hd0 = try zio.spawn(Handler.run, .{ stream0, &users, &users_mu, &board });
    var hd1 = try zio.spawn(Handler.run, .{ stream1, &users, &users_mu, &board });
    var hd2 = try zio.spawn(Handler.run, .{ stream2, &users, &users_mu, &board });

    for (&chs) |*h| h.join() catch {};
    hd0.join() catch {};
    hd1.join() catch {};
    hd2.join() catch {};

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), board.committed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 3), recvs[0].items.len);
    try std.testing.expectEqual(@as(usize, 3), recvs[1].items.len);
    try std.testing.expectEqual(@as(usize, 3), recvs[2].items.len);
    try std.testing.expectEqual(push.KIND_MSG, recvs[0].items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recvs[1].items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recvs[2].items[0].kind);
}
