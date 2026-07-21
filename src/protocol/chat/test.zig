const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Mux = polyrole.family_mux_channel.MultiplexChannel;
const init = @import("init.zig");
const chat = @import("chat.zig");
const push = @import("push.zig");

test "chat: persistent Chat loop" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(3, false, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var board: std.ArrayList(chat.Message) = .empty;
    defer { for (board.items) |m| { allocator.free(m.from); allocator.free(m.text); } board.deinit(allocator); }
    var board_mu: zio.Mutex = .{};

    var ch = try zio.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var chat_buf: [4][]const u8 = @splat(undefined);
            var cc = zio.Channel([]const u8).init(&chat_buf);
            var cctx = chat.ClientContext{ .input_ch = &cc };
            var hc = try zio.spawn(struct {
                fn run(mx2: *M, c2: *chat.ClientContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.client, c2, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &cctx });
            cc.send("hello") catch {};
            cc.close(.graceful);
            hc.join() catch {};
        }
    }.run, .{l.socket.address});

    var sh = try zio.spawn(struct {
        fn run(lsn: *@TypeOf(l), bd: *std.ArrayList(chat.Message), bm: *zio.Mutex) !void {
            const s = try lsn.accept(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var csrv = chat.ServerContext{ .board = bd, .mu = bm, .username = "alice", .gpa = allocator };
            polyrole.runner.Runner(chat.Say).symmetric_run(.server, &csrv, mx.subChannel(1), chat.Say, 2000) catch {};
        }
    }.run, .{ &l, &board, &board_mu });

    sh.join() catch {};
    ch.join() catch {};
    try std.testing.expect(board.items.len >= 1);
}

test "chat: Init + Chat + Push" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(3, false, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var users_mu: zio.Mutex = .{};
    var users = std.StringHashMap(void).init(allocator);
    defer users.deinit();
    var board: std.ArrayList(chat.Message) = .empty;
    defer { for (board.items) |m| { allocator.free(m.from); allocator.free(m.text); } board.deinit(allocator); }
    var board_mu: zio.Mutex = .{};

    var ch = try zio.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();

            var ic = init.ClientContext{ .username = "alice" };
            try polyrole.runner.Runner(init.Send).symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);

            var chat_buf: [4][]const u8 = @splat(undefined);
            var cc = zio.Channel([]const u8).init(&chat_buf);
            var cctx = chat.ClientContext{ .input_ch = &cc };
            var hc = try zio.spawn(struct {
                fn run(mx2: *M, c2: *chat.ClientContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.client, c2, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &cctx });

            var recv: std.ArrayList(push.Message) = .empty;
            defer { for (recv.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv.deinit(allocator); }
            var pctx = push.ClientContext{ .recv = &recv, .gpa = allocator };
            var hp = try zio.spawn(struct {
                fn run(mx2: *M, pc: *push.ClientContext) !void {
                    try polyrole.runner.Runner(push.Push).symmetric_run(.client, pc, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &pctx });

            cc.send("hello") catch {};
            cc.send("world") catch {};
            cc.close(.graceful);
            hc.join() catch {};
            hp.join() catch {};
        }
    }.run, .{l.socket.address});

    var sh = try zio.spawn(struct {
        fn run(lsn: *@TypeOf(l), us: *std.StringHashMap(void), um: *zio.Mutex,
               bd: *std.ArrayList(chat.Message), bm: *zio.Mutex) !void {
            const s = try lsn.accept(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();

            var isrv = init.ServerContext{ .users = us, .mu = um };
            try polyrole.runner.Runner(init.Send).symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);

            var csrv = chat.ServerContext{ .board = bd, .mu = bm, .username = "alice", .gpa = allocator };
            var hc = try zio.spawn(struct {
                fn run(mx2: *M, cs: *chat.ServerContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.server, cs, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &csrv });

            var push_buf: [4]push.Message = @splat(undefined);
            var push_ch = zio.Channel(push.Message).init(&push_buf);
            var psrv = push.ServerContext{ .board_ch = &push_ch };
            var hp = try zio.spawn(struct {
                fn run(mx2: *M, ps: *push.ServerContext) !void {
                    try polyrole.runner.Runner(push.Push).symmetric_run(.server, ps, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &psrv });

            hc.join() catch {};
            for (bd.items) |msg| {
                push_ch.send(.{ .kind = push.KIND_MSG, .from = msg.from, .text = msg.text }) catch break;
            }
            push_ch.close(.graceful);
            hp.join() catch {};
        }
    }.run, .{ &l, &users, &users_mu, &board, &board_mu });

    sh.join() catch {};
    ch.join() catch {};

    try std.testing.expectEqual(@as(usize, 1), users.count());
    try std.testing.expect(board.items.len >= 1);
}
