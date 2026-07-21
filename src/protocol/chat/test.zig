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
    const MUX = M;
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
            var mx: MUX = undefined;
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
            var mx: MUX = undefined;
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

    const Handler = struct {
        fn run(stream: zio.net.Stream, us: *std.StringHashMap(void), um: *zio.Mutex,
               bd: *std.ArrayList(chat.Message), bm: *zio.Mutex) !void {
            var sc: SC = undefined;
            try sc.init(allocator, stream, 4096, 4096);
            defer sc.deinit(allocator);
            var mx: MUX = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var isrv = init.ServerContext{ .users = us, .mu = um };
            try polyrole.runner.Runner(init.Send).symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);
            const username = if (isrv.pending_name.len > 0) isrv.pending_name else "unknown";

            // Persistent push fiber — alive before and after chat
            var push_buf: [8]push.Message = @splat(undefined);
            var push_ch = zio.Channel(push.Message).init(&push_buf);
            var psrv = push.ServerContext{ .board_ch = &push_ch };
            var hp = try zio.spawn(struct {
                fn run(mx2: *MUX, ps: *push.ServerContext) !void {
                    try polyrole.runner.Runner(push.Push).symmetric_run(.server, ps, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &psrv });

            // Short delay so all 3 push fibers start before any chat runs
            try zio.sleep(zio.Duration.fromMilliseconds(50));

            var csrv = chat.ServerContext{ .board = bd, .mu = bm, .username = username, .gpa = allocator };
            var hc = try zio.spawn(struct {
                fn run(mx2: *MUX, cs: *chat.ServerContext) !void {
                    try polyrole.runner.Runner(chat.Say).symmetric_run(.server, cs, mx2.subChannel(1), chat.Say, null);
                }
            }.run, .{ &mx, &csrv });
            hc.join() catch {};

            // Push all board items through the persistent push fiber
            {
                bm.lockUncancelable();
                defer bm.unlock();
                for (bd.items) |msg| {
                    push_ch.send(.{ .kind = push.KIND_MSG, .from = msg.from, .text = msg.text }) catch break;
                }
            }
            push_ch.close(.graceful);
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
                    try polyrole.runner.Runner(push.Push).symmetric_run(.client, pc, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, &pctx });
            cc.send(text) catch {};
            cc.close(.graceful);
            hc.join() catch {};
            hp.join() catch {};
        }
    };

    var recv0: std.ArrayList(push.Message) = .empty;
    defer { for (recv0.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv0.deinit(allocator); }
    var recv1: std.ArrayList(push.Message) = .empty;
    defer { for (recv1.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv1.deinit(allocator); }
    var recv2: std.ArrayList(push.Message) = .empty;
    defer { for (recv2.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv2.deinit(allocator); }

    var c0 = try zio.spawn(Cli.run, .{ l.socket.address, "alice", "hello", &recv0 });
    var c1 = try zio.spawn(Cli.run, .{ l.socket.address, "bob", "hi there", &recv1 });
    var c2 = try zio.spawn(Cli.run, .{ l.socket.address, "charlie", "hey", &recv2 });

    // Accept in main fiber, spawn Handler per connection
    const s0_stream = try l.accept(.{});
    var s0 = try zio.spawn(Handler.run, .{ s0_stream, &users, &users_mu, &board, &board_mu });
    const s1_stream = try l.accept(.{});
    var s1 = try zio.spawn(Handler.run, .{ s1_stream, &users, &users_mu, &board, &board_mu });
    const s2_stream = try l.accept(.{});
    var s2 = try zio.spawn(Handler.run, .{ s2_stream, &users, &users_mu, &board, &board_mu });

    c0.join() catch {}; c1.join() catch {}; c2.join() catch {};
    s0.join() catch {}; s1.join() catch {}; s2.join() catch {};

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), board.items.len);
    try std.testing.expect(recv0.items.len >= 1);
    try std.testing.expect(recv1.items.len >= 1);
    try std.testing.expect(recv2.items.len >= 1);
    try std.testing.expectEqual(push.KIND_MSG, recv0.items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recv1.items[0].kind);
    try std.testing.expectEqual(push.KIND_MSG, recv2.items[0].kind);
}
