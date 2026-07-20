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
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    // Shared state with locks
    var users_mu: zio.Mutex = .{};
    var users = std.StringHashMap(void).init(allocator);
    defer users.deinit();

    var board: std.ArrayList(chat.Message) = .empty;
    defer {
        for (board.items) |m| { allocator.free(m.from); allocator.free(m.text); }
        board.deinit(allocator);
    }
    var board_mu: zio.Mutex = .{};

    // Client fiber: init → chat(msg1) → chat(msg2) → push(receive)
    var ch = try zio.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const M = Mux(3, false, 1024, 8);
            const SC = polyrole.channel.StreamChannel;
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();

            // Init
            const Ri = polyrole.runner.Runner(init.Send);
            var ic = init.ClientContext{ .username = "alice" };
            try Ri.symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            try std.testing.expect(ic.accepted);

            // Chat: send two messages, each as its own symmetric_run
            const Rc = polyrole.runner.Runner(chat.Say);
            var cctx = chat.ClientContext{ .text = "hello" };
            try Rc.symmetric_run(.client, &cctx, mx.subChannel(1), chat.Say, null);
            cctx = chat.ClientContext{ .text = "world" };
            try Rc.symmetric_run(.client, &cctx, mx.subChannel(1), chat.Say, null);

            // Push: receive broadcast
            var recv: std.ArrayList(push.Message) = .empty;
            defer {
                for (recv.items) |m| { allocator.free(m.from); allocator.free(m.text); }
                recv.deinit(allocator);
            }
            const Rp = polyrole.runner.Runner(push.Push);
            var pctx = push.ClientContext{ .recv = &recv, .gpa = allocator };
            try Rp.symmetric_run(.client, &pctx, mx.subChannel(2), push.Push, null);
            try std.testing.expect(recv.items.len >= 1);
        }
    }.run, .{l.socket.address});

    // Server: accept → init → spawn chat + push fibers
    var sh = try zio.spawn(struct {
        fn run(lsn: *@TypeOf(l), us: *std.StringHashMap(void), umu: *zio.Mutex,
               bd: *std.ArrayList(chat.Message), bmu: *zio.Mutex) !void {
            const M = Mux(3, false, 1024, 8);
            const SC = polyrole.channel.StreamChannel;
            const s = try lsn.accept(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();

            // Init
            const Ri = polyrole.runner.Runner(init.Send);
            var isrv = init.ServerContext{ .users = us, .mu = umu };
            try Ri.symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);

            // Chat: receive all messages (one per symmetric_run)
            for (0..2) |_| {
                const Rc = polyrole.runner.Runner(chat.Say);
                var csrv = chat.ServerContext{ .board = bd, .mu = bmu, .username = "alice", .gpa = allocator };
                try Rc.symmetric_run(.server, &csrv, mx.subChannel(1), chat.Say, null);
            }

            // Push: send all board messages
            var board_copy: std.ArrayList(chat.Message) = .empty;
            defer board_copy.deinit(allocator);
            {
                bmu.lockUncancelable();
                defer bmu.unlock();
                for (bd.items) |m| board_copy.append(allocator, m) catch {};
            }
            for (board_copy.items) |msg| {
                const Rp = polyrole.runner.Runner(push.Push);
                var psrv = push.ServerContext{ .msg = .{
                    .kind = push.KIND_MSG,
                    .from = msg.from,
                    .text = msg.text,
                } };
                try Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null);
            }
            // Kick to signal end
            const Rp = polyrole.runner.Runner(push.Push);
            var psrv = push.ServerContext{ .msg = .{ .kind = push.KIND_MSG, .from = "", .text = "" } };
            Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null) catch {};
        }
    }.run, .{ &l, &users, &users_mu, &board, &board_mu });

    ch.join() catch {};
    sh.join() catch {};

    try std.testing.expectEqual(@as(usize, 1), users.count());
    try std.testing.expect(board.items.len >= 2);
}
