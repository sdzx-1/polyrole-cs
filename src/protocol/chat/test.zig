const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Mux = polyrole.family_mux_channel.MultiplexChannel;
const init = @import("init.zig");
const chat_mod = @import("chat.zig");
const push = @import("push.zig");

test "chat: three users send and receive" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(3, false, 1024, 8);

    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var users = std.StringHashMap(void).init(allocator);
    defer users.deinit();
    var all_msgs: std.ArrayList(chat_mod.Message) = .empty;
    defer {
        for (all_msgs.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        all_msgs.deinit(allocator);
    }
    var recv0: std.ArrayList(push.Message) = .empty;
    defer {
        for (recv0.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        recv0.deinit(allocator);
    }
    var recv1: std.ArrayList(push.Message) = .empty;
    defer {
        for (recv1.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        recv1.deinit(allocator);
    }
    var recv2: std.ArrayList(push.Message) = .empty;
    defer {
        for (recv2.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        recv2.deinit(allocator);
    }

    const names = [_][]const u8{ "alice", "bob", "charlie" };
    const msgs = [_][]const u8{ "hello", "hi there", "hey" };
    const recvs = [_]*std.ArrayList(push.Message){ &recv0, &recv1, &recv2 };

    // ── Client fibers: each does connect → init → chat → push ──────────
    var ch0 = try zio.spawn(struct {
        fn run(
            n: []const u8,
            m_: []const u8,
            a: zio.net.Address,
            r: *std.ArrayList(push.Message),
        ) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            const Ri = polyrole.runner.Runner(init.Send);
            var ic = init.ClientContext{ .username = n };
            try Ri.symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            const Rc = polyrole.runner.Runner(chat_mod.Say);
            var cc = chat_mod.ClientContext{ .pending_text = m_ };
            try Rc.symmetric_run(.client, &cc, mx.subChannel(1), chat_mod.Say, null);
            const Rp = polyrole.runner.Runner(push.Push);
            var pc = push.ClientContext{ .received = r, .gpa = allocator };
            try Rp.symmetric_run(.client, &pc, mx.subChannel(2), push.Push, null);
        }
    }.run, .{ names[0], msgs[0], l.socket.address, recvs[0] });

    var ch1 = try zio.spawn(struct {
        fn run(n: []const u8, m_: []const u8, a: zio.net.Address, r: *std.ArrayList(push.Message)) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var ic = init.ClientContext{ .username = n };
            try polyrole.runner.Runner(init.Send).symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            var cc = chat_mod.ClientContext{ .pending_text = m_ };
            try polyrole.runner.Runner(chat_mod.Say).symmetric_run(.client, &cc, mx.subChannel(1), chat_mod.Say, null);
            var pc = push.ClientContext{ .received = r, .gpa = allocator };
            try polyrole.runner.Runner(push.Push).symmetric_run(.client, &pc, mx.subChannel(2), push.Push, null);
        }
    }.run, .{ names[1], msgs[1], l.socket.address, recvs[1] });

    var ch2 = try zio.spawn(struct {
        fn run(n: []const u8, m_: []const u8, a: zio.net.Address, r: *std.ArrayList(push.Message)) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();
            var ic = init.ClientContext{ .username = n };
            try polyrole.runner.Runner(init.Send).symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            var cc = chat_mod.ClientContext{ .pending_text = m_ };
            try polyrole.runner.Runner(chat_mod.Say).symmetric_run(.client, &cc, mx.subChannel(1), chat_mod.Say, null);
            var pc = push.ClientContext{ .received = r, .gpa = allocator };
            try polyrole.runner.Runner(push.Push).symmetric_run(.client, &pc, mx.subChannel(2), push.Push, null);
        }
    }.run, .{ names[2], msgs[2], l.socket.address, recvs[2] });

    // ── Server fibers: each accepts one connection, runs init → chat → push ──
    // All 3 server fibers run concurrently.
    const Srv = struct {
        fn run(
            lsn: *@TypeOf(l),
            usrs: *std.StringHashMap(void),
            msgs_: *std.ArrayList(chat_mod.Message),
            n: []const u8,
        ) !void {
            const s = try lsn.accept(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var mx: M = undefined;
            try mx.initFromChannel(allocator, &sc);
            defer mx.deinit();

            const Ri = polyrole.runner.Runner(init.Send);
            var isrv = init.ServerContext{ .users = usrs };
            try Ri.symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);

            const Rc = polyrole.runner.Runner(chat_mod.Say);
            var csrv = chat_mod.ServerContext{ .messages = msgs_, .username = n, .gpa = allocator };
            try Rc.symmetric_run(.server, &csrv, mx.subChannel(1), chat_mod.Say, null);

            const Rp = polyrole.runner.Runner(push.Push);
            for (msgs_.items) |m_| {
                var psrv = push.ServerContext{};
                psrv.pending = push.Message{ .kind = push.KIND_MSG, .from = m_.from, .text = m_.text };
                try Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null);
            }
            var psrv = push.ServerContext{ .kick = true };
            Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null) catch {};
        }
    };

    var sh0 = try zio.spawn(Srv.run, .{ &l, &users, &all_msgs, names[0] });
    var sh1 = try zio.spawn(Srv.run, .{ &l, &users, &all_msgs, names[1] });
    var sh2 = try zio.spawn(Srv.run, .{ &l, &users, &all_msgs, names[2] });

    ch0.join() catch {};
    ch1.join() catch {};
    ch2.join() catch {};
    sh0.join() catch {};
    sh1.join() catch {};
    sh2.join() catch {};

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), all_msgs.items.len);
    try std.testing.expect(recv0.items.len >= 1);
    try std.testing.expect(recv1.items.len >= 1);
    try std.testing.expect(recv2.items.len >= 1);
}
