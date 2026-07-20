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
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var users = std.StringHashMap(void).init(allocator);
    defer users.deinit();
    var all_msgs: std.ArrayList(chat_mod.Message) = .empty;
    defer {
        for (all_msgs.items) |m| { allocator.free(m.from); allocator.free(m.text); }
        all_msgs.deinit(allocator);
    }

    var recvs: [3]std.ArrayList(push.Message) = @splat(.empty);
    defer for (&recvs) |*r| {
        for (r.items) |m| { allocator.free(m.from); allocator.free(m.text); }
        r.deinit(allocator);
    };

    const C = struct {
        fn run(
            a: zio.net.Address, n: []const u8, m_: []const u8,
            r: *std.ArrayList(push.Message), gpa: std.mem.Allocator,
        ) !void {
            const M = Mux(3, false, 1024, 8);
            const SC = polyrole.channel.StreamChannel;
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(gpa, s, 1024, 1024);
            defer sc.deinit(gpa);
            var mx: M = undefined;
            try mx.initFromChannel(gpa, &sc);
            defer mx.deinit();
            // Init first
            var ic = init.ClientContext{ .username = n };
            try polyrole.runner.Runner(init.Send).symmetric_run(.client, &ic, mx.subChannel(0), init.Send, null);
            // Chat and Push concurrently
            var hc = try zio.spawn(struct {
                fn run(mx2: *M, text: []const u8) !void {
                    var cc = chat_mod.ClientContext{ .pending_text = text };
                    try polyrole.runner.Runner(chat_mod.Say).symmetric_run(.client, &cc, mx2.subChannel(1), chat_mod.Say, null);
                }
            }.run, .{ &mx, m_ });
            var hp = try zio.spawn(struct {
                fn run(mx2: *M, gpa2: std.mem.Allocator, recv: *std.ArrayList(push.Message)) !void {
                    var pc = push.ClientContext{ .received = recv, .gpa = gpa2 };
                    try polyrole.runner.Runner(push.Push).symmetric_run(.client, &pc, mx2.subChannel(2), push.Push, null);
                }
            }.run, .{ &mx, gpa, r });
            hc.join() catch {};
            hp.join() catch {};
        }
    };

    var ch0 = try zio.spawn(C.run, .{ l.socket.address, "alice", "hello", &recvs[0], allocator });
    var ch1 = try zio.spawn(C.run, .{ l.socket.address, "bob", "hi there", &recvs[1], allocator });
    var ch2 = try zio.spawn(C.run, .{ l.socket.address, "charlie", "hey", &recvs[2], allocator });

    const S = struct {
        fn run(
            lsn: *@TypeOf(l), usrs: *std.StringHashMap(void),
            msgs_: *std.ArrayList(chat_mod.Message),
            n: []const u8, gpa: std.mem.Allocator,
        ) !void {
            const M = Mux(3, false, 1024, 8);
            const SC = polyrole.channel.StreamChannel;
            const s = try lsn.accept(.{});
            var sc: SC = undefined;
            try sc.init(gpa, s, 1024, 1024);
            defer sc.deinit(gpa);
            var mx: M = undefined;
            try mx.initFromChannel(gpa, &sc);
            defer mx.deinit();
            // Init first
            const Ri = polyrole.runner.Runner(init.Send);
            var isrv = init.ServerContext{ .users = usrs };
            try Ri.symmetric_run(.server, &isrv, mx.subChannel(0), init.Send, null);
            // Chat and Push concurrently
            var hc = try zio.spawn(struct {
                fn run(mx2: *M, gpa2: std.mem.Allocator, ms2: *std.ArrayList(chat_mod.Message), name: []const u8) !void {
                    const Rc = polyrole.runner.Runner(chat_mod.Say);
                    var csrv = chat_mod.ServerContext{ .messages = ms2, .username = name, .gpa = gpa2 };
                    try Rc.symmetric_run(.server, &csrv, mx2.subChannel(1), chat_mod.Say, null);
                }
            }.run, .{ &mx, gpa, msgs_, n });
            // Wait briefly for chat to produce messages, then push
            try zio.sleep(zio.Duration.fromMilliseconds(50));
            const Rp = polyrole.runner.Runner(push.Push);
            for (msgs_.items) |m_| {
                var psrv = push.ServerContext{};
                psrv.pending = push.Message{ .kind = push.KIND_MSG, .from = m_.from, .text = m_.text };
                try Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null);
            }
            var psrv = push.ServerContext{ .kick = true };
            Rp.symmetric_run(.server, &psrv, mx.subChannel(2), push.Push, null) catch {};
            hc.join() catch {};
        }
    };

    var sh0 = try zio.spawn(S.run, .{ &l, &users, &all_msgs, "alice", allocator });
    var sh1 = try zio.spawn(S.run, .{ &l, &users, &all_msgs, "bob", allocator });
    var sh2 = try zio.spawn(S.run, .{ &l, &users, &all_msgs, "charlie", allocator });

    ch0.join() catch {}; ch1.join() catch {}; ch2.join() catch {};
    sh0.join() catch {}; sh1.join() catch {}; sh2.join() catch {};

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), all_msgs.items.len);
    try std.testing.expect(recvs[0].items.len >= 1);
    try std.testing.expect(recvs[1].items.len >= 1);
    try std.testing.expect(recvs[2].items.len >= 1);
}
