const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const Mux = polyrole.family_mux_channel.MultiplexChannel;
const init = @import("protocol/chat/init.zig");
const chat_mod = @import("protocol/chat/chat.zig");
const push = @import("protocol/chat/push.zig");

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
        for (all_msgs.items) |m| { allocator.free(m.from); allocator.free(m.text); }
        all_msgs.deinit(allocator);
    }
    var recv0: std.ArrayList(push.Message) = .empty;
    defer { for (recv0.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv0.deinit(allocator); }
    var recv1: std.ArrayList(push.Message) = .empty;
    defer { for (recv1.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv1.deinit(allocator); }
    var recv2: std.ArrayList(push.Message) = .empty;
    defer { for (recv2.items) |m| { allocator.free(m.from); allocator.free(m.text); } recv2.deinit(allocator); }

    const Client = struct {
        fn run(
            name: []const u8, msg: []const u8,
            a: zio.net.Address, recv: *std.ArrayList(push.Message),
        ) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            const Ri = polyrole.runner.Runner(init.Send);
            var ictx = init.ClientContext{ .username = name };
            try Ri.symmetric_run(.client, &ictx, m.subChannel(0), init.Send, null);
            try std.testing.expect(ictx.accepted);
            const Rc = polyrole.runner.Runner(chat_mod.Say);
            var cctx = chat_mod.ClientContext{ .pending_text = msg };
            try Rc.symmetric_run(.client, &cctx, m.subChannel(1), chat_mod.Say, null);
            const Rp = polyrole.runner.Runner(push.Push);
            var pctx = push.ClientContext{ .received = recv, .gpa = allocator };
            try Rp.symmetric_run(.client, &pctx, m.subChannel(2), push.Push, null);
        }
    };

    const names = [_][]const u8{ "alice", "bob", "charlie" };
    const msgs  = [_][]const u8{ "hello", "hi there", "hey" };
    const recvs = [_]*std.ArrayList(push.Message){ &recv0, &recv1, &recv2 };

    for (names, msgs, recvs) |name, msg, rc| {
        var h = try zio.spawn(Client.run, .{ name, msg, l.socket.address, rc });
        const s = try l.accept(.{});
        var sc: SC = undefined;
        try sc.init(allocator, s, 1024, 1024);
        defer sc.deinit(allocator);
        var m: M = undefined;
        try m.initFromChannel(allocator, &sc);
        defer m.deinit();

        const Ri = polyrole.runner.Runner(init.Send);
        var isrv = init.ServerContext{ .users = &users };
        try Ri.symmetric_run(.server, &isrv, m.subChannel(0), init.Send, null);
        try std.testing.expect(isrv.users.contains(name));

        const Rc = polyrole.runner.Runner(chat_mod.Say);
        var csrv = chat_mod.ServerContext{ .messages = &all_msgs, .username = name, .gpa = allocator };
        try Rc.symmetric_run(.server, &csrv, m.subChannel(1), chat_mod.Say, null);

        // Push: send exactly one notification to verify the channel works
        const Rp = polyrole.runner.Runner(push.Push);
        var psrv = push.ServerContext{};
        if (all_msgs.items.len > 0) {
            psrv.pending = push.Message{ .kind = push.KIND_MSG, .from = all_msgs.items[0].from, .text = all_msgs.items[0].text };
        } else {
            psrv.kick = true;
        }
        try Rp.symmetric_run(.server, &psrv, m.subChannel(2), push.Push, null);
        h.join() catch {};
    }

    try std.testing.expectEqual(@as(usize, 3), users.count());
    try std.testing.expectEqual(@as(usize, 3), all_msgs.items.len);
    try std.testing.expect(recv0.items.len >= 1);
    try std.testing.expect(recv1.items.len >= 1);
    try std.testing.expect(recv2.items.len >= 1);
}
