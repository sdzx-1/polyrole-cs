const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const Mux = polyrole.family_mux_channel.MultiplexChannel;
const init = @import("protocol/chat/init.zig");
const chat_mod = @import("protocol/chat/chat.zig");
const push = @import("protocol/chat/push.zig");

test "chat: init + chat + push over Mux(3)" {
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
    var client_recv: std.ArrayList(push.Message) = .empty;
    defer {
        for (client_recv.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        client_recv.deinit(allocator);
    }
    var server_chat_msgs: std.ArrayList(chat_mod.Message) = .empty;
    defer {
        for (server_chat_msgs.items) |m| {
            allocator.free(m.from);
            allocator.free(m.text);
        }
        server_chat_msgs.deinit(allocator);
    }

    // Client: init → chat(send msg) → push(receive broadcast)
    var h = try zio.spawn(struct {
        fn run(
            a: zio.net.Address,
            recv: *std.ArrayList(push.Message),
        ) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 1024, 1024);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();

            // Init
            const Ri = polyrole.runner.Runner(init.Send);
            var ictx = init.ClientContext{};
            const name = "alice";
            @memcpy(ictx.username[0..name.len], name);
            ictx.name_len = name.len;
            try Ri.symmetric_run(.client, &ictx, m.subChannel(0), init.Send, null);
            try std.testing.expect(ictx.accepted);

            // Chat: send one message then exit
            const Rc = polyrole.runner.Runner(chat_mod.Say);
            var cctx = chat_mod.ClientContext{ .pending_text = "hello" };
            try Rc.symmetric_run(.client, &cctx, m.subChannel(1), chat_mod.Say, null);

            // Push: receive broadcast messages
            const Rp = polyrole.runner.Runner(push.Push);
            var pctx = push.ClientContext{ .received = recv, .gpa = allocator };
            try Rp.symmetric_run(.client, &pctx, m.subChannel(2), push.Push, null);
        }
    }.run, .{l.socket.address, &client_recv});

    // Server: accept → init → chat → push
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
    try std.testing.expect(isrv.users.contains("alice"));

    const Rc = polyrole.runner.Runner(chat_mod.Say);
    var csrv = chat_mod.ServerContext{ .messages = &server_chat_msgs, .username = "alice", .gpa = allocator };
    try Rc.symmetric_run(.server, &csrv, m.subChannel(1), chat_mod.Say, null);

    const Rp = polyrole.runner.Runner(push.Push);
    var psrv = push.ServerContext{};
    // Send a chat message notification (the one received from chat)
    if (server_chat_msgs.items.len > 0) {
        psrv.pending = push.Message{
            .kind = push.KIND_MSG,
            .from = server_chat_msgs.items[0].from,
            .text = server_chat_msgs.items[0].text,
        };
    }
    try Rp.symmetric_run(.server, &psrv, m.subChannel(2), push.Push, null);

    h.join() catch {};

    try std.testing.expect(client_recv.items.len >= 1);
    try std.testing.expectEqual(push.KIND_MSG, client_recv.items[0].kind);
}
