const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Runner = polyrole.runner.Runner;
const nm = @import("root.zig");
const types = @import("context.zig");

test "模拟：基本流程" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const io = rt.io();
    var client: types.ClientContext = .{
        .io = io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.deinit();
    var server: types.ServerContext = .{};

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // fencepost: remaining=5 produces exactly 5 pings
    try testing.expectEqual(@as(u64, 5), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expectEqual(@as(usize, 5), client.results.items.len);

    // seq_num monotonic
    for (client.results.items, 0..) |r, i| {
        try testing.expectEqual(@as(u64, @intCast(i + 1)), r.seq_num);
    }

    // server receives and echoes last seq_num
    try testing.expectEqual(@as(u64, 5), server.last_seq_num);

    // last_send_ms was recorded
    try testing.expect(client.last_send_ms > 0);

    // RTT values are non-negative and reasonable
    for (client.results.items) |r| {
        try testing.expect(r.rtt_ms < std.math.maxInt(u64));
    }
}

test "对称运行：通过 StreamChannel 通信" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const io = rt.io();
    const allocator = testing.allocator;

    var client: types.ClientContext = .{
        .io = io,
        .allocator = allocator,
        .remaining = 3,
        .interval_ms = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.deinit();
    var server: types.ServerContext = .{};

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    const StreamChannel = polyrole.channel.StreamChannel;
    const R = Runner(nm.PingQuery);

    const C = struct {
        fn run(addr: zio.net.Address, ctx: *types.ClientContext) !void {
            var stream = try addr.connect(.{});
            defer stream.close();

            var ch: StreamChannel = undefined;
            try ch.init(allocator, stream, 128, 128);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, nm.PingQuery);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(C.run, .{ listener.socket.address, &client });

    var stream = try listener.accept(.{});
    defer stream.close();

    var ch: StreamChannel = undefined;
    try ch.init(allocator, stream, 128, 128);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, nm.PingQuery);

    try testing.expectEqual(@as(u64, 3), client.seq_num);
    try testing.expectEqual(@as(usize, 3), client.results.items.len);
}
