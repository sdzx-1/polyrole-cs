const std = @import("std");
const polyrole = @import("../../root.zig");
const Runner = polyrole.runner.Runner;
const nm = @import("root.zig");
const types = @import("context.zig");

test "simulate: N pings" {
    const testing = std.testing;
    var client = types.ClientContext{
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ns = 0,
        .window_duration_ns = 60_000_000_000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{};

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 5), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expect(client.windows.items.len >= 1);
    try testing.expectEqual(@as(u32, 5), client.windows.items[0].rtt_count);
}

test "simulate: remaining=1 produces exactly 1 ping" {
    const testing = std.testing;
    var client = types.ClientContext{
        .allocator = testing.allocator,
        .remaining = 1,
        .interval_ns = 0,
        .window_duration_ns = 60_000_000_000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{};

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 1), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expectEqual(@as(u32, 1), client.windows.items[0].rtt_count);
}

test "symmetric run over StreamChannel" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    var client = types.ClientContext{
        .allocator = allocator,
        .remaining = 5,
        .interval_ns = 0,
        .window_duration_ns = 60_000_000_000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{};

    const localhost: net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try localhost.listen(io, .{});
    defer listener.deinit(io);

    const StreamChannel = polyrole.channel.StreamChannel;
    const R = Runner(nm.PingQuery);

    const C = struct {
        fn run(addr: net.IpAddress, ctx: *types.ClientContext) !void {
            var stream = try addr.connect(io, .{ .mode = .stream });
            defer stream.close(io);

            var ch: StreamChannel = undefined;
            try ch.init(io, allocator, stream, 128, 128);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, nm.PingQuery);
        }
    };

    var client_task = try io.concurrent(C.run, .{ listener.socket.address, &client });
    defer client_task.cancel(io) catch {};

    var stream = try listener.accept(io);
    defer stream.close(io);

    var ch: StreamChannel = undefined;
    try ch.init(io, allocator, stream, 128, 128);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, nm.PingQuery);

    try testing.expectEqual(@as(u64, 5), client.seq_num);
    try testing.expect(client.windows.items.len >= 1);
    try testing.expectEqual(@as(u32, 5), client.windows.items[0].rtt_count);
}

test "RTT values are non-negative" {
    const testing = std.testing;
    var client = types.ClientContext{
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ns = 0,
        .window_duration_ns = 60_000_000_000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{};

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    for (client.windows.items) |w| {
        if (w.rtt_count == 0) continue;
        try testing.expect(w.rtt_min_ns < std.math.maxInt(u64));
        try testing.expect(w.rtt_max_ns >= w.rtt_min_ns);
    }
}
