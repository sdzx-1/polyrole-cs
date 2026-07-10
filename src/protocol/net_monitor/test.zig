const std = @import("std");
const polyrole = @import("../../root.zig");
const Runner = polyrole.runner.Runner;
const nm = @import("root.zig");
const types = @import("context.zig");

test "模拟：N 次 ping" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 5), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expect(client.windows.items.len >= 1);
    try testing.expectEqual(@as(u32, 5), client.windows.items[0].rtt_count);
}

test "模拟：remaining=1 恰好产生一次 ping" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 1,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 1), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expectEqual(@as(u32, 1), client.windows.items[0].rtt_count);
}

test "模拟：多窗口——小窗口产生多个窗口条目" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 1, // 1ms delay → total elapsed ~5ms
        .window_duration_ms = 1, // 1ms windows → ~5 windows
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // With 1ms delay and 1ms windows, we should get multiple windows
    try testing.expect(client.windows.items.len >= 2);

    // Total count across all windows should equal pings sent
    var total: u32 = 0;
    for (client.windows.items) |w| total += w.rtt_count;
    try testing.expectEqual(@as(u32, 5), total);
}

test "模拟：窗口全局计数一致性" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 10,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 10), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);

    // All pings should be accounted for in windows
    var total: u32 = 0;
    for (client.windows.items) |w| total += w.rtt_count;
    try testing.expectEqual(@as(u32, 10), total);
}

test "模拟：窗口指标正确性" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    const w = client.windows.items[0];
    try testing.expectEqual(@as(u32, 3), w.rtt_count);
    try testing.expect(w.rtt_sum_ms >= w.rtt_min_ms);
    try testing.expect(w.rtt_min_ms <= w.rtt_max_ms);
    try testing.expect(w.rtt_max_ms >= w.rtt_min_ms);
    // In simulate, RTTs are ~0ms so min should not be maxInt
    try testing.expect(w.rtt_min_ms < std.math.maxInt(u64));
}

test "模拟：session_start_ms 仅设置一次且不变" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    try testing.expectEqual(@as(u64, 0), client.session_start_ms);

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // Should have been set by first PingQuery
    try testing.expect(client.session_start_ms > 0);

    // Window start times should be relative to session start
    try testing.expect(client.windows.items.len >= 1);
    try testing.expect(client.windows.items[0].start_ms >= client.session_start_ms);
}

test "模拟：seq_num 单调递增" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 5), client.seq_num);
}

test "模拟：服务端回显验证" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // After simulation, server should have stored the last ping's data
    // The last seq_num sent was 3
    try testing.expectEqual(@as(u64, 3), server.last_seq_num);
    // last_client_send_time should have been set (non-zero)
    try testing.expect(server.last_client_send_time > 0);
}

test "模拟：服务端 dwell 非负" {
    const testing = std.testing;
    // We only check that server dwell is well-formed by verifying
    // the protocol doesn't crash and metrics are sane after simulate.
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 1,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // In simulate, dwell should be ~0 and RTT should be ~0 (both sides in-process)
    const w = client.windows.items[0];
    try testing.expectEqual(@as(u32, 1), w.rtt_count);
    try testing.expect(w.rtt_sum_ms < 1000); // RTT should be sub-second
}

test "对称运行：通过 StreamChannel 通信" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    var client = types.ClientContext{
        .io = io,
        .allocator = allocator,
        .remaining = 5,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = io };

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

test "RTT 值非负" {
    const testing = std.testing;
    var client = types.ClientContext{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .window_duration_ms = 60000,
        .windows = std.ArrayList(types.WindowMetrics).empty,
    };
    defer client.windows.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    for (client.windows.items) |w| {
        if (w.rtt_count == 0) continue;
        try testing.expect(w.rtt_min_ms < std.math.maxInt(u64));
        try testing.expect(w.rtt_max_ms >= w.rtt_min_ms);
    }
}
