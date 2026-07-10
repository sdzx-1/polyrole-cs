const std = @import("std");
const polyrole = @import("../../root.zig");
const Runner = polyrole.runner.Runner;
const nm = @import("root.zig");
const types = @import("context.zig");

test "模拟：N 次 ping" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 5), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expectEqual(@as(usize, 5), client.results.items.len);
}

test "模拟：remaining=1 恰好产生一次 ping" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 1,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 1), client.seq_num);
    try testing.expectEqual(@as(u32, 0), client.remaining);
    try testing.expectEqual(@as(usize, 1), client.results.items.len);
}

test "模拟：max_results 限制" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 10,
        .interval_ms = 0,
        .max_results = 3,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    // Only first 3 results stored, seq still reached 10
    try testing.expectEqual(@as(u64, 10), client.seq_num);
    try testing.expectEqual(@as(usize, 3), client.results.items.len);
    try testing.expectEqual(@as(u64, 1), client.results.items[0].seq_num);
    try testing.expectEqual(@as(u64, 3), client.results.items[2].seq_num);
}

test "模拟：seq_num 单调递增" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 5,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    for (client.results.items, 0..) |r, i| {
        try testing.expectEqual(@as(u64, @intCast(i + 1)), r.seq_num);
    }
}

test "模拟：结果字段完整性" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    for (client.results.items) |r| {
        try testing.expect(r.seq_num > 0);
        try testing.expect(r.rtt_ms < 1000);      // simulate RTT should be near-zero
        try testing.expect(r.server_dwell_ms < 1000); // dwell should be near-zero
    }
}

test "模拟：服务端回显验证" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expectEqual(@as(u64, 3), server.last_seq_num);
}

test "模拟：last_send_ms 已设置" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 1,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    try testing.expect(client.last_send_ms > 0);
}

test "对称运行：通过 StreamChannel 通信" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    var client: types.ClientContext = .{
        .io = io,
        .allocator = allocator,
        .remaining = 5,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
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
    try testing.expectEqual(@as(usize, 5), client.results.items.len);
}

test "RTT 值非负" {
    const testing = std.testing;
    var client: types.ClientContext = .{
        .io = testing.io,
        .allocator = testing.allocator,
        .remaining = 3,
        .interval_ms = 0,
        .max_results = 0,
        .results = std.ArrayList(types.PingResult).empty,
    };
    defer client.results.deinit(client.allocator);
    var server: types.ServerContext = .{ .io = testing.io };

    const R = Runner(nm.PingQuery);
    try R.simulate(&client, &server, nm.PingQuery);

    for (client.results.items) |r| {
        try testing.expect(r.rtt_ms < std.math.maxInt(u64)); // not sat_min
    }
}
