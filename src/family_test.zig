const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const Mux = @import("family_mux_channel.zig").MultiplexChannel;

// ── tests ─────────────────────────────────────────────────────────────────

const TestProtocol = struct {
    fn make(comptime name: []const u8, comptime Next: type) type {
        return struct {
            const Info = polyrole.ProtocolInfo(name, i32, i32);
            pub const A = union(enum) {
                to_b: polyrole.Data(void, B),
                pub const info: Info = .{ .agent = .client, .name = name ++ ".A" };
                pub fn process(ctx: *i32) @This() { _ = ctx; return .to_b; }
            };
            pub const B = union(enum) {
                to_a: polyrole.Data(void, A),
                done: polyrole.Data(void, Next),
                pub const info: Info = .{ .agent = .server, .name = name ++ ".B" };
                pub fn process(ctx: *i32) @This() {
                    ctx.* += 1;
                    if (ctx.* >= 3) return .done;
                    return .to_a;
                }
            };
        };
    }
};

test "family: full handshake" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const R = polyrole.runner.Runner(P1.A);
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(1, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx: i32 = 0;
    var cli_ctx: i32 = 0;

    var h = try zio.spawn(struct {
        fn run(a: zio.net.Address, ctx: *i32) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            try R.symmetric_run(.client, ctx, m.subChannel(0), P1.A, null);
        }
    }.run, .{l.socket.address, &cli_ctx});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();
    var sh = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{m.subChannel(0), &srv_ctx});
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx);
    sh.join() catch {};
    h.join() catch {};
}

test "family: recv timeout" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const R2 = polyrole.runner.Runner(P2.A);
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(2, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx1: i32 = 0;
    var srv_ctx2: i32 = 0;

    // Client: P1 sends normally, P2 never sends
    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            // P1: run normally
            var c1: i32 = 0;
            try R1.symmetric_run(.client, &c1, m.subChannel(0), P1.A, null);
            // Keep connection alive for P2 timeout
            try zio.sleep(zio.Duration.fromSeconds(1));
        }
    }.run, .{l.socket.address});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();

    var sh = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R1.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{m.subChannel(0), &srv_ctx1});

    // P2: server recvs with 100ms timeout, client never sends
    const err = R2.symmetric_run(.server, &srv_ctx2, m.subChannel(1), P2.A, 100);
    try std.testing.expectError(error.Canceled, err);

    try zio.sleep(zio.Duration.fromMilliseconds(300));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    sh.join() catch {};
}

test "family: two protocols concurrent" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const R2 = polyrole.runner.Runner(P2.A);
    const SC = polyrole.channel.StreamChannel;
    const M = Mux(2, 1024, 8);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx1: i32 = 0;
    var srv_ctx2: i32 = 0;
    var cli_ctx1: i32 = 0;
    var cli_ctx2: i32 = 0;

    var h = try zio.spawn(struct {
        fn run(a: zio.net.Address, c1: *i32, c2: *i32) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            var h1 = try zio.spawn(struct {
                fn run(mx: *M, ch: *M.SubChannel, ctx: *i32) anyerror!void {
                    _ = mx;
                    try R1.symmetric_run(.client, ctx, ch, P1.A, null);
                }
            }.run, .{ &m, m.subChannel(0), c1 });
            var h2 = try zio.spawn(struct {
                fn run(mx: *M, ch: *M.SubChannel, ctx: *i32) anyerror!void {
                    _ = mx;
                    try R2.symmetric_run(.client, ctx, ch, P2.A, null);
                }
            }.run, .{ &m, m.subChannel(1), c2 });
            h1.join() catch {};
            h2.join() catch {};
        }
    }.run, .{l.socket.address, &cli_ctx1, &cli_ctx2});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();
    var sh1 = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R1.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{m.subChannel(0), &srv_ctx1});
    var sh2 = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R2.symmetric_run(.server, ctx, ch, P2.A, null);
        }
    }.run, .{m.subChannel(1), &srv_ctx2});
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx2);
    sh1.join() catch {};
    sh2.join() catch {};
    h.join() catch {};
}

