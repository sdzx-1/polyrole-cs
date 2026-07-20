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
    const M = Mux(1);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx: i32 = 0;
    var cli_ctx: i32 = 0;

    // Client fiber
    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address, ctx: *i32) !void {
            const s = try a.connect(.{});
            var m: M = undefined;
            try m.init(allocator, s, 256, 256);
            defer m.deinit();
            try R.symmetric_run(.client, ctx, m.subChannel(0), P1.A, null);
        }
    }.run, .{l.socket.address, &cli_ctx});

    // Server
    const s = try l.accept(.{});
    var m: M = undefined;
    try m.init(allocator, s, 256, 256);
    defer m.deinit();
    _ = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{m.subChannel(0), &srv_ctx});
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx);
}

test "family: two protocols concurrent" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const R2 = polyrole.runner.Runner(P2.A);
    const M = Mux(2);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx1: i32 = 0;
    var srv_ctx2: i32 = 0;
    var cli_ctx1: i32 = 0;
    var cli_ctx2: i32 = 0;

    // Client: run both protocols concurrently
    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address, c1: *i32, c2: *i32) !void {
            const s = try a.connect(.{});
            var m: M = undefined;
            try m.init(allocator, s, 256, 256);
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

    // Server
    const s = try l.accept(.{});
    var m: M = undefined;
    try m.init(allocator, s, 256, 256);
    defer m.deinit();
    _ = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R1.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{m.subChannel(0), &srv_ctx1});
    _ = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R2.symmetric_run(.server, ctx, ch, P2.A, null);
        }
    }.run, .{m.subChannel(1), &srv_ctx2});
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx2);
}

test "family: TLS + two sequential protocols over encrypted TlsChannel" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const crypto = std.crypto;
    const tls = @import("protocol/tls.zig");
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const R2 = polyrole.runner.Runner(P2.A);
    const Rtls = polyrole.runner.Runner(tls.ClientHello);
    const StreamChannel = polyrole.channel.StreamChannel;
    const TlsChannel = polyrole.channel.TlsChannel;

    // Generate TLS keypairs
    var seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&seed);
    const kp_c = try crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    try zio.randomSecure(&seed);
    const kp_s = try crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);

    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx1: i32 = 0;
    var srv_ctx2: i32 = 0;
    var cli_ctx1: i32 = 0;
    var cli_ctx2: i32 = 0;

    // Client fiber: TLS handshake → P1 → P2
    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address, kp: crypto.sign.Ed25519.KeyPair, pk: crypto.sign.Ed25519.PublicKey, c1: *i32, c2: *i32) !void {
            const s = try a.connect(.{});
            var sc: StreamChannel = undefined;
            try sc.init(allocator, s, 256, 256);
            defer sc.deinit(allocator);

            // Phase 1: TLS handshake
            var tls_ctx = tls.ClientContext.init(kp, pk);
            try Rtls.symmetric_run(.client, &tls_ctx, &sc, tls.ClientHello, null);

            var tc: TlsChannel = undefined;
            try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
            defer tc.deinit(allocator);
            tls_ctx.deinit();

            // Phase 2: P1
            try R1.symmetric_run(.client, c1, &tc, P1.A, null);
            // Phase 3: P2 — reuse same encrypted channel
            try R2.symmetric_run(.client, c2, &tc, P2.A, null);
        }
    }.run, .{l.socket.address, kp_c, kp_s.public_key, &cli_ctx1, &cli_ctx2});

    // Server: accept → TLS → P1 → P2
    const s = try l.accept(.{});
    var sc: StreamChannel = undefined;
    try sc.init(allocator, s, 256, 256);
    defer sc.deinit(allocator);

    var tls_ctx = tls.ServerContext.init(kp_s, kp_c.public_key);
    try Rtls.symmetric_run(.server, &tls_ctx, &sc, tls.ClientHello, null);

    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
    defer tc.deinit(allocator);
    tls_ctx.deinit();

    try R1.symmetric_run(.server, &srv_ctx1, &tc, P1.A, null);
    try R2.symmetric_run(.server, &srv_ctx2, &tc, P2.A, null);

    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx2);
}
