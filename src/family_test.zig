const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const family = @import("family_mux_channel.zig");
const Mux = family.Mux;
const MultiplexChannel = family.MultiplexChannel;

// ── 测试 ─────────────────────────────────────────────────────────────────

const TestProtocol = struct {
    fn make(comptime name: []const u8, comptime Next: type) type {
        return struct {
            const Info = polyrole.ProtocolInfo(name, i32, i32);
            pub const A = union(enum) {
                to_b: polyrole.Data(void, B),
                pub const info: Info = .{ .agent = .client, .name = name ++ ".A" };
                pub fn process(ctx: *i32) @This() {
                    _ = ctx;
                    return .to_b;
                }
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
            try sc.init(allocator, s, 256, 256, 4096);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            try R.symmetric_run(.client, ctx, m.subChannel(0), P1.A, null);
        }
    }.run, .{ l.socket.address, &cli_ctx });

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256, 4096);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();
    var sh = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{ m.subChannel(0), &srv_ctx });
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

    // 客户端：P1 正常发送，P2 从不发送
    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256, 4096);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            // P1：正常运行
            var c1: i32 = 0;
            try R1.symmetric_run(.client, &c1, m.subChannel(0), P1.A, null);
            // 保持连接存活以等待 P2 超时
            try zio.sleep(zio.Duration.fromSeconds(1));
        }
    }.run, .{l.socket.address});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256, 4096);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();

    var sh = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R1.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{ m.subChannel(0), &srv_ctx1 });

    // P2：服务端以 100ms 超时 recv，客户端从不发送
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
            try sc.init(allocator, s, 256, 256, 4096);
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
    }.run, .{ l.socket.address, &cli_ctx1, &cli_ctx2 });

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256, 4096);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();
    var sh1 = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R1.symmetric_run(.server, ctx, ch, P1.A, null);
        }
    }.run, .{ m.subChannel(0), &srv_ctx1 });
    var sh2 = try zio.spawn(struct {
        fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
            try R2.symmetric_run(.server, ctx, ch, P2.A, null);
        }
    }.run, .{ m.subChannel(1), &srv_ctx2 });
    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx2);
    sh1.join() catch {};
    sh2.join() catch {};
    h.join() catch {};
}

test "family: overflow close_channel surfaces ProtocolOverflow, other protocols unaffected" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const SC = polyrole.channel.StreamChannel;
    const M = MultiplexChannel(&.{
        .{ .capacity = 8, .max_message_size = 1024 },
        .{ .capacity = 1, .max_message_size = 1024, .overflow = .close_channel },
    });
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    const P2Tag = std.meta.Tag(P2.A);
    const to_b_msg: P2.A = .{ .to_b = .{ .data = {} } };
    // 客户端灌满子通道 1（从不 recv），然后正常运行 P1。
    var h = try zio.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256, 4096);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            for (0..10) |_| try m.subChannel(1).send(@as(P2Tag, .to_b), P2.A, to_b_msg);
            var c1: i32 = 0;
            try R1.symmetric_run(.client, &c1, m.subChannel(0), P1.A, null);
        }
    }.run, .{l.socket.address});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256, 4096);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();

    // 让 Reader 处理灌入的帧：队列容纳 1 帧，第二帧溢出并以可区分的错误关闭 sub1。
    try zio.sleep(zio.Duration.fromMilliseconds(200));
    try std.testing.expectError(error.ProtocolOverflow, m.subChannel(1).recv(@as(P2Tag, .to_b), P2.A));

    // sub0 不受溢出影响，正常完成握手。
    var srv_ctx1: i32 = 0;
    try R1.symmetric_run(.server, &srv_ctx1, m.subChannel(0), P1.A, null);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    h.join() catch {};
}

test "family: backpressure policy blocks reader, no frames lost" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const SC = polyrole.channel.StreamChannel;
    const M = MultiplexChannel(&.{.{ .capacity = 1, .max_message_size = 1024, .overflow = .backpressure }});
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    const P2Tag = std.meta.Tag(P2.A);
    const to_b_msg: P2.A = .{ .to_b = .{ .data = {} } };
    // 客户端不等待地连发 10 帧；队列满（容量 1）时 Reader 阻塞，
    // 随后服务端把 10 帧全部取出。
    var h = try zio.spawn(struct {
        fn run(a: zio.net.Address) !void {
            const s = try a.connect(.{});
            var sc: SC = undefined;
            try sc.init(allocator, s, 256, 256, 4096);
            defer sc.deinit(allocator);
            var m: M = undefined;
            try m.initFromChannel(allocator, &sc);
            defer m.deinit();
            for (0..10) |_| try m.subChannel(0).send(@as(P2Tag, .to_b), P2.A, to_b_msg);
        }
    }.run, .{l.socket.address});

    const s = try l.accept(.{});
    var sc: SC = undefined;
    try sc.init(allocator, s, 256, 256, 4096);
    defer sc.deinit(allocator);
    var m: M = undefined;
    try m.initFromChannel(allocator, &sc);
    defer m.deinit();

    // 尽管队列容量为 1，10 帧仍按序全部到达。
    for (0..10) |_| {
        _ = try m.subChannel(0).recv(@as(P2Tag, .to_b), P2.A);
    }
    h.join() catch {};
}

test "family: protocols over one TLS session via TlsChannel.transport()" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const crypto = std.crypto;
    const tls = @import("protocol/tls.zig");
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const P2 = TestProtocol.make("p2", polyrole.Exit);
    const R1 = polyrole.runner.Runner(P1.A);
    const R2 = polyrole.runner.Runner(P2.A);
    const SC = polyrole.channel.StreamChannel;
    const TC = polyrole.channel.TlsChannel;
    const M = MultiplexChannel(&.{
        .{ .capacity = 8, .max_message_size = 1024 },
        .{ .capacity = 8, .max_message_size = 1024 },
    });
    const R_tls = polyrole.runner.Runner(tls.ClientHello);

    var kp_seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&kp_seed);
    const kp_c = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);
    try zio.randomSecure(&kp_seed);
    const kp_s = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);

    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var cli_ctx1: i32 = 0;
    var cli_ctx2: i32 = 0;

    const ClientTask = struct {
        fn run(
            a: zio.net.Address,
            kp: crypto.sign.Ed25519.KeyPair,
            peer_pk: crypto.sign.Ed25519.PublicKey,
            c1: *i32,
            c2: *i32,
        ) !void {
            const s = try a.connect(.{});
            defer s.close();

            // 阶段 1：整个协议族共享一次 TLS 握手。
            var tls_ctx = tls.ClientContext.init(kp, peer_pk);
            var sc: SC = undefined;
            try sc.init(allocator, s, 512, 512, 4096);
            defer sc.deinit(allocator);
            try R_tls.symmetric_run(.client, &tls_ctx, &sc, tls.ClientHello, null);

            // 阶段 2：在已建立的 TLS 记录层之上运行 Mux。
            var tc: TC = undefined;
            try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 2048);
            defer tc.deinit(allocator);
            tls_ctx.deinit();

            var m: M = undefined;
            try m.initFromTransport(allocator, tc.transport());
            defer m.deinit();

            try R1.symmetric_run(.client, c1, m.subChannel(0), P1.A, null);
            try R2.symmetric_run(.client, c2, m.subChannel(1), P2.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(ClientTask.run, .{
        l.socket.address,
        kp_c,
        kp_s.public_key,
        &cli_ctx1,
        &cli_ctx2,
    });

    const s = try l.accept(.{});
    defer s.close();

    var tls_ctx = tls.ServerContext.init(kp_s, kp_c.public_key);
    var sc: SC = undefined;
    try sc.init(allocator, s, 512, 512, 4096);
    defer sc.deinit(allocator);
    try R_tls.symmetric_run(.server, &tls_ctx, &sc, tls.ClientHello, null);

    var tc: TC = undefined;
    try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 2048);
    defer tc.deinit(allocator);
    tls_ctx.deinit();

    var m: M = undefined;
    try m.initFromTransport(allocator, tc.transport());
    defer m.deinit();

    var srv_ctx1: i32 = 0;
    var srv_ctx2: i32 = 0;
    try R1.symmetric_run(.server, &srv_ctx1, m.subChannel(0), P1.A, null);
    try R2.symmetric_run(.server, &srv_ctx2, m.subChannel(1), P2.A, null);

    try std.testing.expectEqual(@as(i32, 3), srv_ctx1);
    try std.testing.expectEqual(@as(i32, 3), srv_ctx2);
}
