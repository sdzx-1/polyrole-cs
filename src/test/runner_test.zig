const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const Runner = polyrole.runner.Runner;
const Protocol = polyrole.runner.Protocol;
const Mux = polyrole.runner.Mux;
const MuxKeys = polyrole.runner.MuxKeys;
const Exit = polyrole.Exit;
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const StreamChannel = polyrole.channel.StreamChannel;
const TlsChannel = polyrole.channel.TlsChannel;
const InMemoryChannel = polyrole.channel.InMemoryChannel;
const HalfChannel = polyrole.channel.HalfChannel;
const tls = polyrole.tls;

fn CreateTestProtocol(name: []const u8, Next: type) type {
    return struct {
        const TestInfo = ProtocolInfo(name, i32, i32);

        pub const A = union(enum) {
            add: Data(void, B),

            pub const info: TestInfo = .{ .agent = .client, .name = "A" };

            pub fn process(ctx: *i32) @This() {
                _ = ctx;
                return .add;
            }
        };

        pub const B = union(enum) {
            to_a: Data(void, A),
            next: Data(void, C),

            pub const info: TestInfo = .{ .agent = .server, .name = "B" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* >= 1000) return .next;
                ctx.* += 1;
                return .to_a;
            }
        };

        pub const C = union(enum) {
            client_add: Data(void, @This()),
            next: Data(void, Next),

            pub const info: TestInfo = .{ .agent = .server, .name = "C" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* == 0) return .next;
                ctx.* -= 1;
                return .client_add;
            }

            pub fn preprocess(ctx: *i32, msg: @This()) !void {
                switch (msg) {
                    .client_add => ctx.* += 1,
                    .next => {},
                }
            }
        };
    };
}
test "simulate" {
    const testing = std.testing;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client: i32 = 0;
    var server: i32 = 0;
    try R.simulate(&client, &server, P.A);
    try testing.expectEqual(client, 1000);
}
test "symmetric run" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client_context: i32 = 0;
    var server_context: i32 = 0;

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try localhost.listen(.{});
    defer server.close();


    const S = struct {
        fn clientFn(server_address: zio.net.Address, ctx: *i32) !void {
            var stream = try server_address.connect(.{});
            defer stream.close();

            var stream_channel: StreamChannel = undefined;
            try stream_channel.init(allocator, stream, 100, 100, 4096);
            defer stream_channel.deinit(allocator);

            try R.symmetric_run(.client, ctx, &stream_channel, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ server.socket.address, &client_context });

    var stream = try server.accept(.{});
    defer stream.close();

    var stream_channel: StreamChannel = undefined;
    try stream_channel.init(allocator, stream, 100, 100, 4096);
    defer stream_channel.deinit(allocator);

    try R.symmetric_run(.server, &server_context, &stream_channel, P.A, null);

    // 服务端跑完时客户端可能还在收 C 阶段的剩余消息,必须等客户端完成再断言。
    try group.wait();
    try testing.expectEqual(client_context, 1000);
}

test "symmetric run over in-memory channel" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P = CreateTestProtocol("inmem", Exit);
    const R = Runner(P.A);

    var client_context: i32 = 0;
    var server_context: i32 = 0;

    // InMemoryChannel 不经过网络 I/O：两个 HalfChannel 交叉配对成
    // 全双工管道，ch1（客户端）与 ch2（服务端）各持一端。
    var half1: HalfChannel = undefined;
    var half2: HalfChannel = undefined;
    try half1.init(allocator, 1024);
    try half2.init(allocator, 1024);
    defer half1.deinit(allocator);
    defer half2.deinit(allocator);

    const ch1: InMemoryChannel = .{ .max_slice_len = 1024, .half_self = &half1, .half_peer = &half2 };
    const ch2: InMemoryChannel = .{ .max_slice_len = 1024, .half_self = &half2, .half_peer = &half1 };

    const S = struct {
        fn clientFn(chan: *const InMemoryChannel, ctx: *i32) !void {
            try R.symmetric_run(.client, ctx, chan, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ &ch1, &client_context });

    try R.symmetric_run(.server, &server_context, &ch2, P.A, null);

    try group.wait();
    try testing.expectEqual(client_context, 1000);
}

test "symmetric_run: recv timeout" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P = CreateTestProtocol("timeout", Exit);
    const R = Runner(P.A);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    // 客户端连接但什么都不发送——服务端 recv 应超时
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            var stream = try addr.connect(.{});
            defer stream.close();
            // 永远保持连接打开，不发送任何协议消息
            try zio.sleep(zio.Duration.fromSeconds(60));
        }
    }.run, .{listener.socket.address});

    var stream = try listener.accept(.{});
    defer stream.close();

    var ctx: i32 = 0;
    var ch: StreamChannel = undefined;
    try ch.init(allocator, stream, 128, 128, 4096);
    defer ch.deinit(allocator);

    // P.A 是客户端角色。服务端先 recv，客户端从不发送 → 超时
    // zio 的读取层会把 fiber 的 Canceled 转换为 ReadFailed
    try testing.expectError(error.ReadFailed, R.symmetric_run(.server, &ctx, &ch, P.A, 100));
    try testing.expectEqual(error.Canceled, ch.stream_reader.err.?);
}

test "tls channel: symmetric_run over encrypted channel" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;
    const crypto = std.crypto;

    var kp_seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&kp_seed);
    const kp_c = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);
    try zio.randomSecure(&kp_seed);
    const kp_s = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);


    const P = CreateTestProtocol("tls_proto", Exit);
    const R_pp = Runner(P.A);
    const R_tls = Runner(tls.ClientHello);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    var client_counter: i32 = 0;
    var server_counter: i32 = 0;

    const ClientTask = struct {
        fn run(
            addr: zio.net.Address,
            kp: crypto.sign.Ed25519.KeyPair,
            peer_pk: crypto.sign.Ed25519.PublicKey,
            counter: *i32,
        ) !void {
            var stream = try addr.connect(.{});
            defer stream.close();

            // 阶段 1：TLS 握手
            var tls_ctx = tls.ClientContext.init(kp, peer_pk);

            var sc: StreamChannel = undefined;
            try sc.init(allocator, stream, 256, 256, 4096);
            defer sc.deinit(allocator);
            try R_tls.symmetric_run(.client, &tls_ctx, &sc, tls.ClientHello, null);

            // 阶段 2：加密协议——复用 sc
            var tc: TlsChannel = undefined;
            try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
            defer tc.deinit(allocator);

            // 密钥已复制到 TlsChannel——清零握手上下文
            tls_ctx.deinit();

            try R_pp.symmetric_run(.client, counter, &tc, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(ClientTask.run, .{
        listener.socket.address,
        kp_c,
        kp_s.public_key,
        &client_counter,
    });

    var stream = try listener.accept(.{});
    defer stream.close();

    // 阶段 1：TLS 握手
    var tls_ctx = tls.ServerContext.init(kp_s, kp_c.public_key);
    var sc: StreamChannel = undefined;
    try sc.init(allocator, stream, 256, 256, 4096);
    defer sc.deinit(allocator);
    try R_tls.symmetric_run(.server, &tls_ctx, &sc, tls.ClientHello, null);

    // 阶段 2：加密协议——复用 sc
    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
    defer tc.deinit(allocator);

    // 密钥已复制到 TlsChannel——清零握手上下文
    tls_ctx.deinit();

    try R_pp.symmetric_run(.server, &server_counter, &tc, P.A, null);

    try group.wait();
    try testing.expectEqual(client_counter, 1000);
}

test "mux test" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P1 = CreateTestProtocol("p1", Exit);
    const R1 = Runner(P1.A);

    const P2 = CreateTestProtocol("p2", Exit);
    const R2 = Runner(P2.A);

    var client_context: i32 = 0;
    var server_context: i32 = 0;

    var client_context1: i32 = 0;
    var server_context1: i32 = 0;

    const protocol1: Protocol = .{
        .enter = P1.A,
        .runner = R1,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const protocol2: Protocol = .{
        .enter = P2.A,
        .runner = R2,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const TmpMux = Mux(&.{
        protocol1,
        protocol2,
    }, false);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try localhost.listen(.{});
    defer server.close();

    const S = struct {
        fn clientFn(
            server_address: zio.net.Address,
            gpa: std.mem.Allocator,
            ctxs: struct { *i32, *i32 },
        ) !void {
            var stream = try server_address.connect(.{});
            defer stream.close();

            var sc: StreamChannel = undefined;
            try sc.init(gpa, stream, 1024, 1024, 4096);
            defer sc.deinit(gpa);

            var mux: TmpMux = undefined;
            try mux.init(gpa, &sc, null);
            defer mux.deinit(gpa);

            try mux.run(.client, ctxs);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ server.socket.address, allocator, .{ &client_context, &client_context1 } });

    var stream = try server.accept(.{});
    defer stream.close();

    var sc: StreamChannel = undefined;
    try sc.init(allocator, stream, 1024, 1024, 4096);
    defer sc.deinit(allocator);

    var mux: TmpMux = undefined;
    try mux.init(allocator, &sc, null);
    defer mux.deinit(allocator);

    try mux.run(.server, .{ &server_context, &server_context1 });

    // 服务端跑完时客户端可能还在收 C 阶段的剩余消息,必须等客户端完成再断言。
    try group.wait();
    try testing.expectEqual(client_context, 1000);
    try testing.expectEqual(client_context1, 1000);
}

test "mux test encrypted" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;
    const crypto = std.crypto;

    var kp_seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&kp_seed);
    const kp_c = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);
    try zio.randomSecure(&kp_seed);
    const kp_s = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);

    const R_tls = Runner(tls.ClientHello);

    const P1 = CreateTestProtocol("p1", Exit);
    const R1 = Runner(P1.A);

    const P2 = CreateTestProtocol("p2", Exit);
    const R2 = Runner(P2.A);

    var client_context: i32 = 0;
    var server_context: i32 = 0;

    var client_context1: i32 = 0;
    var server_context1: i32 = 0;

    const protocol1: Protocol = .{
        .enter = P1.A,
        .runner = R1,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const protocol2: Protocol = .{
        .enter = P2.A,
        .runner = R2,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const TmpMux = Mux(&.{
        protocol1,
        protocol2,
    }, true);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try localhost.listen(.{});
    defer server.close();

    const S = struct {
        fn clientFn(
            server_address: zio.net.Address,
            gpa: std.mem.Allocator,
            ctxs: struct { *i32, *i32 },
            kp: crypto.sign.Ed25519.KeyPair,
            peer_pk: crypto.sign.Ed25519.PublicKey,
        ) !void {
            var stream = try server_address.connect(.{});
            defer stream.close();

            var sc: StreamChannel = undefined;
            try sc.init(gpa, stream, 256, 256, 4096);
            defer sc.deinit(gpa);

            // 阶段 1：TLS 握手
            var tls_ctx = tls.ClientContext.init(kp, peer_pk);
            try R_tls.symmetric_run(.client, &tls_ctx, &sc, tls.ClientHello, null);

            // 阶段 2：加密 Mux——复用 sc，密钥来自握手
            var mux: TmpMux = undefined;
            try mux.init(gpa, &sc, .{ .write_key = tls_ctx.write_key, .read_key = tls_ctx.read_key });
            tls_ctx.deinit();
            defer mux.deinit(gpa);

            try mux.run(.client, ctxs);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{
        server.socket.address,
        allocator,
        .{ &client_context, &client_context1 },
        kp_c,
        kp_s.public_key,
    });

    var stream = try server.accept(.{});
    defer stream.close();

    var sc: StreamChannel = undefined;
    try sc.init(allocator, stream, 256, 256, 4096);
    defer sc.deinit(allocator);

    // 阶段 1：TLS 握手
    var tls_ctx = tls.ServerContext.init(kp_s, kp_c.public_key);
    try R_tls.symmetric_run(.server, &tls_ctx, &sc, tls.ClientHello, null);

    // 阶段 2：加密 Mux——复用 sc，密钥来自握手
    var mux: TmpMux = undefined;
    try mux.init(allocator, &sc, .{ .write_key = tls_ctx.write_key, .read_key = tls_ctx.read_key });
    tls_ctx.deinit();
    defer mux.deinit(allocator);

    try mux.run(.server, .{ &server_context, &server_context1 });

    // 服务端跑完时客户端可能还在收 C 阶段的剩余消息,必须等客户端完成再断言。
    try group.wait();
    try testing.expectEqual(client_context, 1000);
    try testing.expectEqual(client_context1, 1000);
}
