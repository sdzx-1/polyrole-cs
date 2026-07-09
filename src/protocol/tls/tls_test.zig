const std = @import("std");
const crypto = std.crypto;
const polyrole = @import("../../root.zig");
const Runner = polyrole.runner.Runner;
const tls = @import("tls.zig");
const types = @import("types.zig");

fn initClientCtx(io: std.Io, kp: crypto.sign.Ed25519.KeyPair, server_pk: crypto.sign.Ed25519.PublicKey) types.ClientContext {
    return .{
        .io = io,
        .id_keypair = kp,
        .peer_id_public = server_pk,
        .ephemeral_sk = undefined,
        .own_ephemeral_pk = undefined,
        .own_nonce = undefined,
        .peer_nonce = undefined,
        .peer_ephemeral_pk = undefined,
        .peer_signature = undefined,
        .peer_mac = undefined,
        .shared_secret = undefined,
        .handshake_key = undefined,
        .write_key = undefined,
        .read_key = undefined,
    };
}

fn initServerCtx(io: std.Io, kp: crypto.sign.Ed25519.KeyPair, client_pk: crypto.sign.Ed25519.PublicKey) types.ServerContext {
    return .{
        .io = io,
        .id_keypair = kp,
        .peer_id_public = client_pk,
        .ephemeral_sk = undefined,
        .own_ephemeral_pk = undefined,
        .peer_ephemeral_pk = undefined,
        .peer_nonce = undefined,
        .own_nonce = undefined,
        .shared_secret = undefined,
        .handshake_key = undefined,
        .own_signature = undefined,
        .own_mac = undefined,
        .read_key = undefined,
        .write_key = undefined,
    };
}

// HKDF 密钥派生自测试：验证 Extract + Expand 流程正确性
test "hkdf" {
    _ = @import("types.zig");
}

// 纯握手：客户端和服务端完成三次握手后正常退出
test "simulate handshake only" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);
}

// 通过 TCP 网络通道运行完整握手：验证编解码 + 网络传输
test "symmetric run handshake" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const localhost: net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try localhost.listen(io, .{});
    defer listener.deinit(io);

    const StreamChannel = polyrole.channel.StreamChannel;
    const R = Runner(tls.ClientHello);

    const S = struct {
        fn clientFn(address: net.IpAddress, ctx: *types.ClientContext) !void {
            var stream = try address.connect(io, .{ .mode = .stream });
            defer stream.close(io);

            var ch: StreamChannel = undefined;
            try ch.init(io, allocator, stream, 256, 256);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, tls.ClientHello);
        }
    };

    var client_task = try io.concurrent(S.clientFn, .{ listener.socket.address, &client });
    defer client_task.cancel(io) catch {};

    var stream = try listener.accept(io);
    defer stream.close(io);

    var ch: StreamChannel = undefined;
    try ch.init(io, allocator, stream, 256, 256);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, tls.ClientHello);
}

// 篡改服务端签名：客户端验证 ServerHello 签名时应返回 SignatureInvalid
test "handshake: tampered server signature → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);
    sh.to_client.data.signature = [_]u8{0} ** 64;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.SignatureInvalid, err);
}

// 篡改服务端 MAC：签名正确但 HMAC 不匹配，应返回 HmacInvalid
test "handshake: tampered server MAC → HmacInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);
    sh.to_client.data.mac = [_]u8{0} ** 32;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.HmacInvalid, err);
}

// 篡改客户端签名：服务端验证 ClientFinished 签名时应返回 SignatureInvalid
test "handshake: tampered client signature → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);
    const sh = try tls.ServerHello.process(&server);
    try tls.ServerHello.preprocess(&client, sh);

    var cf = try tls.ClientFinished.process(&client);
    cf.close.data.signature = [_]u8{0} ** 64;

    const err = tls.ClientFinished.preprocess(&server, cf);
    try testing.expectError(error.SignatureInvalid, err);
}

// 服务端提供全零临时公钥 → X25519 scalarmult 返回错误 → 映射为 DhFailed
test "handshake: invalid ephemeral public key → DhFailed" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);
    sh.to_client.data.ephemeral_pk = [_]u8{0} ** 32;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.DhFailed, err);
}

// 多会话复用：两轮握手产生不同的 write_key，验证 session 隔离
test "simulate multiple sessions with same contexts" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);

    // Session 1
    try R.simulate(&client, &server, tls.ClientHello);
    const key1 = client.write_key;

    // Session 2 — 新的临时密钥对，write_key 应不同
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expect(!std.mem.eql(u8, &key1, &client.write_key));
}
