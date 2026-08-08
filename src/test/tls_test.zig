const std = @import("std");
const crypto = std.crypto;
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const Runner = polyrole.runner.Runner;
const tls = polyrole.tls;
const types = polyrole.tls;

fn ed25519KeyPair() !crypto.sign.Ed25519.KeyPair {
    var seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&seed);
    return try crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
}

fn x25519KeyPair() !crypto.dh.X25519.KeyPair {
    var seed: [crypto.dh.X25519.seed_length]u8 = undefined;
    try zio.randomSecure(&seed);
    return try crypto.dh.X25519.KeyPair.generateDeterministic(seed);
}

test "hkdf" {
    const testing = std.testing;
    const ikm = [_]u8{0x0b} ** 32;
    const salt = [_]u8{0} ** 32;
    const prk = types.hkdf_extract(salt, ikm);
    const key = types.hkdf_expand(prk, "test");
    try testing.expect(key.len == 32);
}

// 纯握手：客户端和服务端完成三次握手后正常退出
test "simulate handshake only" {
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    client.deinit();
    server.deinit();
}

// 通过 TCP 网络通道运行完整握手：验证编解码 + 网络传输
test "symmetric run handshake" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    const StreamChannel = polyrole.channel.StreamChannel;
    const R = Runner(tls.ClientHello);

    const S = struct {
        fn clientFn(address: zio.net.Address, ctx: *types.ClientContext) !void {
            var stream = try address.connect(.{});
            defer stream.close();

            var ch: StreamChannel = undefined;
            try ch.init(allocator, stream, 256, 256, 4096);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, tls.ClientHello, null);
            ctx.deinit();
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ listener.socket.address, &client });

    var stream = try listener.accept(.{});
    defer stream.close();

    var ch: StreamChannel = undefined;
    try ch.init(allocator, stream, 256, 256, 4096);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, tls.ClientHello, null);
    server.deinit();
}

// 篡改服务端签名：客户端验证 ServerHello 签名时应返回 SignatureInvalid
test "handshake: tampered server signature → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

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
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

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
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

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
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

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
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);

    // 会话 1
    try R.simulate(&client, &server, tls.ClientHello);
    const key1 = client.write_key;

    // 会话 2——新的临时密钥对，write_key 应不同
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expect(!std.mem.eql(u8, &key1, &client.write_key));

    client.deinit();
    server.deinit();
}

// 篡改客户端 MAC：客户端签名正确但 HMAC 不匹配，应返回 HmacInvalid
test "handshake: tampered client MAC → HmacInvalid" {
    const testing = std.testing;
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);
    const sh = try tls.ServerHello.process(&server);
    try tls.ServerHello.preprocess(&client, sh);

    var cf = try tls.ClientFinished.process(&client);
    cf.close.data.mac = [_]u8{0} ** 32;

    const err = tls.ClientFinished.preprocess(&server, cf);
    try testing.expectError(error.HmacInvalid, err);
}

// Client 使用错误身份密钥（非 server 信任的公钥对应的私钥）
// → server 端 ClientFinished.preprocess 签名验证失败 → SignatureInvalid
test "handshake: wrong client identity key → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = try ed25519KeyPair();
    const kp_c_rogue = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();

    // Server 信任 kp_c，但 client 用 kp_c_rogue 签名
    var client = types.ClientContext.init(kp_c_rogue, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);
    const sh = try tls.ServerHello.process(&server);
    try tls.ServerHello.preprocess(&client, sh);

    const err = tls.ClientFinished.process(&client);
    // 签名本身不会失败，但 server 验证时用 kp_c.public_key 验证 kp_c_rogue 的签名
    const cf = try err;
    const verify_err = tls.ClientFinished.preprocess(&server, cf);
    try testing.expectError(error.SignatureInvalid, verify_err);
}

// MITM 替换 Client 临时公钥 → Server 签名基于不同的 t1
// → Client 重建 t1 不匹配 → SignatureInvalid
test "handshake: swapped client ephemeral pk → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();

    var client = types.ClientContext.init(kp_c, kp_s.public_key);
    var server = types.ServerContext.init(kp_s, kp_c.public_key);

    var ch = try tls.ClientHello.process(&client);
    // MITM 替换临时公钥
    const fake_kp = try x25519KeyPair();
    ch.to_server.data.ephemeral_pk = fake_kp.public_key;

    tls.ClientHello.preprocess(&server, ch);

    const sh = try tls.ServerHello.process(&server);
    // Server 的 t1 = SHA256(cn || fake_epk || sn || epk_s)
    // Client 的 t1 = SHA256(cn || real_epk || sn || epk_s) → 签名不匹配
    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.SignatureInvalid, err);
}

// ServerHello 跨会话重放：
// Client 先后执行两次 ClientHello（临时密钥不同），
// 将第一次的 ServerHello 重放到第二次 → t1 不匹配 → SignatureInvalid
test "handshake: replayed ServerHello from previous session → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = try ed25519KeyPair();
    const kp_s = try ed25519KeyPair();

    var client = types.ClientContext.init(kp_c, kp_s.public_key);

    // Round 1: 生成 ServerHello
    const ch1 = try tls.ClientHello.process(&client);
    var s1 = types.ServerContext.init(kp_s, kp_c.public_key);
    tls.ClientHello.preprocess(&s1, ch1);
    const sh1 = try tls.ServerHello.process(&s1);
    try tls.ServerHello.preprocess(&client, sh1);

    // Round 2: Client 生成新临时密钥，攻击者重放 sh1
    _ = try tls.ClientHello.process(&client);
    const err = tls.ServerHello.preprocess(&client, sh1);
    try testing.expectError(error.SignatureInvalid, err);
}

// 不同身份密钥对的握手互不干扰
test "simulate: two handshakes with different keypairs produce different keys" {
    const testing = std.testing;

    const kp_c1 = try ed25519KeyPair();
    const kp_s1 = try ed25519KeyPair();
    const kp_c2 = try ed25519KeyPair();
    const kp_s2 = try ed25519KeyPair();

    var c1 = types.ClientContext.init(kp_c1, kp_s1.public_key);
    var s1 = types.ServerContext.init(kp_s1, kp_c1.public_key);
    var c2 = types.ClientContext.init(kp_c2, kp_s2.public_key);
    var s2 = types.ServerContext.init(kp_s2, kp_c2.public_key);

    const R = Runner(tls.ClientHello);

    try R.simulate(&c1, &s1, tls.ClientHello);
    try R.simulate(&c2, &s2, tls.ClientHello);

    try testing.expect(!std.mem.eql(u8, &c1.write_key, &c2.write_key));
    try testing.expect(!std.mem.eql(u8, &s1.write_key, &s2.write_key));

    c1.deinit();
    s1.deinit();
    c2.deinit();
    s2.deinit();
}
