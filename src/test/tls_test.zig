const std = @import("std");
const crypto = std.crypto;
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const Runner = polyrole.runner.Runner;
const tls = polyrole.tls;
const types = polyrole.tls;
const InMemoryChannel = polyrole.channel.InMemoryChannel;
const HalfChannel = polyrole.channel.HalfChannel;

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

// HKDF-Extract/Expand 密钥派生函数正确性
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
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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

    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);
    sh.to_client.data.mac = [_]u8{0} ** 32;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.HmacInvalid, err);
}

// 服务端提供全零临时公钥 → X25519 scalarmult 返回错误 → 映射为 DhFailed
test "handshake: invalid ephemeral public key → DhFailed" {
    const testing = std.testing;
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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
    const kp_s = try ed25519KeyPair();
    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);
    const sh = try tls.ServerHello.process(&server);
    try tls.ServerHello.preprocess(&client, sh);

    var cf = try tls.ClientFinished.process(&client);
    cf.close.data.mac = [_]u8{0} ** 32;

    const err = tls.ClientFinished.preprocess(&server, cf);
    try testing.expectError(error.HmacInvalid, err);
}

// MITM 替换 Client 临时公钥 → Server 签名基于不同的 t1
// → Client 重建 t1 不匹配 → SignatureInvalid
test "handshake: swapped client ephemeral pk → SignatureInvalid" {
    const testing = std.testing;
    const kp_s = try ed25519KeyPair();

    var client = types.ClientContext.init(kp_s.public_key);
    var server = types.ServerContext.init(kp_s);

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
    const kp_s = try ed25519KeyPair();

    var client = types.ClientContext.init(kp_s.public_key);

    // Round 1: 生成 ServerHello
    const ch1 = try tls.ClientHello.process(&client);
    var s1 = types.ServerContext.init(kp_s);
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

    const kp_s1 = try ed25519KeyPair();
    const kp_s2 = try ed25519KeyPair();

    var c1 = types.ClientContext.init(kp_s1.public_key);
    var s1 = types.ServerContext.init(kp_s1);
    var c2 = types.ClientContext.init(kp_s2.public_key);
    var s2 = types.ServerContext.init(kp_s2);

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

// ─────────────────── ClientFinished 负向测试 ───────────────────

fn testSha256(parts: []const []const u8) [32]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    for (parts) |p| h.update(p);
    return h.finalResult();
}

fn testHmacSha256(key: *const [32]u8, label: []const u8, msg: *const [32]u8) [32]u8 {
    var buf: [64]u8 = undefined;
    @memcpy(buf[0..label.len], label);
    @memcpy(buf[label.len..][0..msg.len], msg);
    var out: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&out, buf[0 .. label.len + msg.len], key);
    return out;
}

/// 包装 InMemoryChannel，可在 server 端 recv ClientFinished 时注入篡改消息。
const InjectChannel = struct {
    inner: *InMemoryChannel,
    server_ctx: *types.ServerContext,
    /// 注入用 "server_fin" 标签（错误域）计算的 mac
    inject_wrong_label: bool = false,
    /// 注入旧会话捕获的完整 ClientFinished 消息
    inject_old_finished: ?tls.ClientFinished = null,

    pub fn send(self: *@This(), state_id: anytype, T: type, val: anytype) !void {
        try self.inner.send(state_id, T, val);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        if (comptime T == tls.ClientFinished) {
            if (self.inject_old_finished) |f| return f;
            if (self.inject_wrong_label) {
                const ctx = self.server_ctx;
                const t1 = testSha256(&[_][]const u8{ &ctx.peer_nonce, &ctx.peer_ephemeral_pk, &ctx.own_nonce, &ctx.own_ephemeral_pk });
                const t2 = testSha256(&[_][]const u8{ &t1, &ctx.own_signature });
                const t3 = testSha256(&[_][]const u8{ &t2, &ctx.own_mac });
                const wrong = testHmacSha256(&ctx.handshake_key, "server_fin", &t3);
                return .{ .close = .{ .data = .{ .mac = wrong } } };
            }
        }
        return self.inner.recv(state_id, T);
    }
};

/// 包装 InMemoryChannel，在 client 端捕获其发送的 ClientFinished 消息。
const CaptureChannel = struct {
    inner: *InMemoryChannel,
    captured_finished: ?tls.ClientFinished = null,

    pub fn send(self: *@This(), state_id: anytype, T: type, val: anytype) !void {
        if (comptime T == tls.ClientFinished) {
            self.captured_finished = val;
        }
        try self.inner.send(state_id, T, val);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        return self.inner.recv(state_id, T);
    }
};

const ChannelPair = struct {
    a: InMemoryChannel,
    b: InMemoryChannel,
    h1: *HalfChannel,
    h2: *HalfChannel,
    gpa: std.mem.Allocator,

    fn deinit(self: *@This()) void {
        self.h1.deinit(self.gpa);
        self.h2.deinit(self.gpa);
        self.gpa.destroy(self.h1);
        self.gpa.destroy(self.h2);
    }
};

/// HalfChannel 必须堆分配——InMemoryChannel 持有其指针，栈上生命周期不足。
fn makeHandshakeChannelPair(gpa: std.mem.Allocator) !ChannelPair {
    const h1 = try gpa.create(HalfChannel);
    errdefer gpa.destroy(h1);
    const h2 = try gpa.create(HalfChannel);
    errdefer gpa.destroy(h2);
    try h1.init(gpa, 512);
    try h2.init(gpa, 512);
    return .{
        .a = InMemoryChannel{ .max_slice_len = 4096, .half_self = h1, .half_peer = h2 },
        .b = InMemoryChannel{ .max_slice_len = 4096, .half_self = h2, .half_peer = h1 },
        .h1 = h1,
        .h2 = h2,
        .gpa = gpa,
    };
}

test "handshake: replayed ClientFinished from previous session is rejected" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const kp_s = try ed25519KeyPair();

    const R = Runner(tls.ClientHello);

    const ClientRunner = struct {
        fn run(ch: *CaptureChannel, ctx: *types.ClientContext) !void {
            try R.symmetric_run(.client, ctx, ch, tls.ClientHello, null);
        }
    };

    // 会话 1：完整握手，捕获 client 发送的 ClientFinished
    var ch_pair1 = try makeHandshakeChannelPair(allocator);
    defer ch_pair1.deinit();
    var c1 = types.ClientContext.init(kp_s.public_key);
    var s1 = types.ServerContext.init(kp_s);
    var cap_ch = CaptureChannel{ .inner = &ch_pair1.a };
    defer c1.deinit();
    defer s1.deinit();

    var client_task = try zio.spawn(ClientRunner.run, .{ &cap_ch, &c1 });
    try R.symmetric_run(.server, &s1, &ch_pair1.b, tls.ClientHello, null);
    try client_task.join();

    const old_finished = cap_ch.captured_finished.?;

    // 会话 2：把旧 ClientFinished 注入 server 端 → 跨会话重放被拒绝
    var ch_pair2 = try makeHandshakeChannelPair(allocator);
    defer ch_pair2.deinit();
    var c2 = types.ClientContext.init(kp_s.public_key);
    var s2 = types.ServerContext.init(kp_s);
    defer c2.deinit();
    defer s2.deinit();
    var inj_ch = InjectChannel{
        .inner = &ch_pair2.b,
        .server_ctx = &s2,
        .inject_old_finished = old_finished,
    };

    var cap_ch2 = CaptureChannel{ .inner = &ch_pair2.a };
    var client_task2 = try zio.spawn(ClientRunner.run, .{ &cap_ch2, &c2 });
    try testing.expectError(error.HmacInvalid, R.symmetric_run(.server, &s2, &inj_ch, tls.ClientHello, null));
    try client_task2.join();
}

test "handshake: client_fin MAC domain separation" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const kp_s = try ed25519KeyPair();

    const R = Runner(tls.ClientHello);

    var ch_pair = try makeHandshakeChannelPair(allocator);
    defer ch_pair.deinit();
    var c = types.ClientContext.init(kp_s.public_key);
    var s = types.ServerContext.init(kp_s);
    defer c.deinit();
    defer s.deinit();
    const ClientRunner = struct {
        fn run(ch: *InMemoryChannel, ctx: *types.ClientContext) !void {
            try R.symmetric_run(.client, ctx, ch, tls.ClientHello, null);
        }
    };

    var inj_ch = InjectChannel{
        .inner = &ch_pair.b,
        .server_ctx = &s,
        .inject_wrong_label = true,
    };

    var client_task = try zio.spawn(ClientRunner.run, .{ &ch_pair.a, &c });
    try testing.expectError(error.HmacInvalid, R.symmetric_run(.server, &s, &inj_ch, tls.ClientHello, null));
    try client_task.join();
}
