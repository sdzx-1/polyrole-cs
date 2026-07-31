const std = @import("std");
const crypto = std.crypto;
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const types = @import("context.zig");

const TlsInfo = ProtocolInfo("simple_tls", types.ClientContext, types.ServerContext);

const TlsError = error{
    /// 系统熵不可用
    EntropyUnavailable,
    /// DH 密钥协商失败（无效的对端公钥）
    DhFailed,
    /// Ed25519 签名验证失败
    SignatureInvalid,
    /// HMAC 验证失败（握手密钥错误或转录被篡改）
    HmacInvalid,
};

// ─────────────────── 载荷类型 ───────────────────

const ClientHelloPayload = struct {
    nonce: [24]u8,
    ephemeral_pk: [32]u8,
};

const ServerHelloPayload = struct {
    nonce: [24]u8,
    ephemeral_pk: [32]u8,
    signature: [64]u8,
    mac: [32]u8,
};

const ClientFinishedPayload = struct {
    signature: [64]u8,
    mac: [32]u8,
};

// ─────────────────── SHA256 辅助函数 ───────────────────

fn sha256(parts: anytype) [32]u8 {
    var h = crypto.hash.sha2.Sha256.init(.{});
    inline for (parts) |part| {
        h.update(part);
    }
    var out: [32]u8 = undefined;
    h.final(&out);
    return out;
}

fn randomBytes(comptime n: usize) ![n]u8 {
    var buf: [n]u8 = undefined;
    try zio.randomSecure(&buf);
    return buf;
}

fn generateX25519Keypair() !crypto.dh.X25519.KeyPair {
    var seed: [crypto.dh.X25519.seed_length]u8 = undefined;
    try zio.randomSecure(&seed);
    return try crypto.dh.X25519.KeyPair.generateDeterministic(seed);
}

// ─────────────────── 步骤 1：ClientHello ───────────────────

pub const ClientHello = union(enum) {
    to_server: Data(ClientHelloPayload, ServerHello),

    pub const info: TlsInfo = .{ .agent = .client, .name = "ClientHello" };

    pub fn process(ctx: *types.ClientContext) !@This() {
        const nonce = try randomBytes(24);
        const kp = try generateX25519Keypair();

        ctx.own_nonce = nonce;
        ctx.ephemeral_sk = kp.secret_key;
        ctx.own_ephemeral_pk = kp.public_key;

        return .{ .to_server = .{ .data = .{
            .nonce = nonce,
            .ephemeral_pk = kp.public_key,
        } } };
    }

    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        switch (result) {
            .to_server => |d| {
                ctx.peer_nonce = d.data.nonce;
                ctx.peer_ephemeral_pk = d.data.ephemeral_pk;
            },
        }
    }
};

// ─────────────────── 步骤 2：ServerHello ───────────────────

pub const ServerHello = union(enum) {
    to_client: Data(ServerHelloPayload, ClientFinished),

    pub const info: TlsInfo = .{ .agent = .server, .name = "ServerHello" };

    pub fn process(ctx: *types.ServerContext) !@This() {
        const nonce = try randomBytes(24);
        const kp = try generateX25519Keypair();

        const shared_secret = crypto.dh.X25519.scalarmult(kp.secret_key, ctx.peer_ephemeral_pk) catch
            return error.DhFailed;
        ctx.shared_secret = shared_secret;

        const keys = types.deriveKeys(shared_secret);
        ctx.handshake_key = keys.handshake_key;

        const t1 = sha256(.{ &ctx.peer_nonce, &ctx.peer_ephemeral_pk, &nonce, &kp.public_key });
        const signature = try sign(ctx.id_keypair, &t1);
        const t2 = sha256(.{ &t1, &signature });
        const mac = hmacSha256(&ctx.handshake_key, "server_fin", &t2);

        ctx.own_nonce = nonce;
        ctx.ephemeral_sk = kp.secret_key;
        ctx.own_ephemeral_pk = kp.public_key;
        ctx.own_signature = signature;
        ctx.own_mac = mac;

        return .{ .to_client = .{ .data = .{
            .nonce = nonce,
            .ephemeral_pk = kp.public_key,
            .signature = signature,
            .mac = mac,
        } } };
    }

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) !void {
        switch (result) {
            .to_client => |d| {
                const payload = d.data;

                const shared_secret = crypto.dh.X25519.scalarmult(ctx.ephemeral_sk, payload.ephemeral_pk) catch
                    return error.DhFailed;
                ctx.shared_secret = shared_secret;

                const keys = types.deriveKeys(shared_secret);
                ctx.handshake_key = keys.handshake_key;

                const t1 = sha256(.{ &ctx.own_nonce, &ctx.own_ephemeral_pk, &payload.nonce, &payload.ephemeral_pk });
                try verifySignature(payload.signature, &t1, ctx.peer_id_public);
                const t2 = sha256(.{ &t1, &payload.signature });
                try verifyHmac(&ctx.handshake_key, "server_fin", &t2, payload.mac);

                ctx.peer_nonce = payload.nonce;
                ctx.peer_ephemeral_pk = payload.ephemeral_pk;
                ctx.peer_signature = payload.signature;
                ctx.peer_mac = payload.mac;
            },
        }
    }
};

// ─────────────────── 步骤 3：ClientFinished ───────────────────

pub const ClientFinished = union(enum) {
    close: Data(ClientFinishedPayload, Exit),

    pub const info: TlsInfo = .{ .agent = .client, .name = "ClientFinished" };

    pub fn process(ctx: *types.ClientContext) !@This() {
        const keys = types.deriveKeys(ctx.shared_secret);

        const t1 = sha256(.{ &ctx.own_nonce, &ctx.own_ephemeral_pk, &ctx.peer_nonce, &ctx.peer_ephemeral_pk });
        const t2 = sha256(.{ &t1, &ctx.peer_signature });
        const t3 = sha256(.{ &t2, &ctx.peer_mac });
        const signature = try sign(ctx.id_keypair, &t3);
        const t4 = sha256(.{ &t3, &signature });
        const mac = hmacSha256(&ctx.handshake_key, "client_fin", &t4);

        ctx.write_key = keys.client_write_key;
        ctx.read_key = keys.server_write_key;

        return .{ .close = .{ .data = .{
            .signature = signature,
            .mac = mac,
        } } };
    }

    pub fn preprocess(ctx: *types.ServerContext, result: @This()) !void {
        const payload = result.close.data;

        const t1 = sha256(.{ &ctx.peer_nonce, &ctx.peer_ephemeral_pk, &ctx.own_nonce, &ctx.own_ephemeral_pk });
        const t2 = sha256(.{ &t1, &ctx.own_signature });
        const t3 = sha256(.{ &t2, &ctx.own_mac });
        try verifySignature(payload.signature, &t3, ctx.peer_id_public);
        const t4 = sha256(.{ &t3, &payload.signature });
        try verifyHmac(&ctx.handshake_key, "client_fin", &t4, payload.mac);

        const keys = types.deriveKeys(ctx.shared_secret);
        ctx.read_key = keys.client_write_key;
        ctx.write_key = keys.server_write_key;
    }
};

// ─────────────────── 密码学辅助函数 ───────────────────

fn sign(kp: crypto.sign.Ed25519.KeyPair, msg: []const u8) ![64]u8 {
    const sig = try kp.sign(msg, null);
    return sig.toBytes();
}

fn verifySignature(sig_bytes: [64]u8, msg: []const u8, pubkey: crypto.sign.Ed25519.PublicKey) TlsError!void {
    const sig = crypto.sign.Ed25519.Signature.fromBytes(sig_bytes);
    sig.verify(msg, pubkey) catch return error.SignatureInvalid;
}

fn hmacSha256(key: *const [32]u8, comptime label: []const u8, msg: *const [32]u8) [32]u8 {
    var buf: [label.len + 32]u8 = undefined;
    @memcpy(buf[0..label.len], label);
    @memcpy(buf[label.len..], msg);
    const total = buf[0 .. label.len + 32];
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, total, key);
    return out;
}

fn verifyHmac(key: *const [32]u8, comptime label: []const u8, msg: *const [32]u8, expected: [32]u8) TlsError!void {
    const got = hmacSha256(key, label, msg);
    if (!crypto.timing_safe.eql([32]u8, got, expected)) {
        return error.HmacInvalid;
    }
}
