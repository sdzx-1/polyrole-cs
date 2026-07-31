const std = @import("std");
const crypto = std.crypto;

pub const ClientContext = struct {
    /// 自己的 Ed25519 身份密钥对
    id_keypair: crypto.sign.Ed25519.KeyPair,
    /// 服务端的 Ed25519 身份公钥
    peer_id_public: crypto.sign.Ed25519.PublicKey,

    /// 自己的临时 X25519 私钥（在 ClientHello 中生成）
    ephemeral_sk: [32]u8,
    /// 自己的临时 X25519 公钥
    own_ephemeral_pk: [32]u8,
    /// 自己的 nonce（在 ClientHello 中生成）
    own_nonce: [24]u8,

    /// 对端的 nonce（在 ServerHello 中收到）
    peer_nonce: [24]u8,
    /// 对端的临时 X25519 公钥（在 ServerHello 中收到）
    peer_ephemeral_pk: [32]u8,

    /// 对端的 Ed25519 签名（在 ServerHello 中收到）
    peer_signature: [64]u8,
    /// 对端的 Finished MAC（在 ServerHello 中收到）
    peer_mac: [32]u8,

    /// X25519 共享密钥
    shared_secret: [32]u8,
    /// 由 shared_secret 通过 HKDF 派生
    handshake_key: [32]u8,

    /// 派生的应用密钥（用于 TlsChannel 写入）
    write_key: [32]u8,
    /// 派生的应用密钥（用于 TlsChannel 读取）
    read_key: [32]u8,

    pub fn init(id_keypair: crypto.sign.Ed25519.KeyPair, peer_id_public: crypto.sign.Ed25519.PublicKey) ClientContext {
        return .{
            .id_keypair = id_keypair,
            .peer_id_public = peer_id_public,
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

    /// 清零敏感密钥材料。握手完成且密钥已复制到 TlsChannel（若使用）后调用。
    pub fn deinit(self: *ClientContext) void {
        @memset(&self.ephemeral_sk, 0);
        @memset(&self.shared_secret, 0);
        @memset(&self.handshake_key, 0);
        @memset(&self.write_key, 0);
        @memset(&self.read_key, 0);
    }
};

pub const ServerContext = struct {
    /// 自己的 Ed25519 身份密钥对
    id_keypair: crypto.sign.Ed25519.KeyPair,
    /// 客户端的 Ed25519 身份公钥
    peer_id_public: crypto.sign.Ed25519.PublicKey,

    /// 自己的临时 X25519 私钥（在 ServerHello 中生成）
    ephemeral_sk: [32]u8,
    /// 自己的临时 X25519 公钥
    own_ephemeral_pk: [32]u8,
    /// 对端的临时 X25519 公钥（在 ClientHello 中收到）
    peer_ephemeral_pk: [32]u8,

    /// 对端的 nonce（在 ClientHello 中收到）
    peer_nonce: [24]u8,
    /// 自己的 nonce（在 ServerHello 中生成）
    own_nonce: [24]u8,

    /// X25519 共享密钥
    shared_secret: [32]u8,
    /// 由 shared_secret 通过 HKDF 派生
    handshake_key: [32]u8,
    /// 自己的 Ed25519 签名（在 ServerHello 中计算，用于 ClientFinished 转录）
    own_signature: [64]u8,
    /// 自己的 Finished MAC（在 ServerHello 中计算，用于 ClientFinished 转录）
    own_mac: [32]u8,

    /// 派生的应用密钥（用于 TlsChannel 读取）
    read_key: [32]u8,
    /// 派生的应用密钥（用于 TlsChannel 写入）
    write_key: [32]u8,

    pub fn init(id_keypair: crypto.sign.Ed25519.KeyPair, peer_id_public: crypto.sign.Ed25519.PublicKey) ServerContext {
        return .{
            .id_keypair = id_keypair,
            .peer_id_public = peer_id_public,
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

    /// 清零敏感密钥材料。握手完成且密钥已复制到 TlsChannel（若使用）后调用。
    pub fn deinit(self: *ServerContext) void {
        @memset(&self.ephemeral_sk, 0);
        @memset(&self.shared_secret, 0);
        @memset(&self.handshake_key, 0);
        @memset(&self.write_key, 0);
        @memset(&self.read_key, 0);
    }
};

/// HKDF-Extract：prk = HMAC-SHA256(salt, ikm)
fn hkdf_extract(salt: [32]u8, ikm: [32]u8) [32]u8 {
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, &ikm, &salt);
    return out;
}

/// HKDF-Expand：okm = HMAC-SHA256(prk, info || 0x01)[0..L]
///
/// 目前仅支持单次迭代（一次 HMAC 调用）。返回类型 `[32]u8` 在类型层面
/// 强制这一点——如果将来需要更大输出，调用方必须实现 RFC 5869 §2.3
/// 的多轮迭代链。
inline fn hkdf_expand(prk: [32]u8, info: []const u8) [32]u8 {
    var buf: [info.len + 1]u8 = undefined;
    @memcpy(buf[0..info.len], info);
    buf[info.len] = 0x01;
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, &buf, &prk);
    return out;
}

test "hkdf" {
    const testing = std.testing;
    const ikm = [_]u8{0x0b} ** 32;
    const salt = [_]u8{0} ** 32;
    const prk = hkdf_extract(salt, ikm);
    const key = hkdf_expand(prk, "test");
    try testing.expect(key.len == 32);
}

pub fn deriveKeys(shared_secret: [32]u8) struct {
    handshake_key: [32]u8,
    client_write_key: [32]u8,
    server_write_key: [32]u8,
} {
    const salt = [_]u8{0} ** 32;
    const prk = hkdf_extract(salt, shared_secret);
    return .{
        .handshake_key = hkdf_expand(prk, "hs"),
        .client_write_key = hkdf_expand(prk, "c2s"),
        .server_write_key = hkdf_expand(prk, "s2c"),
    };
}
