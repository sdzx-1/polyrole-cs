const std = @import("std");
const crypto = std.crypto;

/// Maximum plaintext size for a single data-phase message.
pub const max_msg_size = 1024;

pub const ClientContext = struct {
    /// Io interface for CSPRNG
    io: std.Io,
    /// Own Ed25519 identity keypair
    id_keypair: crypto.sign.Ed25519.KeyPair,
    /// Server's Ed25519 identity public key
    peer_id_public: crypto.sign.Ed25519.PublicKey,

    /// Own ephemeral X25519 secret key (generated in ClientHello)
    ephemeral_sk: [32]u8,
    /// Own ephemeral X25519 public key
    own_ephemeral_pk: [32]u8,
    /// Own nonce (generated in ClientHello)
    own_nonce: [24]u8,

    /// Peer's nonce (received in ServerHello)
    peer_nonce: [24]u8,
    /// Peer's ephemeral X25519 public key (received in ServerHello)
    peer_ephemeral_pk: [32]u8,

    /// Peer's Ed25519 signature (received in ServerHello)
    peer_signature: [64]u8,
    /// Peer's Finished MAC (received in ServerHello)
    peer_mac: [32]u8,

    /// X25519 shared secret
    shared_secret: [32]u8,
    /// Derived from shared_secret via HKDF
    handshake_key: [32]u8,

    /// Derived application key: encrypts ClientData
    write_key: [32]u8,
    /// Derived application key: decrypts ServerData
    read_key: [32]u8,

    /// Buffer for encrypting outbound data-phase messages
    encrypted_buf: [max_msg_size + 16]u8,

    /// Plaintext to send (set by application before calling Runner)
    send_buffer: []const u8,
    /// Buffer for received plaintext (set by application)
    recv_buffer: []u8,
};

pub const ServerContext = struct {
    /// Io interface for CSPRNG
    io: std.Io,
    /// Own Ed25519 identity keypair
    id_keypair: crypto.sign.Ed25519.KeyPair,
    /// Client's Ed25519 identity public key
    peer_id_public: crypto.sign.Ed25519.PublicKey,

    /// Own ephemeral X25519 secret key (generated in ServerHello)
    ephemeral_sk: [32]u8,
    /// Own ephemeral X25519 public key
    own_ephemeral_pk: [32]u8,
    /// Peer's ephemeral X25519 public key (received in ClientHello)
    peer_ephemeral_pk: [32]u8,

    /// Peer's nonce (received in ClientHello)
    peer_nonce: [24]u8,
    /// Own nonce (generated in ServerHello)
    own_nonce: [24]u8,

    /// X25519 shared secret
    shared_secret: [32]u8,
    /// Derived from shared_secret via HKDF
    handshake_key: [32]u8,
    /// Own Ed25519 signature (computed in ServerHello, used in ClientFinished transcript)
    own_signature: [64]u8,
    /// Own Finished MAC (computed in ServerHello, used in ClientFinished transcript)
    own_mac: [32]u8,

    /// Derived application key: decrypts ClientData
    read_key: [32]u8,
    /// Derived application key: encrypts ServerData
    write_key: [32]u8,

    /// Buffer for encrypting outbound data-phase messages
    encrypted_buf: [max_msg_size + 16]u8,

    /// Plaintext to send (set by application before calling Runner)
    send_buffer: []const u8,
    /// Buffer for received plaintext (set by application)
    recv_buffer: []u8,
};

/// HKDF-Extract: prk = HMAC-SHA256(salt, ikm)
fn hkdf_extract(salt: [32]u8, ikm: [32]u8) [32]u8 {
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, &ikm, &salt);
    return out;
}

/// HKDF-Expand: okm = HMAC-SHA256(prk, info || 0x01)[0..L]
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
