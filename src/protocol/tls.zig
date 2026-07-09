const std = @import("std");
const crypto = std.crypto;
const polyrole = @import("../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const types = @import("types.zig");


const TlsInfo = ProtocolInfo("simple_tls", types.ClientContext, types.ServerContext);

// ─────────────────── Payload Types ───────────────────

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

pub const Ciphertext = struct {
    nonce: [24]u8,
    tag: [16]u8,
    ciphertext: []const u8,
};

// ─────────────────── SHA256 helper ───────────────────

fn sha256(parts: anytype) [32]u8 {
    var h = crypto.hash.sha2.Sha256.init(.{});
    inline for (parts) |part| {
        h.update(part);
    }
    var out: [32]u8 = undefined;
    h.final(&out);
    return out;
}

fn randomBytes(io: std.Io, comptime n: usize) [n]u8 {
    var buf: [n]u8 = undefined;
    io.randomSecure(&buf) catch @panic("Io.randomSecure failed");
    return buf;
}

fn generateX25519Keypair(io: std.Io) crypto.dh.X25519.KeyPair {
    return crypto.dh.X25519.KeyPair.generate(io);
}

fn packNonce(counter: u64, random: [16]u8) [24]u8 {
    var nonce: [24]u8 = undefined;
    std.mem.writeInt(u64, nonce[0..8], counter, .big);
    @memcpy(nonce[8..24], &random);
    return nonce;
}

fn unpackNonce(nonce: [24]u8) struct { counter: u64, random: [16]u8 } {
    const counter = std.mem.readInt(u64, nonce[0..8], .big);
    var random: [16]u8 = undefined;
    @memcpy(&random, nonce[8..24]);
    return .{ .counter = counter, .random = random };
}

fn verifyCounter(ctx_recv_counter: *u64, received: u64) void {
    // maxInt(u64) is the sentinel for "no message received yet"
    if (ctx_recv_counter.* != std.math.maxInt(u64) and received <= ctx_recv_counter.*) {
        @panic("replay or out-of-order data message detected");
    }
    ctx_recv_counter.* = received;
}

// ─────────────────── Step 1: ClientHello ───────────────────

pub const ClientHello = union(enum) {
    to_server: Data(ClientHelloPayload, ServerHello),

    pub const info: TlsInfo = .{ .agent = .client, .name = "ClientHello" };

    pub fn process(ctx: *types.ClientContext) @This() {
        const nonce = randomBytes(ctx.io, 24);
        const kp = generateX25519Keypair(ctx.io);

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

// ─────────────────── Step 2: ServerHello ───────────────────

pub const ServerHello = union(enum) {
    to_client: Data(ServerHelloPayload, ClientFinished),

    pub const info: TlsInfo = .{ .agent = .server, .name = "ServerHello" };

    pub fn process(ctx: *types.ServerContext) @This() {
        const nonce = randomBytes(ctx.io, 24);
        const kp = generateX25519Keypair(ctx.io);

        const shared_secret = crypto.dh.X25519.scalarmult(kp.secret_key, ctx.peer_ephemeral_pk) catch
            @panic("x25519 scalarmult failed");
        ctx.shared_secret = shared_secret;

        const keys = types.deriveKeys(shared_secret);
        ctx.handshake_key = keys.handshake_key;

        const t1 = sha256(.{ &ctx.peer_nonce, &ctx.peer_ephemeral_pk, &nonce, &kp.public_key });
        const signature = sign(ctx.id_keypair, &t1);
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

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) void {
        switch (result) {
            .to_client => |d| {
                const payload = d.data;

                const shared_secret = crypto.dh.X25519.scalarmult(ctx.ephemeral_sk, payload.ephemeral_pk) catch
                    @panic("x25519 scalarmult failed");
                ctx.shared_secret = shared_secret;

                const keys = types.deriveKeys(shared_secret);
                ctx.handshake_key = keys.handshake_key;

                const t1 = sha256(.{ &ctx.own_nonce, &ctx.own_ephemeral_pk, &payload.nonce, &payload.ephemeral_pk });
                verifySignature(payload.signature, &t1, ctx.peer_id_public);
                const t2 = sha256(.{ &t1, &payload.signature });
                verifyHmac(&ctx.handshake_key, "server_fin", &t2, payload.mac);

                ctx.peer_nonce = payload.nonce;
                ctx.peer_ephemeral_pk = payload.ephemeral_pk;
                ctx.peer_signature = payload.signature;
                ctx.peer_mac = payload.mac;
            },
        }
    }
};

// ─────────────────── Step 3: ClientFinished ───────────────────

pub const ClientFinished = union(enum) {
    enter_data: Data(ClientFinishedPayload, ClientData),
    close: Data(ClientFinishedPayload, Exit),

    pub const info: TlsInfo = .{ .agent = .client, .name = "ClientFinished" };

    pub fn process(ctx: *types.ClientContext) @This() {
        const keys = types.deriveKeys(ctx.shared_secret);

        const t1 = sha256(.{ &ctx.own_nonce, &ctx.own_ephemeral_pk, &ctx.peer_nonce, &ctx.peer_ephemeral_pk });
        const t2 = sha256(.{ &t1, &ctx.peer_signature });
        const t3 = sha256(.{ &t2, &ctx.peer_mac });
        const signature = sign(ctx.id_keypair, &t3);
        const t4 = sha256(.{ &t3, &signature });
        const mac = hmacSha256(&ctx.handshake_key, "client_fin", &t4);

        ctx.write_key = keys.client_write_key;
        ctx.read_key = keys.server_write_key;

        const payload = ClientFinishedPayload{ .signature = signature, .mac = mac };
        if (ctx.send_buffer.len > 0) {
            return .{ .enter_data = .{ .data = payload } };
        } else {
            return .{ .close = .{ .data = payload } };
        }
    }

    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        const payload: ClientFinishedPayload = switch (result) {
            .enter_data => |d| d.data,
            .close => |d| d.data,
        };

        const t1 = sha256(.{ &ctx.peer_nonce, &ctx.peer_ephemeral_pk, &ctx.own_nonce, &ctx.own_ephemeral_pk });
        const t2 = sha256(.{ &t1, &ctx.own_signature });
        const t3 = sha256(.{ &t2, &ctx.own_mac });
        verifySignature(payload.signature, &t3, ctx.peer_id_public);
        const t4 = sha256(.{ &t3, &payload.signature });
        verifyHmac(&ctx.handshake_key, "client_fin", &t4, payload.mac);

        const keys = types.deriveKeys(ctx.shared_secret);
        ctx.read_key = keys.client_write_key;
        ctx.write_key = keys.server_write_key;
    }
};

// ─────────────────── Data Phase: ClientData ───────────────────

pub const ClientData = union(enum) {
    send: Data(Ciphertext, ServerData),
    close: Data(void, Exit),

    pub const info: TlsInfo = .{ .agent = .client, .name = "ClientData" };

    pub fn process(ctx: *types.ClientContext) @This() {
        if (ctx.send_buffer.len == 0) return .close;

        const plaintext = ctx.send_buffer;
        const counter = ctx.send_counter;
        ctx.send_counter += 1;
        const nonce = packNonce(counter, randomBytes(ctx.io, 16));

        const ct_len = plaintext.len + 16;
        const combined = ctx.encrypted_buf[0..ct_len];
        crypto.nacl.SecretBox.seal(combined, plaintext, nonce, ctx.write_key);

        const tag = combined[0..16].*;
        const ct = combined[16..][0..plaintext.len];

        ctx.send_buffer = ""; // mark as sent

        return .{ .send = .{ .data = .{
            .nonce = nonce,
            .tag = tag,
            .ciphertext = ct,
        } } };
    }

    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const payload = d.data;
                const n = unpackNonce(payload.nonce);
                verifyCounter(&ctx.recv_counter, n.counter);
                const combined_len = payload.ciphertext.len + 16;
                const combined = ctx.encrypted_buf[0..combined_len];
                @memcpy(combined[0..16], &payload.tag);
                @memcpy(combined[16..], payload.ciphertext);
                crypto.nacl.SecretBox.open(
                    ctx.recv_buffer[0 .. combined_len - 16],
                    combined,
                    payload.nonce,
                    ctx.read_key,
                ) catch @panic("ClientData: AEAD decrypt failed");
            },
            .close => {},
        }
    }
};

// ─────────────────── Data Phase: ServerData ───────────────────

pub const ServerData = union(enum) {
    send: Data(Ciphertext, ClientData),
    close: Data(void, Exit),

    pub const info: TlsInfo = .{ .agent = .server, .name = "ServerData" };

    pub fn process(ctx: *types.ServerContext) @This() {
        if (ctx.send_buffer.len == 0) return .close;

        const plaintext = ctx.send_buffer;
        const counter = ctx.send_counter;
        ctx.send_counter += 1;
        const nonce = packNonce(counter, randomBytes(ctx.io, 16));

        const ct_len = plaintext.len + 16;
        const combined = ctx.encrypted_buf[0..ct_len];
        crypto.nacl.SecretBox.seal(combined, plaintext, nonce, ctx.write_key);

        const tag = combined[0..16].*;
        const ct = combined[16..][0..plaintext.len];

        ctx.send_buffer = ""; // mark as sent

        return .{ .send = .{ .data = .{
            .nonce = nonce,
            .tag = tag,
            .ciphertext = ct,
        } } };
    }

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const payload = d.data;
                const n = unpackNonce(payload.nonce);
                verifyCounter(&ctx.recv_counter, n.counter);
                const combined_len = payload.ciphertext.len + 16;
                const combined = ctx.encrypted_buf[0..combined_len];
                @memcpy(combined[0..16], &payload.tag);
                @memcpy(combined[16..], payload.ciphertext);
                crypto.nacl.SecretBox.open(
                    ctx.recv_buffer[0 .. combined_len - 16],
                    combined,
                    payload.nonce,
                    ctx.read_key,
                ) catch @panic("ServerData: AEAD decrypt failed");
            },
            .close => {},
        }
    }
};

// ─────────────────── Crypto helpers ───────────────────

fn sign(kp: crypto.sign.Ed25519.KeyPair, msg: []const u8) [64]u8 {
    const sig = kp.sign(msg, null) catch @panic("Ed25519 sign failed");
    return sig.toBytes();
}

fn verifySignature(sig_bytes: [64]u8, msg: []const u8, pubkey: crypto.sign.Ed25519.PublicKey) void {
    const sig = crypto.sign.Ed25519.Signature.fromBytes(sig_bytes);
    sig.verify(msg, pubkey) catch @panic("Ed25519 verify failed");
}

fn hmacSha256(key: *const [32]u8, comptime label: []const u8, msg: []const u8) [32]u8 {
    var buf: [512]u8 = undefined;
    @memcpy(buf[0..label.len], label);
    @memcpy(buf[label.len..][0..msg.len], msg);
    const total = buf[0 .. label.len + msg.len];
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, total, key);
    return out;
}

fn verifyHmac(key: *const [32]u8, comptime label: []const u8, msg: []const u8, expected: [32]u8) void {
    const got = hmacSha256(key, label, msg);
    if (!crypto.timing_safe.eql([32]u8, got, expected)) {
        @panic("HMAC verify failed");
    }
}
