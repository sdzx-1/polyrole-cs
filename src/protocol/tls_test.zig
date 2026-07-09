const std = @import("std");
const crypto = std.crypto;
const polyrole = @import("../root.zig");
const Runner = polyrole.runner.Runner;
const tls = @import("tls.zig");
const types = @import("types.zig");

fn initClientCtx(io: std.Io, kp: crypto.sign.Ed25519.KeyPair, server_pk: crypto.sign.Ed25519.PublicKey) types.ClientContext {
    return .{
        .io = io,
        .send_counter = 0,
        .recv_counter = std.math.maxInt(u64),
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
        .encrypted_buf = undefined,
        .send_buffer = "",
        .recv_buffer = undefined,
    };
}

fn initServerCtx(io: std.Io, kp: crypto.sign.Ed25519.KeyPair, client_pk: crypto.sign.Ed25519.PublicKey) types.ServerContext {
    return .{
        .io = io,
        .send_counter = 0,
        .recv_counter = std.math.maxInt(u64),
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
        .encrypted_buf = undefined,
        .send_buffer = "",
        .recv_buffer = undefined,
    };
}

test "hkdf" {
    _ = @import("types.zig");
}

test "simulate handshake only" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);
}

test "simulate with data exchange" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const client_msg = "hello from client";
    const server_msg = "hello from server";

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;

    client.send_buffer = client_msg;
    client.recv_buffer = &client_recv_buf;
    server.send_buffer = server_msg;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expectEqualStrings(server_msg, client_recv_buf[0..server_msg.len]);
    try testing.expectEqualStrings(client_msg, server_recv_buf[0..client_msg.len]);
}

test "symmetric run" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const client_msg = "hello from client";
    const server_msg = "hello from server";

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;

    client.send_buffer = client_msg;
    client.recv_buffer = &client_recv_buf;
    server.send_buffer = server_msg;
    server.recv_buffer = &server_recv_buf;

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
            try ch.init(io, allocator, stream, 1024, 1024);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, tls.ClientHello);
        }
    };

    var client_task = try io.concurrent(S.clientFn, .{ listener.socket.address, &client });
    defer client_task.cancel(io) catch {};

    var stream = try listener.accept(io);
    defer stream.close(io);

    var ch: StreamChannel = undefined;
    try ch.init(io, allocator, stream, 1024, 1024);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, tls.ClientHello);

    try testing.expectEqualStrings(server_msg, client_recv_buf[0..server_msg.len]);
    try testing.expectEqualStrings(client_msg, server_recv_buf[0..client_msg.len]);
}

test "simulate close without data" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    // No send_buffer set → ClientFinished chooses .close variant
    client.send_buffer = "";
    server.send_buffer = "";

    const R = Runner(tls.ClientHello);
    // Should complete without error and without entering data phase
    try R.simulate(&client, &server, tls.ClientHello);
}

test "handshake: tampered server signature → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    // Step 1: ClientHello
    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    // Step 2: ServerHello
    var sh = try tls.ServerHello.process(&server);

    // Tamper server signature
    sh.to_client.data.signature = [_]u8{0} ** 64;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.SignatureInvalid, err);
}

test "handshake: tampered server MAC → HmacInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);

    // Tamper server MAC (signature is valid, MAC is not)
    sh.to_client.data.mac = [_]u8{0} ** 32;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.HmacInvalid, err);
}

test "handshake: tampered client signature → SignatureInvalid" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    // Drive through ClientHello + ServerHello
    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);
    const sh = try tls.ServerHello.process(&server);
    try tls.ServerHello.preprocess(&client, sh);

    // Set send_buffer so ClientFinished chooses .enter_data variant
    client.send_buffer = "test";

    // Step 3: ClientFinished
    var cf = try tls.ClientFinished.process(&client);

    // Ensure it chose .enter_data, not .close
    try testing.expect(cf == .enter_data);

    // Tamper client signature
    cf.enter_data.data.signature = [_]u8{0} ** 64;

    const err = tls.ClientFinished.preprocess(&server, cf);
    try testing.expectError(error.SignatureInvalid, err);
}

test "handshake: invalid ephemeral public key → DhFailed" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    const ch = try tls.ClientHello.process(&client);
    tls.ClientHello.preprocess(&server, ch);

    var sh = try tls.ServerHello.process(&server);

    // Zero ephemeral public key → scalarmult fails with IdentityElementError
    sh.to_client.data.ephemeral_pk = [_]u8{0} ** 32;

    const err = tls.ServerHello.preprocess(&client, sh);
    try testing.expectError(error.DhFailed, err);
}

test "data: AEAD decrypt with tampered ciphertext → DecryptFailed" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;
    client.send_buffer = "hello";
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    // Complete handshake
    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    // Send one legitimate message to establish counter state
    server.send_buffer = "legit";
    const sd = try tls.ServerData.process(&server);
    try tls.ServerData.preprocess(&client, sd);

    // Now craft a tampered ciphertext
    client.send_buffer = "tampered";
    var cd = try tls.ClientData.process(&client);
    cd.send.data.tag[0] ^= 0xFF; // flip one byte in authentication tag

    const err = tls.ClientData.preprocess(&server, cd);
    try testing.expectError(error.DecryptFailed, err);
}

test "data: replay same message → ReplayDetected" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;
    client.send_buffer = "hello";
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    // First delivery succeeds
    server.send_buffer = "legit";
    const sd = try tls.ServerData.process(&server);
    // Save ciphertext before encrypted_buf is overwritten by subsequent calls
    var saved_ct: [128]u8 = undefined;
    @memcpy(saved_ct[0..sd.send.data.ciphertext.len], sd.send.data.ciphertext);
    try tls.ServerData.preprocess(&client, sd);

    // Replay: reconstruct with saved ciphertext
    var replay = sd;
    replay.send.data.ciphertext = saved_ct[0..sd.send.data.ciphertext.len];
    const err = tls.ServerData.preprocess(&client, replay);
    try testing.expectError(error.ReplayDetected, err);
}

test "simulate multiple data exchanges" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [1024]u8 = undefined;
    var server_recv_buf: [1024]u8 = undefined;
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);

    // First exchange
    client.send_buffer = "ping";
    server.send_buffer = "pong";
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expectEqualStrings("pong", client_recv_buf[0..4]);
    try testing.expectEqualStrings("ping", server_recv_buf[0..4]);

    // Reset buffers for next exchange
    client.send_buffer = "msg2";
    server.send_buffer = "ack2";
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expectEqualStrings("ack2", client_recv_buf[0..4]);
    try testing.expectEqualStrings("msg2", server_recv_buf[0..4]);
}

test "data: out-of-order message → ReplayDetected" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;
    client.send_buffer = "hello";
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    // Send and receive first message
    server.send_buffer = "first";
    const sd1 = try tls.ServerData.process(&server);
    var saved_ct1: [128]u8 = undefined;
    @memcpy(saved_ct1[0..sd1.send.data.ciphertext.len], sd1.send.data.ciphertext);
    try tls.ServerData.preprocess(&client, sd1);

    // Send and receive second message (increments counter to 1)
    server.send_buffer = "second";
    const sd2 = try tls.ServerData.process(&server);
    try tls.ServerData.preprocess(&client, sd2);

    // Replay first message (counter=0) after second (counter=1) was received
    var replay = sd1;
    replay.send.data.ciphertext = saved_ct1[0..sd1.send.data.ciphertext.len];
    const err = tls.ServerData.preprocess(&client, replay);
    try testing.expectError(error.ReplayDetected, err);
}

test "simulate large message" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [types.max_msg_size]u8 = undefined;
    var server_recv_buf: [types.max_msg_size]u8 = undefined;
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    // Fill message with non-trivial pattern
    var large_msg: [types.max_msg_size]u8 = undefined;
    for (&large_msg, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    client.send_buffer = &large_msg;
    server.send_buffer = "ok";

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expectEqualStrings("ok", client_recv_buf[0..2]);
    try testing.expectEqualStrings(&large_msg, server_recv_buf[0..types.max_msg_size]);
}

test "simulate asymmetric message sizes" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [types.max_msg_size]u8 = undefined;
    var server_recv_buf: [types.max_msg_size]u8 = undefined;
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    const short_msg = "hi";
    var long_msg: [512]u8 = undefined;
    @memset(&long_msg, 'x');

    client.send_buffer = short_msg;
    server.send_buffer = &long_msg;

    const R = Runner(tls.ClientHello);
    try R.simulate(&client, &server, tls.ClientHello);

    try testing.expectEqualStrings(&long_msg, client_recv_buf[0..512]);
    try testing.expectEqualStrings(short_msg, server_recv_buf[0..2]);
}

test "simulate multiple sessions with same contexts" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;
    client.recv_buffer = &client_recv_buf;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);

    // Session 1
    client.send_buffer = "session1c";
    server.send_buffer = "session1s";
    client.send_counter = 0;
    client.recv_counter = std.math.maxInt(u64);
    server.send_counter = 0;
    server.recv_counter = std.math.maxInt(u64);

    try R.simulate(&client, &server, tls.ClientHello);
    try testing.expectEqualStrings("session1s", client_recv_buf[0..9]);
    try testing.expectEqualStrings("session1c", server_recv_buf[0..9]);

    // Session 2 — fresh counters, new ephemeral keys
    client.send_buffer = "session2c";
    server.send_buffer = "session2s";
    client.send_counter = 0;
    client.recv_counter = std.math.maxInt(u64);
    server.send_counter = 0;
    server.recv_counter = std.math.maxInt(u64);

    try R.simulate(&client, &server, tls.ClientHello);
    try testing.expectEqualStrings("session2s", client_recv_buf[0..9]);
    try testing.expectEqualStrings("session2c", server_recv_buf[0..9]);
}

test "symmetric run large message" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const net = std.Io.net;

    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(testing.io, kp_c, kp_s.public_key);
    var server = initServerCtx(testing.io, kp_s, kp_c.public_key);

    var large_msg: [types.max_msg_size]u8 = undefined;
    for (&large_msg, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    var client_recv_buf: [types.max_msg_size]u8 = undefined;
    var server_recv_buf: [types.max_msg_size]u8 = undefined;

    client.send_buffer = &large_msg;
    client.recv_buffer = &client_recv_buf;
    server.send_buffer = "done";
    server.recv_buffer = &server_recv_buf;

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
            try ch.init(io, allocator, stream, 2048, 2048);
            defer ch.deinit(allocator);

            try R.symmetric_run(.client, ctx, &ch, tls.ClientHello);
        }
    };

    var client_task = try io.concurrent(S.clientFn, .{ listener.socket.address, &client });
    defer client_task.cancel(io) catch {};

    var stream = try listener.accept(io);
    defer stream.close(io);

    var ch: StreamChannel = undefined;
    try ch.init(io, allocator, stream, 2048, 2048);
    defer ch.deinit(allocator);

    try R.symmetric_run(.server, &server, &ch, tls.ClientHello);

    try testing.expectEqualStrings("done", client_recv_buf[0..4]);
    try testing.expectEqualStrings(&large_msg, server_recv_buf[0..types.max_msg_size]);
}
