const std = @import("std");
const crypto = std.crypto;
const polyrole = @import("../root.zig");
const Runner = polyrole.runner.Runner;
const tls = @import("tls.zig");
const types = @import("types.zig");

fn initClientCtx(kp: crypto.sign.Ed25519.KeyPair, server_pk: crypto.sign.Ed25519.PublicKey) types.ClientContext {
    return .{
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

fn initServerCtx(kp: crypto.sign.Ed25519.KeyPair, client_pk: crypto.sign.Ed25519.PublicKey) types.ServerContext {
    return .{
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
    var client = initClientCtx(kp_c, kp_s.public_key);
    var server = initServerCtx(kp_s, kp_c.public_key);

    const R = Runner(tls.ClientHello);
    R.simulate(&client, &server, tls.ClientHello);
}

test "simulate with data exchange" {
    const testing = std.testing;
    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    var client = initClientCtx(kp_c, kp_s.public_key);
    var server = initServerCtx(kp_s, kp_c.public_key);

    const client_msg = "hello from client";
    const server_msg = "hello from server";

    var client_recv_buf: [128]u8 = undefined;
    var server_recv_buf: [128]u8 = undefined;

    client.send_buffer = client_msg;
    client.recv_buffer = &client_recv_buf;
    server.send_buffer = server_msg;
    server.recv_buffer = &server_recv_buf;

    const R = Runner(tls.ClientHello);
    R.simulate(&client, &server, tls.ClientHello);

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
    var client = initClientCtx(kp_c, kp_s.public_key);
    var server = initServerCtx(kp_s, kp_c.public_key);

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
    // todo: test close variant
}
