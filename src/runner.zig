const std = @import("std");
const Io = std.Io;
const root = @import("root.zig");
const StateMap = root.StateMap;
const Role = root.Role;
const Data = root.Data;
const ProtocolInfo = root.ProtocolInfo;
const Exit = root.Exit;
const net = Io.net;

fn returnsError(comptime fun: anytype) bool {
    const ret = @typeInfo(@TypeOf(fun)).@"fn".return_type.?;
    return @typeInfo(ret) == .error_union;
}

/// A state machine runner that drives a protocol between two agents.
///
/// Error handling design:
/// ----------------------
/// Originally, `process` and `preprocess` were not expected to return errors.
/// The reasoning was that in typical communication protocols, neither party
/// should unilaterally abort without first notifying the peer — a clean close
/// through an Exit transition is the expected path.
///
/// However, certain scenarios demand unilateral termination. For example, a
/// server facing an illegal or malicious client must be able to abort the
/// protocol immediately upon detecting invalid credentials, a tampered
/// message, or a replay attack. In these cases, attempting to cooperate with
/// the peer (by sending a graceful close) is undesirable or impossible.
///
/// To support both patterns, the Runner now inspects the return type of each
/// state's `process` and `preprocess` at compile time:
/// - If the return type is a plain union (e.g. `@This()`), it is called
///   directly and the protocol proceeds as before.
/// - If the return type is an error union (e.g. `!@This()`), the Runner uses
///   `try` and propagates the error to its own caller, terminating the
///   protocol immediately.
///
/// This allows protocol authors to choose per-state whether abort-on-error
/// semantics apply, without forcing all states into one model.
pub fn Runner(
    comptime State_: type,
) type {
    return struct {
        const Info = @TypeOf(State_.info);
        pub const Client = Info.client_context;
        pub const Server = Info.server_context;
        pub const state_map: StateMap = .init(State_);
        pub const StateId = state_map.StateId;

        pub fn idFromState(comptime State: type) StateId {
            return state_map.idFromState(State);
        }

        pub fn StateFromId(comptime state_id: StateId) type {
            return state_map.StateFromId(state_id);
        }

        /// Run both sides of the protocol in-memory without a channel.
        ///
        /// Simulates the full protocol execution by driving client and server
        /// states in a single thread. For each state, the owning agent's
        /// `process` is called first, then the other side's `preprocess`
        /// receives the transition — no serialization or network I/O involved.
        ///
        /// Useful for testing protocol logic before deploying it over a real
        /// channel, or when the two sides share an address space.
        pub fn simulate(client: *Client, server: *Server, start: type) !void {
            const start_id = idFromState(start);
            @setEvalBranchQuota(10_000_000);
            sw: switch (start_id) {
                inline else => |state_id| {
                    const State = StateFromId(state_id);
                    if (comptime State == Exit) return;
                    const info = State.info;
                    const process_ctx = if (comptime info.agent == .client) client else server;
                    const preprocess_ctx = if (comptime info.agent == .client) server else client;
                    const process = State.process;
                    const result = if (comptime returnsError(process)) try process(process_ctx) else process(process_ctx);
                    if (@hasDecl(State, "preprocess")) {
                        const preprocess = State.preprocess;
                        if (comptime returnsError(preprocess)) try preprocess(preprocess_ctx, result) else preprocess(preprocess_ctx, result);
                    }

                    switch (result) {
                        inline else => |new_state_wit| {
                            const NewState = @TypeOf(new_state_wit).State;
                            continue :sw comptime idFromState(NewState);
                        },
                    }
                },
            }
        }

        /// Run one side of the protocol over a channel (symmetric topology).
        ///
        /// A single function handles both client and server roles: pass
        /// `.client` or `.server` via `role`. States owned by the current role
        /// are processed locally and sent over the channel; states owned by the
        /// other role are received and handled via `preprocess`.
        ///
        /// "Symmetric" refers to the 1:1 communication topology —
        /// a single client talks to a single server. For N:1 or other
        /// asymmetric topologies, a separate runner is needed.
        ///
        /// The `channel` must implement `send(state_id, State, result)`
        /// and `recv(state_id, State) -> result`.
        pub fn symmetric_run(
            comptime role: Role,
            ctx: if (role == .client) *Client else *Server,
            channel: anytype,
            start: type,
        ) !void {
            const start_id = idFromState(start);
            @setEvalBranchQuota(10_000_000);
            sw: switch (start_id) {
                inline else => |state_id| {
                    const State = StateFromId(state_id);
                    if (comptime State == Exit) return;
                    const info = State.info;
                    const result = blk: {
                        if (comptime role == info.agent) {
                            const process = State.process;
                            const res = if (comptime returnsError(process)) try process(ctx) else process(ctx);
                            try channel.send(state_id, State, res);
                            break :blk res;
                        } else {
                            const res = try channel.recv(state_id, State);
                            if (@hasDecl(State, "preprocess")) {
                                const preprocess = State.preprocess;
                                if (comptime returnsError(preprocess)) try preprocess(ctx, res) else preprocess(ctx, res);
                            }
                            break :blk res;
                        }
                    };
                    switch (result) {
                        inline else => |new_state_wit| {
                            const NewState = @TypeOf(new_state_wit).State;
                            continue :sw comptime idFromState(NewState);
                        },
                    }
                },
            }
        }
    };
}

//
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
            next: Data(void, Next),

            pub const info: TestInfo = .{ .agent = .server, .name = "B" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* >= 10) return .next;
                ctx.* += 1;
                return .to_a;
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
    try testing.expectEqual(server, 10);
}
test "symmetric run" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client_context: i32 = 0;
    var server_context: i32 = 0;

    const localhost: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try localhost.listen(io, .{});
    defer server.deinit(io);

    const StreamChannel = root.channel.StreamChannel;

    const S = struct {
        fn clientFn(server_address: net.IpAddress, ctx: *i32) !void {
            var stream = try server_address.connect(io, .{ .mode = .stream });
            defer stream.close(io);

            var stream_channel: StreamChannel = undefined;
            try stream_channel.init(io, allocator, stream, 100, 100);
            defer stream_channel.deinit(allocator);

            try R.symmetric_run(.client, ctx, &stream_channel, P.A);
        }
    };

    var client_task = try io.concurrent(S.clientFn, .{ server.socket.address, &client_context });
    defer client_task.cancel(io) catch {};

    var stream = try server.accept(io);
    defer stream.close(io);

    var stream_channel: StreamChannel = undefined;
    try stream_channel.init(io, allocator, stream, 100, 100);
    defer stream_channel.deinit(allocator);

    try R.symmetric_run(.server, &server_context, &stream_channel, P.A);

    try testing.expectEqual(server_context, 10);
}

test "symmetric run over tls channel" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const crypto = std.crypto;
    const tls_mod = @import("protocol/tls.zig");
    const tls_types = @import("protocol/types.zig");

    const kp_c = crypto.sign.Ed25519.KeyPair.generate(testing.io);
    const kp_s = crypto.sign.Ed25519.KeyPair.generate(testing.io);

    // --- Step 1: TLS handshake over TCP to get encryption keys ---
    const localhost: net.IpAddress = .{ .ip4 = .loopback(0) };
    var tls_listener = try localhost.listen(io, .{});
    defer tls_listener.deinit(io);

    const R_tls = Runner(tls_mod.ClientHello);
    const StreamChannel = root.channel.StreamChannel;

    // Client runs in concurrent task, does TLS handshake, returns keys
    var tls_client_write_key: [32]u8 = undefined;
    var tls_client_read_key: [32]u8 = undefined;

    const TlsClientTask = struct {
        fn run(
            addr: net.IpAddress,
            kp: crypto.sign.Ed25519.KeyPair,
            peer_pk: crypto.sign.Ed25519.PublicKey,
            wk: *[32]u8,
            rk: *[32]u8,
        ) !void {
            var tls_ctx: tls_types.ClientContext = .{
                .io = io,
                .send_counter = 0,
                .recv_counter = std.math.maxInt(u64),
                .id_keypair = kp,
                .peer_id_public = peer_pk,
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

            var stream = try addr.connect(io, .{ .mode = .stream });
            defer stream.close(io);

            var sc: StreamChannel = undefined;
            try sc.init(io, allocator, stream, 256, 256);
            defer sc.deinit(allocator);

            try R_tls.symmetric_run(.client, &tls_ctx, &sc, tls_mod.ClientHello);

            wk.* = tls_ctx.write_key;
            rk.* = tls_ctx.read_key;
        }
    };

    var client_task = try io.concurrent(TlsClientTask.run, .{
        tls_listener.socket.address,
        kp_c,
        kp_s.public_key,
        &tls_client_write_key,
        &tls_client_read_key,
    });

    var tls_stream = try tls_listener.accept(io);

    var tls_server_write_key: [32]u8 = undefined;
    var tls_server_read_key: [32]u8 = undefined;
    {
        var tls_ctx: tls_types.ServerContext = .{
            .io = io,
            .send_counter = 0,
            .recv_counter = std.math.maxInt(u64),
            .id_keypair = kp_s,
            .peer_id_public = kp_c.public_key,
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

        var sc: StreamChannel = undefined;
        try sc.init(io, allocator, tls_stream, 256, 256);
        defer sc.deinit(allocator);

        try R_tls.symmetric_run(.server, &tls_ctx, &sc, tls_mod.ClientHello);

        tls_server_write_key = tls_ctx.write_key;
        tls_server_read_key = tls_ctx.read_key;
    }
    tls_stream.close(io);
    tls_stream = undefined;

    // Wait for TLS client task to complete before starting next phase
    _ = client_task.cancel(io) catch {};

    // --- Step 2: Run test protocol over encrypted channel (new TCP connection) ---
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client_context: i32 = 0;
    var server_context: i32 = 0;

    var listener = try localhost.listen(io, .{});
    defer listener.deinit(io);

    const TlsChannel = root.channel.TlsChannel;

    const S = struct {
        fn clientFn(
            server_address: net.IpAddress,
            ctx: *i32,
            write_key: [32]u8,
            read_key: [32]u8,
        ) !void {
            var stream = try server_address.connect(io, .{ .mode = .stream });
            defer stream.close(io);

            var tc: TlsChannel = undefined;
            try tc.init(io, allocator, stream, write_key, read_key, 256);
            defer tc.deinit(allocator);

            try R.symmetric_run(.client, ctx, &tc, P.A);
        }
    };

    var pp_client_task = try io.concurrent(S.clientFn, .{
        listener.socket.address,
        &client_context,
        tls_client_write_key,
        tls_client_read_key,
    });

    var stream = try listener.accept(io);
    defer stream.close(io);

    var tc: TlsChannel = undefined;
    try tc.init(io, allocator, stream, tls_server_write_key, tls_server_read_key, 256);
    defer tc.deinit(allocator);

    try R.symmetric_run(.server, &server_context, &tc, P.A);

    _ = pp_client_task.cancel(io) catch {};
    try testing.expectEqual(server_context, 10);
}
