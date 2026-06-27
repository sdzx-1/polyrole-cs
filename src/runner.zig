const std = @import("std");
const Io = std.Io;
const root = @import("root.zig");
const StateMap = root.StateMap;
const Role = root.Role;
const Data = root.Data;
const ProtocolInfo = root.ProtocolInfo;
const Exit = root.Exit;
const net = Io.net;

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

        pub fn simulate(client: *Client, server: *Server, start: type) void {
            const start_id = idFromState(start);
            @setEvalBranchQuota(10_000_000);
            sw: switch (start_id) {
                inline else => |state_id| {
                    const State = StateFromId(state_id);
                    if (comptime State == Exit) return;
                    const info = State.info;
                    const process_ctx = if (comptime info.agent == .client) client else server;
                    const preprocess_ctx = if (comptime info.agent == .client) server else client;
                    const result = State.process(process_ctx);
                    State.preprocess(preprocess_ctx, result);

                    switch (result) {
                        inline else => |new_state_wit| {
                            const NewState = @TypeOf(new_state_wit).State;
                            continue :sw comptime idFromState(NewState);
                        },
                    }
                },
            }
        }

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
                            const res = State.process(ctx);
                            try channel.send(state_id, State, res);
                            break :blk res;
                        } else {
                            const res = try channel.recv(state_id, State);
                            State.preprocess(ctx, res);
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
            to_b: Data(void, B),
            to_next: Data(void, Next),

            pub const info: TestInfo = .{ .agent = .client, .name = "A" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* >= 10) return .to_next;
                return .to_b;
            }

            pub fn preprocess(ctx: *i32, msg: @This()) void {
                _ = ctx;
                _ = msg;
            }
        };

        pub const B = union(enum) {
            to_a: Data(void, A),

            pub const info: TestInfo = .{ .agent = .server, .name = "B" };

            pub fn process(ctx: *i32) @This() {
                _ = ctx;
                return .to_a;
            }

            pub fn preprocess(ctx: *i32, msg: @This()) void {
                _ = msg;
                ctx.* += 1;
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
    R.simulate(&client, &server, P.A);
    try testing.expectEqual(client, 10);
}
test "symmetric run" {
    const testing = std.testing;
    const io = testing.io;
    const allocator = testing.allocator;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client_context: i32 = 0;
    var server_context: i32 = 0;

    _ = &client_context;
    _ = &server_context;

    const localhost: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try localhost.listen(io, .{});
    defer server.deinit(io);

    const StreamChannel = @import("channel.zig").StreamChannel;

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

    try testing.expectEqual(client_context, 10);
}