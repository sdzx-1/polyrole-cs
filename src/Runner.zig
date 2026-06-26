const std = @import("std");
const Io = std.Io;
const root = @import("root.zig");
const StateMap = root.StateMap;
const Role = root.Role;
const Data = root.Data;
const ProtocolInfo = root.ProtocolInfo;
const Exit = root.Exit;

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

        pub fn direct_connect(client: *Client, server: *Server, start: type) void {
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

test "direct_connect" {
    const testing = std.testing;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client: i32 = 0;
    var server: i32 = 0;
    R.direct_connect(&client, &server, P.A);
    try testing.expectEqual(client, 10);
}
