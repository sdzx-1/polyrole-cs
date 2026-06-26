const std = @import("std");
const Io = std.Io;
pub const Graph = @import("Graph.zig");

pub const Role = enum {
    client,
    server,

    pub fn flip(self: @This()) @This() {
        return switch (self) {
            .client => .server,
            .server => .client,
        };
    }
};

pub fn Data(Data_: type, State_: type) type {
    return struct {
        data: Data_,

        pub const Data = Data_;
        pub const State = State_;
    };
}

pub fn ProtocolInfo(
    comptime ProtocolName_: []const u8,
    comptime ClientContext_: type,
    comptime ServerContext_: type,
) type {
    return struct {
        name: []const u8 = "Nameless",
        agent: Role,

        pub const ProtocolName = ProtocolName_;
        pub const client_context = ClientContext_;
        pub const server_context = ServerContext_;
    };
}

//The Exit status is unique.
pub const Exit = union(enum) {
    pub const info: Info = .{};

    pub const Info = struct {
        pub const ProtocolName = "polyrole_exit";
        pub const client_context = void;
        pub const server_context = void;
    };
};

fn TypeSet(comptime bucket_count: usize) type {
    return struct {
        buckets: [bucket_count][]const type,

        const Self = @This();

        pub const init: Self = .{
            .buckets = @splat(&.{}),
        };

        pub fn insert(comptime self: *Self, comptime Type: type) void {
            comptime {
                const hash = std.hash_map.hashString(@typeName(Type));

                self.buckets[hash % bucket_count] = self.buckets[hash % bucket_count] ++ &[_]type{Type};
            }
        }

        pub fn has(comptime self: Self, comptime Type: type) bool {
            comptime {
                const hash = std.hash_map.hashString(@typeName(Type));

                return std.mem.indexOfScalar(type, self.buckets[hash % bucket_count], Type) != null;
            }
        }

        pub fn items(comptime self: Self) []const type {
            comptime {
                var res: []const type = &.{};

                for (&self.buckets) |bucket| {
                    res = res ++ bucket;
                }

                return res;
            }
        }
    };
}

fn reachableStatesDepthFirstSearch(
    comptime states: *[]const type,
    comptime state_machine_names: *[]const []const u8,
    comptime states_stack: *[]const type,
    comptime states_set: *TypeSet(128),
    comptime ExpectedInfo: anytype,
) void {
    @setEvalBranchQuota(20_000_000);

    comptime {
        if (states_stack.len == 0) {
            return;
        }

        const CurrentState = states_stack.*[states_stack.len - 1];
        states_stack.* = states_stack.*[0 .. states_stack.len - 1];

        switch (@typeInfo(CurrentState)) {
            .@"union" => |un| {
                for (un.fields) |field| {
                    const NextState = field.type.State;

                    if (!states_set.has(NextState)) {
                        // Validate that the handler context type matches (skip for special states like Exit)
                        if (NextState != Exit) {
                            const NextInfo = @TypeOf(NextState.info);
                            {
                                for (@typeInfo(Role).@"enum".fields) |F| {
                                    const fname = F.name ++ "_context";

                                    if (@field(NextInfo, fname) != @field(ExpectedInfo, fname)) {
                                        @compileError(std.fmt.comptimePrint(
                                            "{s} context type mismatch: State {any}\nhas context type {any}\nbut expected {any}",
                                            .{ F.name, NextState, @field(NextInfo, fname), @field(ExpectedInfo, fname) },
                                        ));
                                    }
                                }
                            }
                        }

                        states.* = states.* ++ &[_]type{NextState};
                        state_machine_names.* = state_machine_names.* ++ &[_][]const u8{@TypeOf(NextState.info).ProtocolName};
                        states_stack.* = states_stack.* ++ &[_]type{NextState};
                        states_set.insert(NextState);

                        reachableStatesDepthFirstSearch(states, state_machine_names, states_stack, states_set, ExpectedInfo);
                    }
                }
            },
            else => @compileError("Only support tagged union!"),
        }
    }
}

pub fn reachableStates(comptime State: type) struct { states: []const type, state_machine_names: []const []const u8 } {
    comptime {
        var states: []const type = &.{State};
        var state_machine_names: []const []const u8 = &.{@TypeOf(State.info).ProtocolName};
        var states_stack: []const type = &.{State};
        var states_set: TypeSet(128) = .init;
        const ExpectedInfo = @TypeOf(State.info);

        states_set.insert(State);

        reachableStatesDepthFirstSearch(&states, &state_machine_names, &states_stack, &states_set, ExpectedInfo);

        return .{ .states = states, .state_machine_names = state_machine_names };
    }
}

pub const StateMap = struct {
    states: []const type,
    state_machine_names: []const []const u8,
    StateId: type,

    pub fn init(comptime State_: type) StateMap {
        @setEvalBranchQuota(200_000_000);

        comptime {
            const result = reachableStates(State_);
            const TagInt = std.math.IntFittingRange(0, result.states.len - 1);

            return .{
                .states = result.states,
                .state_machine_names = result.state_machine_names,
                .StateId = @Enum(
                    TagInt,
                    .exhaustive,
                    field_names: {
                        var names: [result.states.len][]const u8 = undefined;
                        for (result.states, 0..) |S, i| names[i] = @typeName(S);
                        break :field_names &names;
                    },
                    field_values: {
                        var values: [result.states.len]TagInt = undefined;
                        for (result.states, 0..) |_, i| values[i] = i;
                        break :field_values &values;
                    },
                ),
            };
        }
    }

    pub fn StateFromId(comptime self: StateMap, comptime state_id: self.StateId) type {
        return self.states[@intFromEnum(state_id)];
    }

    pub fn idFromState(comptime self: StateMap, comptime State: type) self.StateId {
        if (!@hasField(self.StateId, @typeName(State))) @compileError(std.fmt.comptimePrint(
            "Can't find State {s}",
            .{@typeName(State)},
        ));
        return @field(self.StateId, @typeName(State));
    }
};

comptime {
    _ = @import("Runner.zig");
}
