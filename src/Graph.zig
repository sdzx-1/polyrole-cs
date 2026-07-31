const std = @import("std");
const polyrole = @import("root.zig");

arena: std.heap.ArenaAllocator,
name: []const u8,
nodes: std.ArrayListUnmanaged(Node),
edges: std.ArrayListUnmanaged(Edge),

const Graph = @This();

const colors: []const []const u8 = &.{
    "red",       "green",    "blue",
    "brown",     "navy",     "teal",
    "cyan",      "magenta",  "darkred",
    "darkgreen", "darkblue", "orange",
    "purple",
};

pub const Node = struct {
    state_description: []const u8,
    id: u32,
    sender: []const u8,
    fsm_description: []const u8,
};

pub const Edge = struct {
    from: u32,
    to: u32,
    label: []const u8,
};

pub fn generateDot(
    self: @This(),
    role_labels: Roles,
    writer: anytype,
) !void {
    try writer.writeAll(
        \\digraph fsm_state_graph {
        \\
    );

    { // 状态图
        try writer.writeAll(
            \\  subgraph cluster_transitions {
            \\    label = "State Transitions";
            \\
        );

        // 为每个状态机的节点创建子图
        var cluster_idx: u32 = 0;
        var current_fsm_name: ?[]const u8 = null;

        for (self.nodes.items) |node| {
            // 必要时开始新的状态机子图
            if (current_fsm_name == null or !std.mem.eql(u8, current_fsm_name.?, node.fsm_description)) {
                // 若存在则关闭上一个子图
                if (current_fsm_name != null) {
                    try writer.writeAll(
                        \\    }
                        \\
                    );
                    cluster_idx += 1;
                }

                // 开始新子图
                current_fsm_name = node.fsm_description;
                try writer.print(
                    \\    subgraph cluster_fsm_{d} {{
                    \\      label = "{s}";
                    \\
                , .{ cluster_idx, node.fsm_description });
            }

            // 向当前状态机子图添加节点
            try writer.print(
                \\      {d}[shape=rect,  label="{s}[{d}] {s}", color = "{s}"];
                \\
            ,
                .{
                    node.id,
                    role_labels.get(node.sender),
                    node.id,
                    node.state_description,
                    colors[@as(usize, @intCast(node.id)) % colors.len],
                },
            );
        }

        // 关闭最后一个子图
        if (current_fsm_name != null) {
            try writer.writeAll(
                \\    }
                \\
            );
        }

        // 添加边
        for (self.edges.items) |edge| {
            try writer.print(
                \\    {d} -> {d} [label = "{s}", color = "{s}", fontcolor = "{s}"];
                \\
            , .{
                edge.from,
                edge.to,
                edge.label,
                colors[@as(usize, @intCast(edge.from)) % colors.len],
                colors[@as(usize, @intCast(edge.from)) % colors.len],
            });
        }

        try writer.writeAll(
            \\  }
            \\
        );
    }

    try writer.writeAll(
        \\}
        \\
    );

    try writer.flush();
}

pub const Roles = struct {
    client: []const u8 = "👤",
    server: []const u8 = "🖥️",

    pub fn get(self: @This(), role: []const u8) []const u8 {
        if (std.mem.eql(u8, role, "client")) return self.client;
        if (std.mem.eql(u8, role, "server")) return self.server;
        return role;
    }
};

pub fn initWithFsm(allocator: std.mem.Allocator, comptime State_: type) !Graph {
    @setEvalBranchQuota(2000000);

    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();

    const arena_allocator = arena.allocator();

    var nodes: std.ArrayListUnmanaged(Node) = .empty;
    var edges: std.ArrayListUnmanaged(Edge) = .empty;

    const state_map: polyrole.StateMap = comptime .init(State_);

    inline for (state_map.states, state_map.state_machine_names, 0..) |State, fsm_name, state_idx| {
        try nodes.append(arena_allocator, .{
            .state_description = if (State == polyrole.Exit) "Exit" else try std.fmt.allocPrint(
                arena_allocator,
                "{s} .{t} -> {any}",
                .{ State.info.name, State.info.agent, State.info.agent.flip() },
            ),
            .id = @intCast(state_idx),
            .sender = if (State == polyrole.Exit) "" else try std.fmt.allocPrint(arena_allocator, "{t}", .{State.info.agent}),
            .fsm_description = if (State == polyrole.Exit) fsm_name else try std.fmt.allocPrint(arena_allocator, "{s}", .{fsm_name}),
        });

        switch (@typeInfo(State)) {
            .@"union" => |un| {
                inline for (un.fields) |field| {
                    const NextState = field.type.State;

                    const next_state_idx: u32 = @intFromEnum(state_map.idFromState(NextState));

                    try edges.append(arena_allocator, .{
                        .from = @intCast(state_idx),
                        .to = next_state_idx,
                        .label = field.name,
                    });
                }
            },
            else => @compileError("Only support tagged union!"),
        }
    }

    // 按状态机名称排序节点
    std.mem.sort(Node, nodes.items, {}, struct {
        pub fn lessThan(_: void, lhs: Node, rhs: Node) bool {
            const cmp = std.mem.order(u8, lhs.fsm_description, rhs.fsm_description);
            if (cmp != .eq) return cmp == .lt;
            return lhs.id < rhs.id;
        }
    }.lessThan);

    return .{
        .arena = arena,
        .edges = edges,
        .name = @TypeOf(State_.info).ProtocolName,
        .nodes = nodes,
    };
}

pub fn deinit(self: *Graph) void {
    self.arena.deinit();
}
