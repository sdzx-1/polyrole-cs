const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const types = @import("context.zig");

const NetMonitorInfo = ProtocolInfo("net_monitor", types.ClientContext, types.ServerContext);

// ─────────────────── Step 1: PingQuery (client → server) ───────────────────

pub const PingQuery = union(enum) {
    to_server: Data(types.PingPayload, PingResponse),

    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingQuery" };

    pub fn process(ctx: *types.ClientContext) @This() {
        const now = types.monotonicMs(ctx.io);
        ctx.last_send_ms = now;
        ctx.seq_num += 1;

        return .{ .to_server = .{ .data = .{ .seq_num = ctx.seq_num } } };
    }

    /// Server stores seq_num so PingResponse can echo it back.
    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        ctx.last_seq_num = result.to_server.data.seq_num;
    }
};

// ─────────────────── Step 2: PingResponse (server → client) ───────────────────

pub const PingResponse = union(enum) {
    to_client: Data(types.PongPayload, PingDecision),

    pub const info: NetMonitorInfo = .{ .agent = .server, .name = "PingResponse" };

    pub fn process(ctx: *types.ServerContext) @This() {
        return .{ .to_client = .{ .data = .{ .seq_num = ctx.last_seq_num } } };
    }

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) !void {
        const pong = result.to_client.data;
        const now = types.monotonicMs(ctx.io);

        const rtt_ms = now -| ctx.last_send_ms;

        try ctx.results.append(ctx.allocator, .{
            .seq_num = pong.seq_num,
            .rtt_ms = rtt_ms,
        });
    }
};

// ─────────────────── Step 3: PingDecision (client) ──────────────────────────

pub const PingDecision = union(enum) {
    ping_again: Data(void, PingQuery),
    close: Data(void, Exit),

    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingDecision" };

    pub fn process(ctx: *types.ClientContext) !@This() {
        ctx.remaining -= 1;
        if (ctx.remaining == 0) {
            return .close;
        }
        try types.sleepMs(ctx.io, ctx.interval_ms);
        return .ping_again;
    }

    // No preprocess — server doesn't participate in this decision.
};
