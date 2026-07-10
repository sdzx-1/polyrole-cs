const std = @import("std");
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
        const now = types.monotonicNs(ctx.io);
        if (ctx.session_start_ns == 0) {
            ctx.session_start_ns = now;
        }
        ctx.seq_num += 1;

        return .{ .to_server = .{ .data = .{
            .seq_num = ctx.seq_num,
            .client_send_time = now,
        } } };
    }

    /// Server receives and stores the ping data so PingResponse can echo it back.
    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        ctx.last_seq_num = result.to_server.data.seq_num;
        ctx.last_client_send_time = result.to_server.data.client_send_time;
    }
};

// ─────────────────── Step 2: PingResponse (server → client) ───────────────────

pub const PingResponse = union(enum) {
    to_client: Data(types.PongPayload, PingDecision),

    pub const info: NetMonitorInfo = .{ .agent = .server, .name = "PingResponse" };

    pub fn process(ctx: *types.ServerContext) @This() {
        const t_arrival = types.monotonicNs(ctx.io);
        const t_departure = types.monotonicNs(ctx.io);

        return .{ .to_client = .{ .data = .{
            .seq_num = ctx.last_seq_num,
            .client_send_time = ctx.last_client_send_time,
            .server_dwell_ns = t_departure - t_arrival,
        } } };
    }

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) !void {
        const pong = result.to_client.data;
        const now = types.monotonicNs(ctx.io);

        const rtt_net = now -| pong.client_send_time -| pong.server_dwell_ns;

        const elapsed = now -| ctx.session_start_ns;
        const index = elapsed / ctx.window_duration_ns;

        while (ctx.windows.items.len <= index) {
            try ctx.windows.append(ctx.allocator, .{
                .start_ns = ctx.session_start_ns + ctx.windows.items.len * ctx.window_duration_ns,
            });
        }

        var w = &ctx.windows.items[index];
        w.rtt_count += 1;
        w.rtt_sum_ns += rtt_net;
        w.rtt_min_ns = @min(w.rtt_min_ns, rtt_net);
        w.rtt_max_ns = @max(w.rtt_max_ns, rtt_net);
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
        try types.sleepNs(ctx.io, ctx.interval_ns);
        return .ping_again;
    }

    // No preprocess — server doesn't participate in this decision.
};
