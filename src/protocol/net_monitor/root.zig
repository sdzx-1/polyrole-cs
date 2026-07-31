const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const types = @import("context.zig");

const NetMonitorInfo = ProtocolInfo("net_monitor", types.ClientContext, types.ServerContext);

// ─────────────────── PingQuery（客户端）─────────────────────────────────────

pub const PingQuery = union(enum) {
    to_server: Data(types.PingPayload, PingResponse),
    close: Data(void, Exit),

    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingQuery" };

    pub fn process(ctx: *types.ClientContext) !@This() {
        if (ctx.remaining == 0) return .close;
        if (ctx.seq_num > 0) try types.sleepMs(ctx.interval_ms);
        ctx.remaining -= 1;

        const now = types.monotonicMs();
        ctx.last_send_ms = now;
        ctx.seq_num += 1;

        return .{ .to_server = .{ .data = .{ .seq_num = ctx.seq_num } } };
    }

    /// 服务端接收 PingQuery：存储 seq_num 用于回显，或退出。
    pub fn preprocess(ctx: *types.ServerContext, result: @This()) void {
        switch (result) {
            .to_server => |d| ctx.last_seq_num = d.data.seq_num,
            .close => {},
        }
    }
};

// ─────────────────── PingResponse（服务端 → 客户端）─────────────────────────

pub const PingResponse = union(enum) {
    to_client: Data(types.PongPayload, PingQuery),

    pub const info: NetMonitorInfo = .{ .agent = .server, .name = "PingResponse" };

    pub fn process(ctx: *types.ServerContext) @This() {
        return .{ .to_client = .{ .data = .{ .seq_num = ctx.last_seq_num } } };
    }

    pub fn preprocess(ctx: *types.ClientContext, result: @This()) !void {
        const pong = result.to_client.data;
        const now_ms = types.monotonicMs();
        const now_ts = zio.Timestamp.now(.real);

        const rtt_ms = now_ms -| ctx.last_send_ms;

        try ctx.results.append(ctx.allocator, .{
            .seq_num = pong.seq_num,
            .rtt_ms = rtt_ms,
            .timestamp = now_ts,
        });

        if (ctx.results.items.len >= 30) try ctx.flushResults();
    }
};
