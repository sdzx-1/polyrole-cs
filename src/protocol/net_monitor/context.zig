const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

const ms_per_s = std.time.ms_per_s;

/// Per-window aggregated RTT metrics.
pub const WindowMetrics = struct {
    /// Monotonic timestamp of the first ping assigned to this window (ms)
    start_ms: u64 = 0,

    /// Sum of all rtt_net values in this window (milliseconds)
    rtt_sum_ms: u64 = 0,

    /// Number of ping responses in this window
    rtt_count: u32 = 0,

    /// Minimum rtt_net in this window (milliseconds)
    rtt_min_ms: u64 = std.math.maxInt(u64),

    /// Maximum rtt_net in this window (milliseconds)
    rtt_max_ms: u64 = 0,
};

/// Client-side protocol context.
pub const ClientContext = struct {
    /// IO interface for clock and sleep
    io: Io,

    /// Allocator for dynamic window list
    allocator: Allocator,

    /// Monotonic ping sequence number (incremented each PingQuery)
    seq_num: u64 = 0,

    /// Total ping cycles including the first one (> 0).
    remaining: u32 = 0,

    /// Milliseconds between pings (sleep in PingDecision)
    interval_ms: u64 = 0,

    /// Per-window width in milliseconds (> 0, e.g. 60000 = 1 min)
    window_duration_ms: u64 = 0,

    /// Monotonic millisecond timestamp of first PingQuery.
    /// 0 means not started.
    session_start_ms: u64 = 0,

    /// Dynamic list of per-window metrics, append-only.
    windows: std.ArrayList(WindowMetrics),
};

/// Server-side protocol context — stateless across pings.
pub const ServerContext = struct {
    /// IO interface for clock
    io: Io,

    /// Last received seq_num (from PingQuery), echoed back in PingResponse
    last_seq_num: u64 = 0,

    /// Last received client_send_time (from PingQuery), echoed back
    last_client_send_time: u64 = 0,
};

// ─────────────────── Payload Types ───────────────────

pub const PingPayload = struct {
    seq_num: u64,
    /// Monotonic millisecond timestamp on the client
    client_send_time: u64,
};

pub const PongPayload = struct {
    seq_num: u64,
    /// Echoed from PingPayload — original client-side timestamp
    client_send_time: u64,
    /// Server-side dwell time in milliseconds
    server_dwell_ms: u64,
};

/// Returns the current monotonic timestamp in milliseconds.
pub fn monotonicMs(io: Io) u64 {
    const ts = Io.Timestamp.now(io, .awake);
    return @intCast(@divFloor(ts.nanoseconds, std.time.ns_per_ms));
}

/// Sleep for `ms` milliseconds on the monotonic clock.
pub fn sleepMs(io: Io, ms: u64) !void {
    const dur = Io.Duration.fromMilliseconds(@intCast(ms));
    try Io.sleep(io, dur, .awake);
}
