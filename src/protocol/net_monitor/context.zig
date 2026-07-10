const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// Per-window aggregated RTT metrics.
pub const WindowMetrics = struct {
    /// Monotonic timestamp of the first ping assigned to this window
    start_ns: u64 = 0,

    /// Sum of all rtt_net values in this window (nanoseconds)
    rtt_sum_ns: u64 = 0,

    /// Number of ping responses in this window
    rtt_count: u32 = 0,

    /// Minimum rtt_net in this window
    rtt_min_ns: u64 = std.math.maxInt(u64),

    /// Maximum rtt_net in this window
    rtt_max_ns: u64 = 0,
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

    /// Nanoseconds between pings (sleep in PingDecision)
    interval_ns: u64 = 0,

    /// Per-window width in nanoseconds (> 0, e.g. 60_000_000_000 = 1 min)
    window_duration_ns: u64 = 0,

    /// Monotonic nanosecond timestamp of first PingQuery.
    /// 0 means not started.
    session_start_ns: u64 = 0,

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
    /// Monotonic nanosecond timestamp on the client
    client_send_time: u64,
};

pub const PongPayload = struct {
    seq_num: u64,
    /// Echoed from PingPayload — original client-side timestamp
    client_send_time: u64,
    /// Server-side dwell time in nanoseconds
    server_dwell_ns: u64,
};

/// Returns the current monotonic timestamp as u64 nanoseconds.
pub fn monotonicNs(io: Io) u64 {
    const ts = Io.Timestamp.now(io, .awake);
    return @intCast(ts.nanoseconds);
}

/// Sleep for `ns` nanoseconds on the monotonic clock.
pub fn sleepNs(io: Io, ns: u64) !void {
    const dur = Io.Duration.fromNanoseconds(@intCast(ns));
    try Io.sleep(io, dur, .awake);
}
