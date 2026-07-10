const std = @import("std");
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
///
/// Caller sets `allocator`, `remaining`, `interval_ns`,
/// `window_duration_ns`, and inits `windows` before entering
/// `symmetric_run()`. After the Runner exits, read
/// `windows.items` for per-window aggregated results.
pub const ClientContext = struct {
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

// ─────────────────── Monotonic clock helper ───────────────────

/// Returns the current CLOCK_MONOTONIC value in nanoseconds.
/// Panics if the syscall fails (should never happen in practice).
pub fn monotonicNs() u64 {
    var ts: std.os.linux.timespec = undefined;
    if (std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts) != 0) {
        @panic("clock_gettime(MONOTONIC) failed");
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// Sleep for `ns` nanoseconds using nanosleep.
pub fn sleepNs(ns: u64) void {
    const req = std.os.linux.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = std.os.linux.nanosleep(&req, null);
}
