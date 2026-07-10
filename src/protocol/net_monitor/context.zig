const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// Per-ping RTT record appended to results list.
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
};

/// Client-side protocol context.
pub const ClientContext = struct {
    /// IO interface for clock and sleep
    io: Io,

    /// Allocator for results list
    allocator: Allocator,

    /// Monotonic ping sequence number (incremented each PingQuery)
    seq_num: u64 = 0,

    /// Monotonic millisecond timestamp of the last sent ping.
    /// Set locally in PingQuery.process(), used for RTT computation later.
    last_send_ms: u64 = 0,

    /// Total ping cycles including the first one (> 0).
    remaining: u32 = 0,

    /// Milliseconds between pings (sleep in PingDecision)
    interval_ms: u64 = 0,

    /// Maximum number of results to store (0 = unlimited).
    /// Results beyond this count are silently dropped.
    max_results: u32 = 0,

    /// Per-ping RTT records, append-only.
    results: std.ArrayList(PingResult),
};

/// Server-side protocol context — stateless echo, no IO needed.
pub const ServerContext = struct {
    /// Last received seq_num (from PingQuery), echoed back in PingResponse
    last_seq_num: u64 = 0,
};

// ─────────────────── Payload Types ───────────────────

pub const PingPayload = struct {
    seq_num: u64,
};

pub const PongPayload = struct {
    seq_num: u64,
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
