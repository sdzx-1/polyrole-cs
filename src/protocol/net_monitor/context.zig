const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;

/// Per-ping RTT record appended to results list.
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
    timestamp: zio.Timestamp,
};

/// Client-side protocol context.
pub const ClientContext = struct {
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

    /// Per-ping RTT records, append-only.
    results: std.ArrayList(PingResult),

    /// Release results memory.
    pub fn deinit(self: *@This()) void {
        self.results.deinit(self.allocator);
        self.* = undefined;
    }
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
pub fn monotonicMs() u64 {
    const ts = zio.Timestamp.now(.monotonic);
    return @intCast(ts.toNanoseconds() / 1_000_000);
}

/// Sleep for `ms` milliseconds on the monotonic clock.
pub fn sleepMs(ms: u64) !void {
    const dur = zio.Duration.fromMilliseconds(@intCast(ms));
    try zio.sleep(dur);
}
