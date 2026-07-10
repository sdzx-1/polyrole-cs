const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// Per-ping RTT record appended to results list.
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
    timestamp: std.Io.Timestamp,
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

    /// Optional CSV file. When set, results are flushed every 30 entries
    /// in append mode and cleared from the in-memory list.
    file: ?std.Io.File = null,

    /// Per-ping RTT records, append-only. Automatically flushed to file
    /// when it reaches 30 entries (if `file` is set).
    results: std.ArrayList(PingResult),

    /// Flush results to CSV and clear the list. Called automatically at
    /// 30 entries and on deinit. No-op if file is null.
    pub fn flushResults(self: *@This()) !void {
        if (self.file) |f| {
            if (self.results.items.len == 0) return;

            var csv: [1024]u8 = undefined;
            var pos: usize = 0;
            for (self.results.items) |r| {
                const line = std.fmt.bufPrint(csv[pos..], "{d},{d},{d}\n", .{ r.seq_num, r.rtt_ms, r.timestamp.nanoseconds }) catch break;
                pos += line.len;
            }
            if (pos == 0) return;

            const offset = f.length(self.io) catch return;
            try f.writePositionalAll(self.io, csv[0..pos], offset);

            self.results.clearRetainingCapacity();
        }
    }

    /// Release results memory. Flushes remaining results to file if set.
    pub fn deinit(self: *@This()) void {
        self.flushResults() catch {};
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
pub fn monotonicMs(io: Io) u64 {
    const ts = Io.Timestamp.now(io, .awake);
    return @intCast(ts.toMilliseconds());
}

/// Sleep for `ms` milliseconds on the monotonic clock.
pub fn sleepMs(io: Io, ms: u64) !void {
    const dur = Io.Duration.fromMilliseconds(@intCast(ms));
    try Io.sleep(io, dur, .awake);
}
