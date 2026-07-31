const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;

/// 追加到结果列表的每次 ping RTT 记录。
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
    timestamp: zio.Timestamp,
};

/// 客户端协议上下文。
pub const ClientContext = struct {
    /// 结果列表的分配器
    allocator: Allocator,

    /// 单调递增的 ping 序号（每次 PingQuery 递增）
    seq_num: u64 = 0,

    /// 最近一次发送 ping 的单调毫秒时间戳。
    /// 在 PingQuery.process() 中本地设置，之后用于 RTT 计算。
    last_send_ms: u64 = 0,

    /// 总 ping 轮数（含第一轮，> 0）。
    remaining: u32 = 0,

    /// ping 间隔毫秒数（在 PingDecision 中休眠）
    interval_ms: u64 = 0,

    /// 可选 CSV 文件。设置后每 30 条结果以追加模式落盘，
    /// 并清空内存列表。
    file: ?zio.File = null,

    /// 每次 ping 的 RTT 记录，只追加。达到 30 条时自动落盘（若设置了 `file`）。
    results: std.ArrayList(PingResult),

    /// 将结果刷入 CSV 并清空列表。若 file 为 null 则不操作。
    pub fn flushResults(self: *@This()) !void {
        if (self.file) |f| {
            if (self.results.items.len == 0) return;

            var csv: [1024]u8 = undefined;
            var pos: usize = 0;
            for (self.results.items) |r| {
                const line = std.fmt.bufPrint(csv[pos..], "{d},{d},{d}\n", .{ r.seq_num, r.rtt_ms, r.timestamp.toNanoseconds() }) catch break;
                pos += line.len;
            }
            if (pos == 0) return;

            const offset = (try f.stat()).size;
            _ = try f.write(csv[0..pos], offset);

            self.results.clearRetainingCapacity();
        }
    }

    /// 释放结果内存。若设置了文件则先刷新剩余结果。
    pub fn deinit(self: *@This()) void {
        self.flushResults() catch {};
        self.results.deinit(self.allocator);
        self.* = undefined;
    }
};

/// 服务端协议上下文——无状态回显，无需 IO。
pub const ServerContext = struct {
    /// 最近收到的 seq_num（来自 PingQuery），在 PingResponse 中原样回显
    last_seq_num: u64 = 0,
};

// ─────────────────── 载荷类型 ───────────────────

pub const PingPayload = struct {
    seq_num: u64,
};

pub const PongPayload = struct {
    seq_num: u64,
};

/// 返回当前单调时钟的毫秒时间戳。
pub fn monotonicMs() u64 {
    const ts = zio.Timestamp.now(.monotonic);
    return @intCast(ts.toNanoseconds() / 1_000_000);
}

/// 在单调时钟上休眠 `ms` 毫秒。
pub fn sleepMs(ms: u64) !void {
    const dur = zio.Duration.fromMilliseconds(@intCast(ms));
    try zio.sleep(dur);
}
