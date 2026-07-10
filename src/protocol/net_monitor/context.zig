const std = @import("std");
const Allocator = std.mem.Allocator;

/// Per-window aggregated RTT metrics.  每窗口聚合的 RTT 指标
pub const WindowMetrics = struct {
    /// Monotonic timestamp of the first ping assigned to this window  本窗口首个 ping 的单调时间戳
    start_ns: u64 = 0,

    /// Sum of all rtt_net values in this window (nanoseconds)  本窗口内所有 rtt_net 之和（纳秒）
    rtt_sum_ns: u64 = 0,

    /// Number of ping responses in this window  本窗口内 ping 响应数
    rtt_count: u32 = 0,

    /// Minimum rtt_net in this window  本窗口内最小 rtt_net
    rtt_min_ns: u64 = std.math.maxInt(u64),

    /// Maximum rtt_net in this window  本窗口内最大 rtt_net
    rtt_max_ns: u64 = 0,
};

/// Client-side protocol context.  客户端协议上下文
///
/// Caller sets `allocator`, `remaining`, `interval_ns`,
/// `window_duration_ns`, and inits `windows` before entering
/// `symmetric_run()`. After the Runner exits, read
/// `windows.items` for per-window aggregated results.
/// 调用方在进入 symmetric_run() 前设置以上字段，Runner 退出后读取 windows.items
pub const ClientContext = struct {
    /// Allocator for dynamic window list  用于动态窗口列表的分配器
    allocator: Allocator,

    /// Monotonic ping sequence number (incremented each PingQuery)  单调递增的 ping 序号
    seq_num: u64 = 0,

    /// Total ping cycles including the first one (> 0).  总 ping 周期数（含首次，> 0）
    remaining: u32 = 0,

    /// Nanoseconds between pings (sleep in PingDecision)  ping 间隔（纳秒）
    interval_ns: u64 = 0,

    /// Per-window width in nanoseconds (> 0, e.g. 60_000_000_000 = 1 min)  每窗口宽度（纳秒）
    window_duration_ns: u64 = 0,

    /// Monotonic nanosecond timestamp of first PingQuery.  首次 PingQuery 的单调纳秒时间戳
    /// 0 means not started.  0 表示尚未开始
    session_start_ns: u64 = 0,

    /// Dynamic list of per-window metrics, append-only.  每窗口指标的动态列表，仅追加
    windows: std.ArrayList(WindowMetrics),
};

/// Server-side protocol context — stateless across pings.  服务端协议上下文——跨 ping 无状态
pub const ServerContext = struct {
    /// Last received seq_num (from PingQuery), echoed back in PingResponse  最近收到的序号
    last_seq_num: u64 = 0,

    /// Last received client_send_time (from PingQuery), echoed back  最近收到的客户端发送时间
    last_client_send_time: u64 = 0,
};

// ─────────────────── Payload Types  负载类型 ───────────────────

pub const PingPayload = struct {
    seq_num: u64,
    /// Monotonic nanosecond timestamp on the client  客户端单调纳秒时间戳
    client_send_time: u64,
};

pub const PongPayload = struct {
    seq_num: u64,
    /// Echoed from PingPayload — original client-side timestamp  回显 PingPayload 中的原始客户端时间戳
    client_send_time: u64,
    /// Server-side dwell time in nanoseconds  服务端停留时间（纳秒）
    server_dwell_ns: u64,
};

// ─────────────────── Monotonic clock helper  单调时钟辅助函数 ───────────────────

/// Returns the current CLOCK_MONOTONIC value in nanoseconds.  返回当前 CLOCK_MONOTONIC 值（纳秒）
/// Panics if the syscall fails (should never happen in practice).  syscall 失败时 panic
pub fn monotonicNs() u64 {
    var ts: std.os.linux.timespec = undefined;
    if (std.os.linux.clock_gettime(std.os.linux.CLOCK.MONOTONIC, &ts) != 0) {
        @panic("clock_gettime(MONOTONIC) failed");
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// Sleep for `ns` nanoseconds using nanosleep.  通过 nanosleep 休眠 ns 纳秒
pub fn sleepNs(ns: u64) void {
    const req = std.os.linux.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = std.os.linux.nanosleep(&req, null);
}
