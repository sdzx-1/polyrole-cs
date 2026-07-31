// 聊天室压测客户端（tools/chat_loadtest.zig）
//
// 用法: chat-loadtest <连接数> [host] [port] [duration_s] [msgs] [batch_size]
// 默认: host=127.0.0.1 port=7788 duration=10 msgs=0 batch_size=连接数(不分批)
//
// 建立 N 个真实 chat 客户端连接（完整 Ctrl+Push 协议），全部注册后维持
// 心跳 duration 秒，然后优雅退出。打印：成功/失败数、总耗时、进程内存。
//
// 当 msgs > 0 时追加广播测试：客户端 0 在注册完成后连发 msgs 条聊天消息，
// 其余 N-1 个客户端统计收到的 chat 消息数（期望 (N-1)*msgs），并测量
// 首条/末条广播延迟。覆盖：Room O(N) 广播、Push 通道吞吐、inbox 背压。
//
// batch_size < N 时按批注册（每批后等待 2s 让加入通知风暴消化）：
// 同时注册 N 人会触发 O(N²) 的加入通知风暴，服务器单线程 Push 推送消化
// 不及，导致客户端 Push 通道被按慢消费者断开（1000 人同时注册仅 ~10%
// 客户端存活）。分批模拟真实渐进加入，注册风暴可控后再测广播。

const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const chat = @import("chat");

const Mux = polyrole.family_mux_channel.MultiplexChannel(&.{
    .{ .capacity = 1, .max_message_size = 4096, .overflow = .close_channel },
    .{ .capacity = 16, .max_message_size = 512, .overflow = .backpressure },
}, 4100);

const CtrlRunner = polyrole.runner.Runner(chat.Login);
const PushRunner = polyrole.runner.Runner(chat.Deliver);

/// 每批注册后的加入通知消化等待（毫秒）。
const BATCH_GAP_MS: u64 = 2000;
/// 广播测试的基础宽限时间（毫秒），另加 批数 × BATCH_GAP_MS。
const BROADCAST_BASE_SETTLE_MS: u64 = 5000;

const Stats = struct {
    ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    fail: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    /// 收到的 chat 消息总数（广播测试）
    received: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    /// 广播开始时间戳（客户端 0 投递第一条时设置）
    broadcast_start_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// 首条 chat 消息收到时间戳
    first_recv_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(std.math.maxInt(u64)),
    /// 末条 chat 消息收到时间戳
    last_recv_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

pub fn main(init: std.process.Init) !void {
    // 压测客户端跑完整协议（Ctrl 在 group fiber 上），栈需求高于纯心跳；
    // 用默认 256KB committed（压测机本地，不追求 64KB 的万级栈内存）
    var rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var args_it = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_it.next(); // 程序名
    const n_arg = args_it.next() orelse {
        std.log.info("用法: chat-loadtest <连接数> [host] [port] [duration_s] [msgs] [batch_size]", .{});
        return;
    };
    const n = try std.fmt.parseInt(usize, n_arg, 10);
    const host = if (args_it.next()) |h| h else "127.0.0.1";
    const port: u16 = if (args_it.next()) |p| try std.fmt.parseInt(u16, p, 10) else 7788;
    const duration_s: u64 = if (args_it.next()) |d| try std.fmt.parseInt(u64, d, 10) else 10;
    const msgs: usize = if (args_it.next()) |m| try std.fmt.parseInt(usize, m, 10) else 0;
    const batch_size: usize = if (args_it.next()) |b| try std.fmt.parseInt(usize, b, 10) else n;

    const addr = try zio.net.IpAddress.parseIp4(host, port);
    const batch_count = (n + batch_size - 1) / batch_size;
    std.log.info("压测：{d} 连接 → {s}:{d}，心跳 {d}s，广播 {d} 条/客户端0，{d} 批×{d}", .{
        n, host, port, duration_s, msgs, batch_count, batch_size,
    });

    var stats = Stats{};
    const start_ms = chat.monotonicMs();

    var group: zio.Group = .init;
    defer group.cancel();
    const ClientFn = struct {
        fn run(a: zio.net.IpAddress, i: usize, s: *Stats, dur_ms: u64, m: usize, settle: u64, gpa: std.mem.Allocator) anyerror!void {
            try clientRun(a, i, s, dur_ms, m, settle, gpa);
        }
    };
    // 广播宽限：批间消化 + 基础余量（客户端 0 用它决定何时发广播）
    const settle_ms = BROADCAST_BASE_SETTLE_MS + batch_count * BATCH_GAP_MS;
    if (n == 1) {
        // 单连接：root 直接跑（对齐 chat-client 结构，用于诊断）
        try clientRun(addr, 0, &stats, duration_s * 1000, msgs, settle_ms, std.heap.page_allocator);
    } else {
        // 分批注册：每批后等待加入通知风暴消化（见文件头说明）
        var spawned: usize = 0;
        while (spawned < n) {
            const this_batch = @min(batch_size, n - spawned);
            for (0..this_batch) |k| {
                try group.spawn(ClientFn.run, .{ addr, spawned + k, &stats, duration_s * 1000, msgs, settle_ms, std.heap.page_allocator });
            }
            spawned += this_batch;
            if (spawned < n) try zio.sleep(zio.Duration.fromMilliseconds(BATCH_GAP_MS));
        }
        try group.wait();
    }

    const elapsed_ms = chat.monotonicMs() - start_ms;
    std.log.info("完成：成功 {d}，失败 {d}，总耗时 {d}ms", .{
        stats.ok.load(.acquire),
        stats.fail.load(.acquire),
        elapsed_ms,
    });
    if (msgs > 0 and n > 1) {
        const expected = (n - 1) * msgs;
        const got = stats.received.load(.acquire);
        const bs = stats.broadcast_start_ms.load(.acquire);
        const first = stats.first_recv_ms.load(.acquire);
        const last = stats.last_recv_ms.load(.acquire);
        if (bs > 0 and first != std.math.maxInt(u64)) {
            std.log.info("广播：期望 {d} 条，实收 {d} 条（{d}%），首条延迟 {d}ms，末条延迟 {d}ms", .{
                expected,
                got,
                got * 100 / expected,
                first - bs,
                last - bs,
            });
        } else {
            std.log.info("广播：期望 {d} 条，实收 {d} 条", .{ expected, got });
        }
    }
    std.log.info("进程内存 VmRSS: {s}", .{try readVmrss(init.io, init.gpa)});
}

/// 单个压测客户端：连接 → 注册 → 心跳维持 duration 后经 /quit 优雅退出。
/// i == 0 且 msgs > 0 时兼任广播发送方（等 settle_ms 后发）。
fn clientRun(
    addr: zio.net.IpAddress,
    i: usize,
    stats: *Stats,
    duration_ms: u64,
    msgs: usize,
    settle_ms: u64,
    allocator: std.mem.Allocator,
) anyerror!void {
    var stream = try addr.connect(.{});
    defer stream.close();

    var sc: polyrole.channel.StreamChannel = undefined;
    try sc.init(allocator, stream, 4096, 4096, 4096);
    defer sc.deinit(allocator);

    var mux: Mux = undefined;
    try mux.initFromChannel(allocator, &sc);
    defer mux.deinit();

    var input_buf: [16]chat.UserInput = undefined;
    var input: zio.Channel(chat.UserInput) = .init(&input_buf);

    var nickname: [chat.MAX_NICK]u8 = [_]u8{0} ** chat.MAX_NICK;
    _ = std.fmt.bufPrint(&nickname, "user{d}", .{i}) catch unreachable;

    var ctrl_ctx = chat.ClientContext.init(null, nickname, &input);
    var push_buf: [256]chat.PushPayload = undefined;
    var push_inbox: zio.Channel(chat.PushPayload) = .init(&push_buf);
    var push_ctx = chat.PushClientContext{ .inbox = &push_inbox, .inbox_all = false };

    const PushFn = struct {
        fn run(ctx: *chat.PushClientContext, ch: *Mux.SubChannel) anyerror!void {
            PushRunner.symmetric_run(.client, ctx, ch, chat.Deliver, null) catch {};
        }
    };
    var push_h = try zio.spawn(PushFn.run, .{ &push_ctx, mux.subChannel(1) });
    // 消费收件箱并统计 chat 消息：注册风暴的加入通知若不消费会把 inbox 塞满，
    // 背压到服务器后被当作慢消费者断开。
    var drain_h = try zio.spawn(drainPush, .{ stats, &push_inbox });
    // 限时退出：duration 到期后投递 /quit，Ctrl 走 Exit 优雅退出
    _ = try zio.spawn(quitAfter, .{ duration_ms, &input });
    // 广播发送方：仅客户端 0，等 settle_ms（全部注册 + 风暴消化）后连发 msgs 条
    if (i == 0 and msgs > 0) {
        _ = try zio.spawn(broadcastDriver, .{ msgs, settle_ms, &stats.broadcast_start_ms, &input });
    }

    // Ctrl 正常结束（/quit → Exit）即视为成功；任何错误视为失败。
    var ok = true;
    CtrlRunner.symmetric_run(.client, &ctrl_ctx, mux.subChannel(0), chat.Login, 20000) catch {
        ok = false;
    };
    if (ok) _ = stats.ok.fetchAdd(1, .monotonic) else _ = stats.fail.fetchAdd(1, .monotonic);

    push_h.cancel();
    drain_h.cancel();
    push_h.join() catch {};
    drain_h.join() catch {};
}

/// 消费推送收件箱：统计收到的 chat 消息与时间戳。
fn drainPush(stats: *Stats, inbox: *zio.Channel(chat.PushPayload)) anyerror!void {
    while (true) {
        const p = inbox.receive() catch return;
        if (p.kind == @intFromEnum(chat.PushKind.chat)) {
            _ = stats.received.fetchAdd(1, .monotonic);
            const now = chat.monotonicMs();
            _ = stats.first_recv_ms.fetchMin(now, .monotonic);
            _ = stats.last_recv_ms.fetchMax(now, .monotonic);
        }
    }
}

/// 广播发送方：等 settle_ms（覆盖分批注册与加入通知风暴消化）后连发 msgs 条聊天消息。
fn broadcastDriver(msgs: usize, settle_ms: u64, start_ms: *std.atomic.Value(u64), input: *zio.Channel(chat.UserInput)) anyerror!void {
    try chat.sleepMs(settle_ms);
    start_ms.store(chat.monotonicMs(), .release);
    for (0..msgs) |k| {
        var text: [chat.MAX_TEXT]u8 = [_]u8{0} ** chat.MAX_TEXT;
        _ = std.fmt.bufPrint(&text, "bench-{d}", .{k}) catch unreachable;
        input.send(.{ .msg = text }) catch return;
    }
}

/// 限时退出：duration 毫秒后投递 /quit，让 Ctrl 优雅退出。
fn quitAfter(duration_ms: u64, input: *zio.Channel(chat.UserInput)) anyerror!void {
    try chat.sleepMs(duration_ms);
    input.send(.quit) catch {};
}

/// 读取进程 VmRSS（KB）。
fn readVmrss(io: std.Io, gpa: std.mem.Allocator) ![]const u8 {
    const content = try std.Io.Dir.cwd().readFileAlloc(io, "/proc/self/status", gpa, .limited(64 * 1024));
    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "VmRSS:")) {
            const v = std.mem.trim(u8, line[6..], " \t");
            const p = std.mem.lastIndexOfScalar(u8, v, ' ');
            return if (p) |idx| v[idx + 1 ..] else v;
        }
    }
    return "?";
}
