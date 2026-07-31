// 聊天室压测客户端（tools/chat_loadtest.zig）
//
// 用法: chat-loadtest <连接数> [host] [port] [duration_s]
// 默认: host=127.0.0.1 port=7788 duration=10
//
// 建立 N 个真实 chat 客户端连接（完整 Ctrl+Push 协议），全部注册后维持
// 心跳 duration 秒，然后优雅退出。打印：成功/失败数、总耗时、进程内存。
//
// 验证目标（docs/chat-scale-10000.md §5）：
//   - 万级连接的注册吞吐与成功率（Room 动态成员表）
//   - 心跳低频（1s）下的连接维持
//   - 每连接 4 fiber × 64KB 栈的内存占用

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

const Stats = struct {
    ok: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    fail: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
};

pub fn main(init: std.process.Init) !void {
    // 压测客户端跑完整协议（Ctrl 在 group fiber 上），栈需求高于纯心跳；
    // 用默认 256KB committed（压测机本地，不追求 64KB 的万级栈内存）
    var rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var args_it = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_it.next(); // 程序名
    const n_arg = args_it.next() orelse {
        std.log.info("用法: chat-loadtest <连接数> [host] [port] [duration_s]", .{});
        return;
    };
    const n = try std.fmt.parseInt(usize, n_arg, 10);
    const host = if (args_it.next()) |h| h else "127.0.0.1";
    const port: u16 = if (args_it.next()) |p| try std.fmt.parseInt(u16, p, 10) else 7788;
    const duration_s: u64 = if (args_it.next()) |d| try std.fmt.parseInt(u64, d, 10) else 10;

    const addr = try zio.net.IpAddress.parseIp4(host, port);
    std.log.info("压测：{d} 连接 → {s}:{d}，心跳维持 {d}s", .{ n, host, port, duration_s });

    var stats = Stats{};
    const start_ms = chat.monotonicMs();

    var group: zio.Group = .init;
    defer group.cancel();
    const ClientFn = struct {
        fn run(a: zio.net.IpAddress, i: usize, s: *Stats, dur_ms: u64, gpa: std.mem.Allocator) anyerror!void {
            try clientRun(a, i, s, dur_ms, gpa);
        }
    };
    if (n == 1) {
        // 单连接：root 直接跑（对齐 chat-client 结构，用于诊断）
        try clientRun(addr, 0, &stats, duration_s * 1000, std.heap.page_allocator);
    } else {
        for (0..n) |i| {
            try group.spawn(ClientFn.run, .{ addr, i, &stats, duration_s * 1000, std.heap.page_allocator });
        }
        try group.wait();
    }

    const elapsed_ms = chat.monotonicMs() - start_ms;
    std.log.info("完成：成功 {d}，失败 {d}，总耗时 {d}ms", .{
        stats.ok.load(.acquire),
        stats.fail.load(.acquire),
        elapsed_ms,
    });
    std.log.info("进程内存 VmRSS: {s}", .{try readVmrss(init.io, init.gpa)});
}

/// 单个压测客户端：连接 → 注册 → 心跳维持 duration 后经心跳通道关闭优雅退出。
fn clientRun(addr: zio.net.IpAddress, i: usize, stats: *Stats, duration_ms: u64, allocator: std.mem.Allocator) anyerror!void {
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
    var push_ctx = chat.PushClientContext{ .inbox = &push_inbox };

    const PushFn = struct {
        fn run(ctx: *chat.PushClientContext, ch: *Mux.SubChannel) anyerror!void {
            PushRunner.symmetric_run(.client, ctx, ch, chat.Deliver, null) catch {};
        }
    };
    var push_h = try zio.spawn(PushFn.run, .{ &push_ctx, mux.subChannel(1) });
    // 消费收件箱（丢弃）：注册风暴的加入通知若不消费会把 inbox 塞满，
    // 背压到服务器后被当作慢消费者断开。
    var drain_h = try zio.spawn(drainPush, .{&push_inbox});
    // 限时退出：duration 到期后投递 /quit，Ctrl 走 Exit 优雅退出
    _ = try zio.spawn(quitAfter, .{ duration_ms, &input });

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

/// 消费推送收件箱（丢弃）。
fn drainPush(inbox: *zio.Channel(chat.PushPayload)) anyerror!void {
    while (true) _ = inbox.receive() catch return;
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
