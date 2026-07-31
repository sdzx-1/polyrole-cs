// 聊天室服务器 demo
//
// 结构：
//   main           —— 监听端口，每连接 spawn 一个监督 fiber
//   Room fiber     —— 串行处理成员操作（register/remove/broadcast/who）
//   监督 fiber     —— 每连接两个协议 fiber（Ctrl 锁步 + Push 推送），
//                     任一退出即清理整个连接（remove + 广播离开 + 关 Mux）
//
// 10000 并发支持（见 docs/chat-scale-10000.md）：
//   - fiber 栈初始提交 256KB → 64KB（万连接栈内存 10GB → 2.5GB）
//   - 心跳间隔 1s（客户端时钟驱动），掉线检测放宽到 20s
//   - Room 成员表动态扩容，无上限

const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const chat = @import("protocol.zig");

/// 控制通道：锁步，容量 1；推送通道：容量 16 + 背压。
const Mux = polyrole.family_mux_channel.MultiplexChannel(&.{
    .{ .capacity = 1, .max_message_size = 4096, .overflow = .close_channel },
    .{ .capacity = 16, .max_message_size = 512, .overflow = .backpressure },
}, 4100);

const CtrlRunner = polyrole.runner.Runner(chat.Login);
const PushRunner = polyrole.runner.Runner(chat.Deliver);

const DEFAULT_PORT: u16 = 7788;
/// 服务器端 Ctrl recv 超时：客户端心跳 1s，20s 未收到即判定掉线。
const CTRL_RECV_TIMEOUT_MS: u64 = 20000;

pub fn main(init: std.process.Init) !void {
    // 万连接场景：每连接 4 fiber × 64KB 初始栈 ≈ 2.5GB（默认 256KB 则 10GB）
    var rt = try zio.Runtime.init(init.gpa, .{
        .stack_pool = .{ .maximum_size = 8 * 1024 * 1024, .committed_size = 64 * 1024 },
    });
    defer rt.deinit();

    var args_it = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_it.next(); // 程序名
    var port: u16 = DEFAULT_PORT;
    if (args_it.next()) |arg| port = try std.fmt.parseInt(u16, arg, 10);

    var room: chat.Room = undefined;
    room.init(init.gpa);
    defer room.deinit();
    try room.spawn();

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var listener = try addr.listen(.{});
    defer listener.close();

    std.log.info("聊天室服务器监听 {f}（Ctrl+C 退出）", .{listener.socket.address});

    var group: zio.Group = .init;
    defer group.cancel();

    while (true) {
        const stream = try listener.accept(.{});
        try group.spawn(serveConnection, .{ init.gpa, stream, &room, CTRL_RECV_TIMEOUT_MS });
    }
}

/// 单个连接的生命周期监督者：Ctrl 协议由本 fiber 亲自驱动（它是连接生命线），
/// Push 推送跑在独立 fiber；Ctrl 结束（退出/掉线/注册失败）即清理整个连接。
/// （pub：供 test.zig 复用）
pub fn serveConnection(
    allocator: std.mem.Allocator,
    stream: zio.net.Stream,
    room: *chat.Room,
    ctrl_timeout_ms: u64,
) anyerror!void {
    var sc: polyrole.channel.StreamChannel = undefined;
    try sc.init(allocator, stream, 4096, 4096, 4096);
    defer sc.deinit(allocator);

    var mux: Mux = undefined;
    try mux.initFromChannel(allocator, &sc);
    errdefer mux.deinit();

    // 广播队列容量按心跳间隔与广播频率配置（64 条足够 1s 心跳下的突发）
    var inbox_buf: [64]chat.PushPayload = undefined;
    var inbox: zio.Channel(chat.PushPayload) = .init(&inbox_buf);

    var ctrl_ctx = chat.ServerContext.init(room, &inbox);
    var push_ctx = chat.PushServerContext{ .inbox = &inbox };

    const PushFn = struct {
        fn run(ctx: *chat.PushServerContext, ch: *Mux.SubChannel) anyerror!void {
            PushRunner.symmetric_run(.server, ctx, ch, chat.Deliver, null) catch {};
        }
    };
    var push_h = try zio.spawn(PushFn.run, .{ &push_ctx, mux.subChannel(1) });

    // Ctrl 结束路径：客户端退出（/quit）、注册失败、心跳超时、连接断开。
    CtrlRunner.symmetric_run(.server, &ctrl_ctx, mux.subChannel(0), chat.Login, ctrl_timeout_ms) catch {};

    // 若已注册则从房间移除并广播离开（幂等：/quit 路径已移除则无操作）。
    ctrl_ctx.leave() catch {};

    // 关闭广播队列，让 Push fiber 排空后自然退出；等它退出后再释放 Mux 缓冲。
    inbox.close(.graceful);
    push_h.join() catch {};

    mux.deinit();
}
