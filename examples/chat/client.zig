// 聊天室客户端 demo
//
// 结构：
//   stdin fiber —— 异步读 stdin，投递到输入队列（/quit 退出）
//   Push fiber  —— 消费服务器推送并打印
//   Ctrl 主循环 —— 锁步协议：注册 → 发送消息 / 心跳 / 退出
//
// 服务器死亡检测由 Ctrl 承担：客户端每 100ms 发一次心跳，
// 服务器 5s 无响应（CTRL_RECV_TIMEOUT_MS）即判定连接死亡。

const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const chat = @import("protocol.zig");

/// 与 server.zig 保持一致的 Mux 配置。
const Mux = polyrole.family_mux_channel.MultiplexChannel(&.{
    .{ .capacity = 1, .max_message_size = 4096, .overflow = .close_channel },
    .{ .capacity = 16, .max_message_size = 512, .overflow = .backpressure },
}, 4100);

const CtrlRunner = polyrole.runner.Runner(chat.Login);
const PushRunner = polyrole.runner.Runner(chat.Deliver);

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT: u16 = 7788;
const CTRL_RECV_TIMEOUT_MS: u64 = 5000;

pub fn main(init: std.process.Init) !void {
    var rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();

    var args_it = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_it.next(); // 程序名
    const nickname_arg = args_it.next() orelse {
        std.log.info("用法: chat-client <昵称> [host] [port]", .{});
        return;
    };
    const host = if (args_it.next()) |h| h else DEFAULT_HOST;
    const port: u16 = if (args_it.next()) |p| try std.fmt.parseInt(u16, p, 10) else DEFAULT_PORT;

    const addr = try zio.net.IpAddress.parseIp4(host, port);
    var stream = try addr.connect(.{});
    defer stream.close();

    var sc: polyrole.channel.StreamChannel = undefined;
    try sc.init(init.gpa, stream, 4096, 4096, 4096);
    defer sc.deinit(init.gpa);

    var mux: Mux = undefined;
    try mux.initFromChannel(init.gpa, &sc);
    defer mux.deinit();

    var nickname: [chat.MAX_NICK]u8 = [_]u8{0} ** chat.MAX_NICK;
    const n = @min(nickname_arg.len, chat.MAX_NICK - 1);
    @memcpy(nickname[0..n], nickname_arg[0..n]);

    var input_buf: [64]chat.UserInput = undefined;
    var input: zio.Channel(chat.UserInput) = .init(&input_buf);

    // Ctrl 与 Push 两个 fiber 共享 stdout，输出需串行化
    var out_mu: zio.Mutex = .init;
    var ctrl_ctx = chat.ClientContext.init(zio.stdout(), nickname, &input);
    ctrl_ctx.out_lock = &out_mu;
    var push_ctx = chat.PushClientContext{ .out = zio.stdout(), .out_lock = &out_mu };

    _ = try zio.spawn(stdinLoop, .{&input});
    var push_h = try zio.spawn(PushFn.run, .{ &push_ctx, mux.subChannel(1) });

    // Ctrl 主循环：阻塞直到退出（/quit）或连接错误。
    CtrlRunner.symmetric_run(.client, &ctrl_ctx, mux.subChannel(0), chat.Login, CTRL_RECV_TIMEOUT_MS) catch |err| {
        std.log.err("连接结束: {}", .{err});
    };

    // 取消 Push 消费并等它退出（避免 Mux 缓冲被提前释放）。
    push_h.cancel();
    push_h.join() catch {};
}

const PushFn = struct {
    fn run(ctx: *chat.PushClientContext, ch: *Mux.SubChannel) anyerror!void {
        // Push 通道不设超时：服务器死亡由 Ctrl 心跳检测，这里只需无限等待推送。
        PushRunner.symmetric_run(.client, ctx, ch, chat.Deliver, null) catch {};
    }
};

/// 异步读 stdin 并投递到输入队列。输入过快时 send 阻塞（背压到终端）。
///
/// 注意：不能用 `takeDelimiterExclusive`——Zig 0.16 std 的实现只 toss
/// 不含分隔符的部分，`\n` 会永远留在缓冲里，导致每次调用都返回空行。
/// `takeDelimiter` 正确消费分隔符，EOF 返回 null。
fn stdinLoop(input: *zio.Channel(chat.UserInput)) anyerror!void {
    var buf: [1024]u8 = undefined;
    var reader = zio.stdin().reader(&buf);
    while (true) {
        const line = reader.interface.takeDelimiter('\n') catch return;
        const l = line orelse return; // EOF
        if (std.mem.eql(u8, l, "/quit")) {
            input.send(.quit) catch return;
            return;
        }
        var text: [chat.MAX_TEXT]u8 = [_]u8{0} ** chat.MAX_TEXT;
        const n = @min(l.len, chat.MAX_TEXT - 1);
        @memcpy(text[0..n], l[0..n]);
        input.send(.{ .msg = text }) catch return;
    }
}
