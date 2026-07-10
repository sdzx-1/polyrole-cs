# 网络监控协议设计

## 概述

基于 polyrole-cs 的会话式网络连通性和延迟监控协议。客户端周期性发送
ping，服务端原样回显。RTT 完全在客户端单侧计算——不依赖跨机时间同步。

**传输无关。** 协议只需 send/recv 接口，可工作于 StreamChannel、TlsChannel
等任何实现该接口的传输之上。

## 状态机

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (循环)
                                                          │
                                                          └── close → Exit
```

## 时钟无关的 RTT

| 字段 | 类型 | 谁填 | 用途 |
|------|------|------|------|
| `seq_num` | `u64` | client，服务端回显 | 排序，诊断 |
| `client_send_time` | `u64` | client，服务端回显 | RTT = now - client_send_time（同钟） |
| `server_dwell_ns` | `u64` | server | 服务端停留时间（同钟差值） |

## 负载

```zig
pub const PingPayload = struct { seq_num: u64, client_send_time: u64, };
pub const PongPayload = struct { seq_num: u64, client_send_time: u64, server_dwell_ns: u64, };
```

## 上下文

```zig
pub const WindowMetrics = struct {
    start_ns: u64 = 0,
    rtt_sum_ns: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ns: u64 = std.math.maxInt(u64),
    rtt_max_ns: u64 = 0,
};

pub const ClientContext = struct {
    io: std.Io,                          // 时钟和休眠
    allocator: std.mem.Allocator,        // 窗口列表扩容
    seq_num: u64 = 0,
    remaining: u32 = 0,                  // 总 ping 次数（> 0）
    interval_ns: u64 = 0,                // ping 间隔
    window_duration_ns: u64 = 0,         // 每窗口宽度
    session_start_ns: u64 = 0,           // 0 = 尚未开始
    windows: std.ArrayList(WindowMetrics),
};

pub const ServerContext = struct {
    io: std.Io,                          // 时钟（停留时间测量）
    last_seq_num: u64 = 0,              // 从 PingQuery 回显
    last_client_send_time: u64 = 0,     // 从 PingQuery 回显
};
```

## 状态详述

### PingQuery (client → server)

```zig
pub const PingQuery = union(enum) {
    to_server: Data(PingPayload, PingResponse),
    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingQuery" };

    pub fn process(ctx: *ClientContext) @This() { ... }
    pub fn preprocess(ctx: *ServerContext, result: @This()) void { ... }
};
```

`process()` 记录 `session_start_ns`（仅首次），递增 `seq_num`，发送 ping。

`preprocess()` 将收到的 `seq_num` 和 `client_send_time` 存入 ServerContext，
供 PingResponse 回显。

### PingResponse (server → client)

```zig
pub const PingResponse = union(enum) {
    to_client: Data(PongPayload, PingDecision),
    pub const info: NetMonitorInfo = .{ .agent = .server, .name = "PingResponse" };

    pub fn process(ctx: *ServerContext) @This() { ... }
    pub fn preprocess(ctx: *ClientContext, result: @This()) !void { ... }
};
```

`process()` 在构造响应前后各采样一次单调时钟，差值即为 `server_dwell_ns`。

`preprocess()` 计算 `rtt_net`，按响应到达时刻确定窗口索引，必要时扩容窗口
列表，累积 RTT 指标。返回 `!void`——窗口扩容可能因内存不足失败。

### PingDecision (client)

```zig
pub const PingDecision = union(enum) {
    ping_again: Data(void, PingQuery),
    close: Data(void, Exit),
    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingDecision" };

    pub fn process(ctx: *ClientContext) !@This() { ... }
};
```

`process()` 递减 `remaining`；若为 0，返回 `.close`；否则休眠 `interval_ns`
后返回 `.ping_again`。返回 `!@This()`——休眠可能因取消而失败。

`remaining` 计包含首次 ping 的**总数**。设置 `remaining = 5` 正好 5 次 ping。
必须 > 0。

## Channel

```
Runner(net_monitor).symmetric_run(role, ctx, channel, PingQuery)
```

可工作于 `StreamChannel`（原始 TCP）或 `TlsChannel`（加密）。协议不假设
底层传输。

## 调用示例

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ns = 1_000_000_000,
    .window_duration_ns = 60_000_000_000,
    .windows = std.ArrayList(WindowMetrics).empty,
};
defer client.windows.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue;
    std.debug.print("窗口 {d}: 平均={d} ns\n", .{ i, w.rtt_sum_ns / w.rtt_count });
}
```

## 错误传播

`PingDecision.process` 和 `PingResponse.preprocess` 返回错误（休眠取消、
内存分配失败）。Runner 在编译期检测并通过 `try` 传播给调用方。

协议没有自定义错误集——依赖标准库错误类型。

## 设计决策

| 决策 | 理由 |
|------|------|
| 无状态服务端 | 可扩展至任意并发客户端 |
| 时钟无关计时 | RTT 仅用客户端时钟；停留时间用服务端本地相对时间 |
| 传输无关 | 仅需 send/recv——StreamChannel、TlsChannel 等 |
| Io 接口 | 使用 `Io.Timestamp.now(io, .awake)` 和 `Io.sleep`——可移植，与 simple_tls 一致 |
| 错误传播 | 无 panic——分配和休眠失败通过 Runner 返回错误 |
| 窗口化聚合 | 通过 `ArrayList` 实现每窗口指标，调用方管理生命周期 |
| 填空式窗口追加 | `while items.len <= index` 追加——填补慢响应跨窗口边界的空位 |
