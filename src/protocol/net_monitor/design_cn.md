# 网络监控协议设计

## 概述

基于 polyrole-cs 的会话式网络连通性和延迟监控协议。客户端周期性发送
ping，服务端原样回显。RTT 完全在客户端单侧计算——不依赖跨机时间同步。

所有时间单位统一使用**毫秒**。网络 RTT 本质上是毫秒级测量——纳秒精度
无实际价值。

**传输无关。** 可工作于 StreamChannel、TlsChannel 等任何 send/recv 传输。

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
| `server_dwell_ms` | `u64` | server | 服务端停留时间（毫秒） |

## 负载

```zig
pub const PingPayload = struct { seq_num: u64, client_send_time: u64, };
pub const PongPayload = struct { seq_num: u64, client_send_time: u64, server_dwell_ms: u64, };
```

## 上下文

```zig
pub const WindowMetrics = struct {
    start_ms: u64 = 0,
    rtt_sum_ms: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ms: u64 = std.math.maxInt(u64),
    rtt_max_ms: u64 = 0,
};

pub const ClientContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    seq_num: u64 = 0,
    remaining: u32 = 0,              // 总 ping 次数（> 0）
    interval_ms: u64 = 0,            // ping 间隔
    window_duration_ms: u64 = 0,     // 每窗口宽度（> 0，如 60000 = 1 分钟）
    session_start_ms: u64 = 0,       // 0 = 尚未开始
    windows: std.ArrayList(WindowMetrics),
};

pub const ServerContext = struct {
    io: std.Io,
    last_seq_num: u64 = 0,
    last_client_send_time: u64 = 0,
};
```

## 状态详述

### PingQuery (client → server)

`process()` 记录 `session_start_ms`（仅首次），递增 `seq_num`，发送 ping。

`preprocess()` 将收到的字段存入 ServerContext 供回显。

### PingResponse (server → client)

`process()` 在响应前后各采样一次时钟，差值即为 `server_dwell_ms`。

`preprocess()` 计算 `rtt_net`，按响应到达时刻确定窗口，必要时扩容，
累积指标。返回 `!void`——分配可能失败。

### PingDecision (client)

`process()` 递减 `remaining`；若为 0 返回 `.close`；否则休眠 `interval_ms`
后返回 `.ping_again`。返回 `!@This()`——休眠可能被取消。

`remaining` 计包含首次 ping 的**总数**。`remaining = 5` 正好 5 次 ping。
必须 > 0。

## 调用示例

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ms = 1000,
    .window_duration_ms = 60000,
    .windows = std.ArrayList(WindowMetrics).empty,
};
defer client.windows.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue;
    std.debug.print("窗口 {d}: 平均={d} ms\n", .{ i, w.rtt_sum_ms / w.rtt_count });
}
```

## 错误传播

`PingDecision.process` 和 `PingResponse.preprocess` 返回错误。Runner
在编译期检测并通过 `try` 传播。

## 设计决策

| 决策 | 理由 |
|------|------|
| 无状态服务端 | 可扩展至任意并发客户端 |
| 时钟无关计时 | RTT 仅用客户端时钟；停留时间用服务端本地相对时间 |
| 全局毫秒单位 | 网络 RTT 是毫秒级别；纳秒只增加噪音，不增加价值 |
| Io 接口 | 使用 `Io.Timestamp.now` / `Io.sleep`——可移植 |
| 错误传播 | 无 panic——失败通过 Runner 返回 |
| 填空式窗口追加 | `while items.len <= index` 填补慢响应跨窗口空位 |
