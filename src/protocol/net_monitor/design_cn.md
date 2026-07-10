# 网络监控协议设计

## 概述

基于 polyrole-cs 的会话式网络连通性和延迟监控协议。客户端周期性发送
ping，服务端原样回显。RTT 完全在客户端本地计算——`client_send_time` 不
经过网络传输，仅在本地存储并在响应到达时与当前时间比较。

每次响应记录为一条 `PingResult`，存入客户端的 `ArrayList`，可选
`max_results` 上限。所有时间单位统一使用毫秒。

**传输无关。** 可工作于 StreamChannel、TlsChannel 等任何 send/recv 传输。

## 状态机

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (循环)
                                                          │
                                                          └── close → Exit
```

## 负载

```zig
pub const PingPayload = struct { seq_num: u64, };
pub const PongPayload = struct { seq_num: u64, };
```

## 上下文

```zig
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
};

pub const ClientContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    seq_num: u64 = 0,
    last_send_ms: u64 = 0,           // 本地时间戳，用于 RTT 计算
    remaining: u32 = 0,              // 总 ping 次数（> 0）
    interval_ms: u64 = 0,            // ping 间隔
    max_results: u32 = 0,            // 0 = 无限制
    results: std.ArrayList(PingResult),
};

pub const ServerContext = struct {
    last_seq_num: u64 = 0,
};
```

## 状态详述

### PingQuery (client → server)

`process()` 递增 `seq_num`，记录 `last_send_ms`，发送 ping。

`preprocess()` 将收到的 `seq_num` 存入 ServerContext 供回显。

### PingResponse (server → client)

纯回显——`process()` 直接返回 PingQuery.preprocess 存储的 `seq_num`。
无需时钟采样，无需任何处理。

`preprocess()` 计算 `rtt_ms = now - last_send_ms`，追加一条
`PingResult` 到 `results`（受 `max_results` 限制）。返回 `!void`。

### PingDecision (client)

`process()` 递减 `remaining`；若为 0 返回 `.close`；否则休眠后返回
`.ping_again`。返回 `!@This()`。

`remaining` 计包含首次 ping 的**总数**。必须 > 0。

## 调用示例

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ms = 1000,
    .max_results = 1000,
    .results = std.ArrayList(PingResult).empty,
};
defer client.results.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.results.items) |r| {
    std.debug.print("[{d}] rtt={d}ms dwell={d}ms\n",
        .{ r.seq_num, r.rtt_ms, r.server_dwell_ms });
}
```

## 错误传播

`PingDecision.process` 和 `PingResponse.preprocess` 返回错误。Runner
在编译期检测并通过 `try` 传播。

## 设计决策

| 决策 | 理由 |
|------|------|
| 无状态服务端 | 可扩展至任意并发客户端 |
| 本地 RTT 计算 | `client_send_time` 不经过网络——payload 更小，无冗余 |
| 毫秒单位 | 网络 RTT 是毫秒级别 |
| 每条记录独立 | 简单——调用方按需分组 |
| max_results 上限 | 防止长会话无限增长 |
| Io 接口 | 通过 `Io.Timestamp` / `Io.sleep` 可移植 |
| 无 panic | 错误通过 Runner 传播 |
