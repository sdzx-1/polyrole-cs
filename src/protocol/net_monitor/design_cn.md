# 网络监控协议设计

## 概述

基于 polyrole-cs 的会话式网络连通性和延迟监控协议。客户端周期性发送
ping，服务端原样回显。RTT 完全在客户端单侧计算，使用同一时钟——不依赖
跨机时间同步。

**传输无关。** 协议只需要一个实现了 `send(state_id, State, result)` 和
`recv(state_id, State) -> result` 接口的 channel。可工作于 `StreamChannel`
（原始 TCP）、`TlsChannel`（加密）或任何实现同一接口的自定义传输之上。

## 状态机

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (循环)
                                                          │
                                                          └── close → Exit
```

三个状态，两种角色：

| 状态 | 拥有者 | 职责 |
|------|--------|------|
| `PingQuery` | client | 打时间戳并发送 ping |
| `PingResponse` | server | 回显所有字段 + 附加停留时间 |
| `PingDecision` | client | 计算 RTT，决定继续还是退出 |

## 时钟无关的 RTT 计算

每次往返三个字段，均不依赖跨机时钟同步：

| 字段 | 类型 | 谁填 | 用途 |
|------|------|------|------|
| `seq_num` | `u64` | client，服务端回显 | 排序，诊断追踪 |
| `client_send_time` | `u64` | client，服务端回显 | RTT = now - client_send_time（均取自客户端时钟） |
| `server_dwell_ns` | `u64` | server | 服务端处理耗时（同钟差值，短间隔内漂移可忽略） |

```
RTT_raw   = client_now - client_send_time
RTT_net   = RTT_raw - server_dwell_ns   // 剥离服务端延迟
```

`server_dwell_ns` 是相对测量值——服务端自身单调时钟上的 `响应时刻 - 到达时刻`。
亚毫秒级间隔内时钟漂移可忽略。没有绝对时间戳跨越机器边界。

`rtt_net` 将服务端时钟测量的时长从客户端时钟测量的时长中减去。这在数学上
不严格（两端时钟漂移速率不同），但误差上界为 `漂移比例 × 停留时间`。对于
典型的 1ms 以下停留时间和 100ppm 以下时钟漂移，误差小于 100ns——远低于
网络抖动。因此该减法是从 RTT 中剥离服务端处理延迟的有用近似。

## 负载设计

```zig
const PingPayload = struct {
    seq_num: u64,
    client_send_time: u64,
};

const PongPayload = struct {
    seq_num: u64,
    client_send_time: u64,
    server_dwell_ns: u64,
};
```

`PingDecision` 携带 `void`——决策变体（`ping_again` / `close`）不需要负载
数据。指标在 `preprocess()` 期间累积到 `ClientContext` 中。

## 上下文设计

### ClientContext

```zig
pub const WindowMetrics = struct {
    start_ns: u64,         // 本窗口内第一个 ping 的时刻（单调时钟）
    rtt_sum_ns: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ns: u64 = math.maxInt(u64),
    rtt_max_ns: u64 = 0,
};

pub const ClientContext = struct {
    /// 用于动态窗口列表的分配器（调用方在 symmetric_run() 前设置）
    allocator: std.mem.Allocator,

    /// 单调递增的 ping 序号
    seq_num: u64,

    /// 剩余周期数（调用方设置）
    remaining: u32,

    /// ping 之间的间隔（纳秒），调用方在 symmetric_run() 前设置
    interval_ns: u64,

    /// 窗口宽度（如 60_000_000_000 = 1 分钟）
    window_duration_ns: u64,

    /// 会话起始时刻（首次 PingQuery 时记录）。
    /// 0 表示尚未开始。单调时钟在系统启动时从接近 0 开始，但到
    /// 监控协议启动时已经过了足够时间，此哨兵值在实践中安全。
    session_start_ns: u64,

    /// 每窗口指标的动态列表，仅追加
    windows: std.ArrayList(WindowMetrics),
};
```

### ServerContext

```zig
pub const ServerContext = struct {
    /// 用于停留时间测量的单调时钟
    clock: std.time.Instant,
};
```

服务端在 ping 之间**完全无状态**——无会话表、无计数器、无历史。每次
`PingResponse.process()` 是一个纯函数：读取 ping，计算停留时间，回显。
这意味着任意数量的客户端可以共享同一个服务端，无需会话管理。

## 逐状态详述

### PingQuery (client)

`process()`：
1. 如果 `session_start_ns == 0`：记录当前单调 `u64` 时间为 `session_start_ns`。
2. 记录当前单调时间为 `client_send_time`。
3. 递增 `seq_num`。
4. 返回 `.to_server` 携带 `PingPayload`。

无 `preprocess()`——服务端状态，客户端此处无需接收任何内容。

### PingResponse (server)

`process()`：
1. 从 `clock` 读取到达时刻 `t_arrival`。
2. 构造响应（回显 `seq_num`、`client_send_time`）。
3. 记录离开时刻 `t_departure`，计算 `server_dwell_ns = t_departure - t_arrival`。
4. 返回 `.to_client` 携带 `PongPayload`。

`preprocess()` (client)：
1. 计算 `rtt_net = now() - client_send_time - server_dwell_ns`。
2. 确定窗口索引：`(now - session_start_ns) / window_duration_ns`。
   窗口归属以**响应到达时刻**为准——ping 在窗口边界附近发出、回复在
   下一窗口到达的，计入后一窗口。
3. 当 `windows.items.len <= index` 时，持续追加新的 `WindowMetrics`，
   其 `start_ns = session_start_ns + windows.items.len * window_duration_ns`。
   这填补了因索引跳跃（如一次极慢的响应跨越多个窗口边界）产生的空位。
4. 将 `rtt_net` 累积到该窗口的 `rtt_sum_ns`、`rtt_count`、`rtt_min_ns`、
   `rtt_max_ns`。

### PingDecision (client)

```zig
pub const PingDecision = union(enum) {
    ping_again: Data(void, PingQuery),
    close: Data(void, Exit),
};
```

`process()`：
1. 递减 `remaining`。
2. 如果 `remaining == 0`：返回 `.close`。
3. 休眠 `interval_ns`（阻塞——Runner 是同步的）。
4. 返回 `.ping_again`。

`remaining` 计包含首次 ping 在内的**总数**。首次 PingQuery 无条件发送
（无检查），此后每次 PingDecision 递减一次。设置 `remaining = 5` 正好
产生 5 次 ping。调用方必须设置 `remaining > 0`。

无 `preprocess()`——服务端不参与此状态。

## Channel

协议运行于任何实现 polyrole-cs send/recv 接口的 channel 之上：

```
Runner(net_monitor).symmetric_run(role, ctx, channel, PingQuery)
```

最小化配置——`StreamChannel`（原始 TCP）：

```
TCP stream
  └─ StreamChannel ─── Runner(net_monitor).symmetric_run
       PingQuery ⇄ PingResponse ⇄ PingDecision → Exit
```

可选叠加 TLS：

```
TCP stream
  ├─ StreamChannel ─── Runner(simple_tls).symmetric_run（握手）
  ├─ StreamChannel.deinit()
  └─ TlsChannel ─── Runner(net_monitor).symmetric_run
       PingQuery ⇄ PingResponse ⇄ PingDecision → Exit
```

协议不对底层传输做任何假设——加密是部署选择，而非协议关注点。

## 协议参数（调用方控制）

进入 `Runner(net_monitor).symmetric_run()` 前，调用方设置：

| 参数 | 字段 | 含义 |
|------|------|------|
| Ping 次数 | `ClientContext.remaining` | 总 ping 周期数 (> 0) |
| 间隔 | `ClientContext.interval_ns` | ping 之间纳秒数（如 `1_000_000_000` = 1 秒） |
| 窗口宽度 | `ClientContext.window_duration_ns` | 每窗口宽度（> 0，如 `60_000_000_000` = 1 分钟） |
| 窗口列表 | `ClientContext.windows` | 动态 `ArrayList(WindowMetrics)`，调用方在 `symmetric_run()` 前初始化，之后读取 |
| 分配器 | `ClientContext.allocator` | 用于 `windows` 列表扩容 |
| 时钟源 | `ServerContext.clock` | 使用哪个单调时钟 |

Runner 退出后，调用方读取 `ClientContext.windows.items` 获取每窗口聚合
结果，并 deinit 该 ArrayList。

**调用方设置示例：**

```zig
var ctx: ClientContext = .{
    .allocator = allocator,
    .remaining = 60,               // 总共 60 次 ping
    .interval_ns = 1_000_000_000,  // 间隔 1 秒
    .window_duration_ns = 60_000_000_000, // 1 分钟窗口
    .windows = std.ArrayList(WindowMetrics).init(allocator),
    .session_start_ns = 0,
    .seq_num = 0,
};
defer ctx.windows.deinit();
try Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &channel, net_monitor.PingQuery);

// ctx.windows.items 现在包含每分钟统计数据
for (ctx.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue; // 空窗口（该时段内无响应）
    std.debug.print("窗口 {d}: 平均={d}ns 最小={d} 最大={d} 次数={d}\n", .{
        i, w.rtt_sum_ns / w.rtt_count, w.rtt_min_ns, w.rtt_max_ns, w.rtt_count,
    });
}
```

### 每 N 分钟将窗口数据保存到文件

协议本身不感知持久化。周期性保存通过在同一 channel 上将长会话拆分为
连续的短运行来实现——只要连接保持健康，无需重建：

```zig
while (total_remaining > 0) {
    const chunk = @min(total_remaining, pings_per_10min);
    ctx.remaining = chunk;
    ctx.windows.clearRetainingCapacity();
    ctx.session_start_ns = 0;
    ctx.seq_num = 0;

    Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &channel, net_monitor.PingQuery) catch |err| {
        // 连接断开——重连重试，或向上传播
        return err;
    };

    try saveWindowFile("monitor_10min.json", ctx.windows.items);
    total_remaining -= chunk;
}
```

关键不变量：
- Channel 在 Runner 多次调用间保持打开——重入开销很小，只要连接存活。
- 每块重置 `session_start_ns` 和 `windows`，因此文件内容恰好覆盖一个时间片。
- 调用方控制文件命名和格式；协议层专注于测量。

## 错误处理

协议不定义任何错误状态。网络或 channel 故障通过 Runner 的 `try` 机制
自然传播——调用方捕获后决定重连、重试或中止。协议本身只建模正确流程的
状态机。

## 设计决策汇总

| 决策 | 理由 |
|------|------|
| 无状态服务端 | 可扩展至任意数量并发客户端，无需会话 GC |
| 时钟无关计时 | RTT 仅用客户端时钟；停留时间用服务端本地相对时间 |
| 通过显式变体实现循环 | 框架约束——每个可能的下一个状态必须是一个单独的 union 变体 |
| 服务端不持有指标 | 服务端不关心 RTT；由调用方在客户端聚合 |
| 客户端 `remaining` 计数器 | 简单、显式的终止条件；服务端是被动的 |
| Decision 携带 void | 决策不携带数据——转移本身就是信号 |
| 协议管理间隔 | ping 频率由上下文中的 `interval_ns` 控制；调用方只需设置一个字段，Runner 处理时序 |
| process() 中同步 sleep | Runner 是单线程的——间隔期间的阻塞 `std.time.sleep` 是预期行为，不是浪费 |
| 窗口化聚合 | 通过 `ArrayList` 实现每窗口 `WindowMetrics`——调用方初始化，协议追加，调用方读取。无长度限制 |
| ArrayList 而非预分配切片 | 动态增长避免了调用方需要提前猜测需要多少窗口 |
| 传输无关 | 协议仅需 send/recv 接口——可工作于 StreamChannel、TlsChannel 或任何自定义传输。加密是部署选择 |
| `remaining > 0` 在调用点强制 | 协议假定至少一次 ping；零次 ping 会话对监控无意义 |
| 错误处理是调用方职责 | 协议只建模正确流程的状态机。故障通过 Runner 传播——调用方决定重试/重连/中止 |
| 填空式窗口追加 | `while windows.items.len <= index` 而非单次 `append`——大 RTT 可能一次跳跃多个窗口，必须填充中间的所有空位 |
