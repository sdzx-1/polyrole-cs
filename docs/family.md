# 协议族 (Protocol Family) 设计

> **注意：** 本文档由 AI 初始设计，存在诸多不合理之处，仅供参考，不作为最终实现依据。

## 1. 概述

单协议模型每连接跑一个协议，浪费端口资源。协议族让多个协议共享一个 TCP 连接，消息按协议 ID 多路复用。

对标单协议的两层架构：

```
单协议:                          协议族:

runner.zig                        family_test.zig
  Runner(State)                     FamilyRunner(.{A, B, C})
  驱动单个状态机                    管理多协议并发
  ↑ 不关心底层传输                   ↑ 不关心底层传输
  ↓                                 ↓
channel.zig                        family_mux_channel.zig
  StreamChannel                      MultiplexChannel(N)
```

`FamilyRunner` 管协议调度，`MultiplexChannel` 管传输。

## 2. Wire 格式

每条消息前缀协议 ID 和长度：

```
┌──────────────┬──────────────────┬─────────────────────────┐
│ protocol_id  │  payload_len     │  payload                │
│    u8        │  u16 BE          │  payload_len bytes      │
└──────────────┴──────────────────┴─────────────────────────┘
```

`payload` 是现有 `codec.encode()` 的输出，完全不动。

## 3. MultiplexChannel —— 传输层

采用对称 Fiber 架构。每端 TCP 连接有两个独立 fiber（Reader / Writer），通过队列与协议通信：

```
                     MultiplexChannel(N)
                     ═══════════════════

  Reader Fiber [R]                     Writer Fiber [W]
  ┌──────────────────┐                 ┌──────────────────┐
  │ loop:            │                 │ loop:            │
  │  TCP → readFrame │                 │  write_ch        │
  │  id = takeByte() │                 │  .receive()      │
  │  len = takeInt() │                 │  → writeFrame    │
  │  data = take(len)│                 │  → flush TCP     │
  │  alloc.dupe      │                 │  → alloc.free    │
  │  → rb.send()     │                 └────────▲─────────┘
  └──┬──┬──┬─────────┘                          │
     │  │  │                         write_ch (共享, 带 protocol_id)
     ▼  ▼  ▼                                    │
  ┌──────────────┐                   ┌──────────┴───┐
  │ SubChannel[0]│                   │ SubChannel[0]│
  │ rb: Channel◇ │──recv()→decode    │ send()→encode│
  └──────────────┘                   └──────────────┘
  ┌──────────────┐                   ┌──────────────┐
  │ SubChannel[1]│                   │ SubChannel[1]│
  │ rb: Channel◇ │──recv()→decode    │ send()→encode│
  └──────────────┘                   └──────────────┘
```

◇ = `zio.Channel([]const u8)`，有界阻塞队列（MVar 语义）

### 3.1 数据流

**上行（读）：**
1. Reader Fiber 从 TCP 读帧：`[id][len][data]`
2. `allocator.dupe(data)` → 堆上复制
3. `sub_channels[id].rb.send(copy)` → 推入协议读队列
4. 协议 fiber：`SubChannel.recv()` → `rb.receive()` 阻塞取 → `codec.decode` → `allocator.free`

**下行（写）：**
1. 协议 fiber：`SubChannel.send()` → `codec.encode` → `alloc.dupe` → `write_ch.send(WriteMsg{protocol_id, data})`
2. Writer Fiber：`write_ch.receive()` 阻塞取 → 组帧 `[id][len][data]` → 写 TCP → `flush` → `allocator.free`
3. `write_ch` 满时 `send()` 阻塞→背压

### 3.2 架构决策

| 决策 | 理由 |
|------|------|
| **Reader Fiber 独立** | 单一入口从 TCP 读，解析 `protocol_id` 后路由到正确队列。协议代码不接触 TCP |
| **Writer Fiber 独立** | 与 Reader 对称。阻塞 `receive()` 等待，组帧后串行写 TCP。替代 `write_lock` |
| **共享写队列 `write_ch`** | 所有协议发送到同一个队列，带 `protocol_id`。Writer 只需阻塞一个 channel，无需 select/poll |
| **`zio.Channel` MVar 语义** | 满则阻塞生产者，空则阻塞消费者。天然提供背压和流量控制 |
| **每协议独立 `rb`** | 接收队列按协议隔离，Reader 按 `protocol_id` 分发，互不干扰 |

### 3.3 内存模型

每条消息在堆上短暂存在，所有权通过队列传递：

```
发送路径:
  Protocol Fiber:    encode → alloc.dupe() → write_ch.send()
  Writer Fiber:      write_ch.receive() → write TCP → allocator.free()

接收路径:
  Reader Fiber:      read TCP → alloc.dupe() → rb.send()
  Protocol Fiber:    rb.receive() → codec.decode → allocator.free()
```

### 3.4 SubChannel 接口

与 `StreamChannel` 完全一致，`symmetric_run` 零改动：

```zig
pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
    // encode → alloc.dupe → write_ch.send(WriteMsg{id, data})
}

pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
    // rb.receive() → Io.Reader.fixed → codec.decode → free
}
```

### 3.5 生命周期

**init**: 分配读写 buffer，初始化 `write_ch` 和所有 SubChannel 的 `rb`，spawn Reader + Writer。

**deinit**（顺序严格）:

```
1. close write_ch   → Writer 的 receive() 返回 ChannelClosed → 退出循环
2. join Writer
3. close all rb     → 协议 fiber recv() 返回 ChannelClosed → 协议退出
4. shutdown(.receive) → Reader 的 takeByte() 返回 EOF → 退出循环
5. join Reader
6. stream.close()
7. free(rbuff), free(wbuff)
```

## 4. FamilyRunner —— 调度层

```zig
pub fn FamilyRunner(comptime states: anytype) type {
    return struct {
        pub fn initServer(mux, contexts, recv_timeout_ms) !void { ... }
        pub fn start(mux, comptime id, ctx, recv_timeout_ms) !void { ... }
    };
}
```

### 4.1 Server 端：initServer

预启动所有协议的服务端 handler。每个 handler 在独立 fiber 中运行 `symmetric_run(.server, ...)`，第一步阻塞在 `rb.receive()` 等待客户端首帧。

```zig
try Fr.initServer(&mux, .{ &srv_ctx_a, &srv_ctx_b }, null);
// 立即返回——所有 handler fiber 已 spawn
```

### 4.2 Client 端：start

阻塞调用——在主调 fiber 中运行 `symmetric_run(.client, ...)`，直到协议完成或出错。

```zig
try Fr.start(&mux, 0, &cli_ctx, null);
```

### 4.3 并发模型

`start()` 阻塞当前 fiber。并发跑多个协议用 `zio.spawn` + `join`：

```zig
var h1 = try zio.spawn(struct{fn run(m: *Mux, c: *i32)!void{
    try Fr.start(m, 0, c, null);
}}.run, .{&mux, &ctx_a});
var h2 = try zio.spawn(struct{fn run(m: *Mux, c: *i32)!void{
    try Fr.start(m, 1, c, null);
}}.run, .{&mux, &ctx_b});
h1.join() catch {};
h2.join() catch {};
```

## 5. 与 Runner 的对比

| | Runner(State) | FamilyRunner(.{A, B, C}) |
|---|---|---|
| 驱动对象 | 单个状态机 | 多个状态机 |
| Channel 类型 | `StreamChannel` | `MultiplexChannel.subChannel(id)` |
| 并发模型 | 同步循环 | zio fiber |
| 读写方式 | 独占 TCP，直接 read/write | 共享 TCP，Reader/Writer Fiber + rb/write_ch 队列 |
| 退出 | Exit 状态 → 返回 | 协议 Exit → handler fiber 退出 |

## 6. 测试

两个 `TestProtocol` 在一个 `MultiplexChannel(2)` 上并发运行，验证消息不串扰：

```zig
test "family: two protocols concurrent" {
    // P1 和 P2 各跑 3 轮（ctx: 0→3→Exit）
    // client: zio.spawn 两个 fiber，join 等完成
    // server: initServer 预启动两个 handler
    // 验证: srv_ctx1 == 3, srv_ctx2 == 3
}
```

## 7. 文件结构

```
src/
├── family_mux_channel.zig    ← 传输层：MultiplexChannel + Reader/Writer Fiber
├── family_test.zig         ← 测试
├── root.zig                  ← pub const 导出
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```
