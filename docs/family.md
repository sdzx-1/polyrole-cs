# 协议族 (Protocol Family) 设计

> **注意：** 本文档由 AI 初始设计，存在诸多不合理之处，仅供参考，不作为最终实现依据。

## 1. 概述

单协议模型每连接跑一个协议，浪费端口资源。协议族让多个协议共享一个 TCP 连接，消息按协议 ID 多路复用。

对标单协议的两层架构：

```
单协议:                          协议族:

runner.zig                        family_runner.zig
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

采用对称 MVar 架构。每端 TCP 连接有两个独立 fiber（Reader / Writer）+ 每协议两个有界队列（rb / wb）：

```
                     MultiplexChannel(N)
                     ═══════════════════

  Reader Fiber [R]                     Writer Fiber [W]
  ┌──────────────────┐                 ┌──────────────────┐
  │ loop:            │                 │ loop:            │
  │  TCP → readFrame │                 │  for each sub:   │
  │  id = takeByte() │                 │   tryReceive(wb) │
  │  len = takeInt() │                 │   → writeFrame   │
  │  data = take(len)│                 │   → flush TCP    │
  │  alloc.dupe      │                 │   sleep(0)       │
  │  → rb.send()     │                 │                  │
  └──┬──┬──┬─────────┘                 └──┬──┬──┬─────────┘
     │  │  │                              │  │  │
     ▼  ▼  ▼                              ▲  ▲  ▲
  ┌──────────────┐                   ┌──────────────┐
  │ SubChannel[0]│                   │ SubChannel[0]│
  │ rb: Channel◇ │──recv()→decode    │ wb: Channel◇ │←send()←encode
  └──────────────┘                   └──────────────┘
  ┌──────────────┐                   ┌──────────────┐
  │ SubChannel[1]│                   │ SubChannel[1]│
  │ rb: Channel◇ │──recv()→decode    │ wb: Channel◇ │←send()←encode
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
1. 协议 fiber：`SubChannel.send()` → `codec.encode` → `allocator.dupe` → `wb.send(copy)`
2. Writer Fiber：`wb.tryReceive()` 非阻塞取 → 组帧 `[id][len][data]` → 写 TCP → `flush`
3. 全队列空时 `sleep(0)` 让出调度器

### 3.2 架构决策

| 决策 | 理由 |
|------|------|
| **Reader Fiber 独立** | 单一入口从 TCP 读，解析 `protocol_id` 后路由到正确队列。协议代码不接触 TCP |
| **Writer Fiber 独立** | 与 Reader 对称。收集所有协议的待发送数据，组帧后串行写 TCP。替代 `write_lock` |
| **每协议独立 rb / wb** | `rb` 接收队列和 `wb` 发送队列各自独立，避免协议间相互阻塞 |
| **`zio.Channel` MVar 语义** | 满则阻塞生产者，空则阻塞消费者。天然提供背压和流量控制 |
| **Writer 轮询 `tryReceive`** | 避免多 channel select 的复杂度。空队列时 `sleep(0)` 让出 CPU |

### 3.3 内存模型

每条消息在堆上短暂存在，所有权通过队列传递：

```
发送路径:
  Protocol Fiber:    encode → alloc.dupe() → wb.send()
  Writer Fiber:      wb.tryReceive() → write TCP → allocator.free()

接收路径:
  Reader Fiber:      read TCP → alloc.dupe() → rb.send()
  Protocol Fiber:    rb.receive() → codec.decode → allocator.free()
```

### 3.4 SubChannel 接口

与 `StreamChannel` 完全一致，`symmetric_run` 零改动：

```zig
pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
    // encode → alloc.dupe → wb.send(copy)
}

pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
    // rb.receive() → Io.Reader.fixed → codec.decode → free
}
```

### 3.5 生命周期

**init**: 分配读写 buffer，初始化所有 SubChannel 的 `rb` / `wb`，spawn Reader Fiber + Writer Fiber。

**deinit**（顺序严格）:

```
1. close all wb     → Writer 检测到 ChannelClosed → 退出循环
2. close all rb     → 协议 fiber recv() 返回 ChannelClosed → 协议退出
3. shutdown(.receive) → Reader 的 takeByte() 返回 EOF → 退出循环
4. join Reader + join Writer
5. stream.close()
6. free(rbuff), free(wbuff)
```

关键：第 1 步关闭 wb 让 Writer 先退出，第 2 步关闭 rb 通知所有协议退出，第 3 步发 EOF 唤醒 Reader。

## 4. FamilyRunner —— 调度层

```zig
pub fn FamilyRunner(comptime states: anytype) type {
    // states = .{ ProtoA.State, ProtoB.State, ... }
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
try Fr.start(&mux, 0, &cli_ctx_a, null);   // 运行协议 0
try Fr.start(&mux, 1, &cli_ctx_b, 5000);   // 运行协议 1，5s 超时
```

### 4.3 并发模型

`start()` 在主调 fiber 中阻塞。并发跑多个用 `zio.Group.spawn`：

```zig
var g: zio.Group = .init;
defer g.cancel();
try g.spawn(Fr.start, .{&mux, 0, &ctx_a, null});
try g.spawn(Fr.start, .{&mux, 1, &ctx_b, null});
```

## 5. 与 Runner 的对比

| | Runner(State) | FamilyRunner(.{A, B, C}) |
|---|---|---|
| 驱动对象 | 单个状态机 | 多个状态机 |
| Channel 类型 | `StreamChannel` | `MultiplexChannel.subChannel(id)` |
| 并发模型 | 同步循环 | zio fiber |
| 读写方式 | 独占 TCP，直接 read/write | 共享 TCP，Reader/Writer Fiber + rb/wb 队列 |
| 退出 | Exit 状态 → 返回 | 协议 Exit → handler fiber 退出 |

## 6. API 示例

```zig
const Mux = MultiplexChannel(2);
const Fr = FamilyRunner(.{ ProtocolA.State, ProtocolB.State });

// ── Server ──
var mux: Mux = undefined;
try mux.init(allocator, tcp_stream, 256, 256);
defer mux.deinit();
try Fr.initServer(&mux, .{ &srv_ctx_a, &srv_ctx_b }, null);

// ── Client ──
try Fr.start(&mux, 0, &cli_ctx_a, 5000);
```

## 7. 文件结构

```
src/
├── family_mux_channel.zig    ← 传输层：MultiplexChannel + Reader/Writer Fiber
├── family_runner.zig         ← 调度层：FamilyRunner + 测试
├── root.zig                  ← pub const 导出
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```
