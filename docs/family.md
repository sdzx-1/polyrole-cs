# 协议族 (Protocol Family) 设计

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

```
                      ┌──────────────────────────────────────┐
                      │       MultiplexChannel(N)             │
                      │                                       │
 TCP ◀── read ────── │  Reader Fiber (独立 zio fiber)         │
                      │  循环: takeByte → takeInt(len)        │
                      │  → take(data) → alloc → recv_ch.send  │
                      │                                       │
 TCP ── write ────── │  SubChannel.send() 直接写 + write_lock │
                      │                                       │
                      │  SubChannel[0].recv() ← recv_ch       │
                      │  SubChannel[1].recv() ← recv_ch       │
                      └──────────────────────────────────────┘
```

### 3.1 架构决策

| 决策 | 理由 |
|------|------|
| **Reader fiber 独立** | 单一入口从 TCP 读，解析 `protocol_id` 后路由到正确队列。协议代码不接触 TCP |
| **send 直接写 + 锁** | 不需要 writer fiber。每个 `SubChannel.send()` 框架 `[id][len][data]` 后直接写 TCP，`write_lock` 串行化 |
| **recv 从 `zio.Channel` 阻塞读** | `zio.Channel([]const u8)` 内建阻塞/唤醒。reader fiber 写，协议 fiber 读 |
| **无界路由和缓冲** | 不再需要 `SubChannel.recv()` 之间的消息转发——所有消息由 reader fiber 统一分发 |

### 3.2 内存模型

Reader fiber 从 TCP 读到数据后，`allocator.dupe()` 复制到堆上，通过 `recv_ch.send()` 将所有权转移给协议 fiber。协议 fiber 在 `recv()` 中 `receive()` 后 `free`。

```
Reader Fiber:       alloc.dupe() → channel.send(slice)
Protocol Fiber:     channel.receive() → codec.decode → allocator.free()
```

每条消息在堆上短暂存在一次——reader 分配，protocol 释放。

### 3.3 生命周期

**init**: 分配读写 buffer，初始化所有 SubChannel 的 `recv_ch`，spawn reader fiber。

**deinit**（顺序严格）:
1. `shutdown(.receive)` — 发 EOF 信号唤醒 reader fiber 的 `takeByte()`
2. `reader_handle.join()` — 等待 reader fiber 退出
3. 关闭所有 `recv_ch`，通知等待中的协议 fiber
4. `stream.close()` — 关闭 TCP
5. `free(rbuff)`, `free(wbuff)`

第 1 步是关键——`takeByte()` 阻塞在 TCP 读上，`shutdown(.receive)` 让读端收到 EOF 后返回错误，reader fiber 退出循环。

### 3.4 SubChannel 接口

与 `StreamChannel` 签名完全一致，`symmetric_run` 零改动：

```zig
pub fn send(self, state_id: anytype, _: type, val: anytype) !void
pub fn recv(self, state_id: anytype, T: type) !T
```

`send` 内部：`codec.encode` 编码消息 → 前缀 `[protocol_id:u8][len:u16]` → `write_lock` 锁 → 写 TCP → `flush` → 解锁。

`recv` 内部：`recv_ch.receive()` 阻塞取数据 → `Io.Reader.fixed` → `codec.decode` → `allocator.free`。

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

预启动所有协议的服务端 handler。每个 handler 在独立 fiber 中运行 `symmetric_run(.server, ...)`。Handler 的第一步是 `recv()` —— 阻塞在 `recv_ch.receive()` 上，等待客户端发来首帧。

```
initServer(&mux, .{&ctx_a, &ctx_b}, timeout)
  ├─ spawn: symmetric_run(.server, &ctx_a, subChannel[0])
  └─ spawn: symmetric_run(.server, &ctx_b, subChannel[1])
```

所有 handler 立即返回（非阻塞），调用方可以继续做其他事情。

### 4.2 Client 端：start

阻塞调用——在主调 fiber 中运行 `symmetric_run(.client, ctx, subChannel[id], ...)`，直到协议完成或出错。

```zig
try Fr.start(&mux, 0, &client_ctx, null);   // 运行协议 0
try Fr.start(&mux, 1, &client_ctx2, 5000);   // 运行协议 1，5s 超时
```

### 4.3 并发模型

`start()` 在主调 fiber 中阻塞执行。要同时跑多个客户端协议，用 `zio.Group.spawn`：

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
| send/recv | 独占 TCP | 共享 TCP，reader fiber 路由 |
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
├── family_mux_channel.zig    ← 传输层：MultiplexChannel + reader fiber
├── family_runner.zig         ← 调度层：FamilyRunner + 测试
├── root.zig                  ← pub const 导出
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```
