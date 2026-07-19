# 协议族 (Protocol Family) 设计

## 1. 问题

单协议模型：一个 TCP 连接跑一个协议（`StreamChannel` + `Runner(State)`）。

```
TCP → StreamChannel → Runner(PingQuery).symmetric_run(...)
```

跑多个协议时，最简单的办法是每个协议独占一个 TCP 连接：

```
TCP₁ → StreamChannel₁ → Runner(A).symmetric_run(...)
TCP₂ → StreamChannel₂ → Runner(B).symmetric_run(...)
```

**问题**：浪费端口资源。客户端需要 N 个连接，服务端需要 N 倍的 accept/fd。

## 2. 方案

多个协议**共享一个 TCP 连接**，消息按协议 ID 多路复用。对标单协议的两层架构：

```
单协议:                          协议族:

runner.zig                        family_runner.zig
  Runner(State)                     FamilyRunner(.{A, B, C})
  驱动单个状态机                    管理多协议并发生命周期
  ↑ 不关心底层传输                   ↑ 不关心底层传输
  ↓                                 ↓
channel.zig                        family_mux_channel.zig
  StreamChannel                      MultiplexChannel(N)
  send/recv                          SubChannel.send/recv
```

`FamilyRunner` 只管协议调度（spawn fiber、管理 Exit）；`MultiplexChannel` 只管传输（TCP 共享、消息路由）。

## 3. Wire 格式

```
┌──────────────┬──────────────────┬─────────────────────────┐
│ protocol_id  │  payload_len     │  payload                │
│    u8        │  u16 BE          │  payload_len bytes      │
└──────────────┴──────────────────┴─────────────────────────┘
```

`payload` 是现有 `codec.encode()` 的输出（`state_id + tag + data`），完全不动。

发送时由 `MultiplexChannel` 自动前缀 `protocol_id` 和 `payload_len`，协议代码无感知。

## 4. MultiplexChannel（传输层）

```
                      ┌────────────────────────────────────────┐
                      │          MultiplexChannel(N)           │
                      │                                        │
 TCP ◀──── read ──── │  Reader Fiber (独立 zio fiber)          │
                      │  循环: read id → read len → read data  │
                      │  → alloc.dupe → sub[id].recv_ch.send   │
                      │                                        │
 TCP ──── write ──── │  SubChannel.send() 直接写 TCP           │
                      │  + zio.Mutex 串行化                     │
                      │                                        │
                      │  SubChannel[0] ── symmetric_run(A)     │
                      │  SubChannel[1] ── symmetric_run(B)     │
                      └────────────────────────────────────────┘
```

### 设计决策

| 决策 | 理由 |
|------|------|
| 无 Writer Fiber | send() 直接写 TCP + 锁。省掉一个 fiber，避免多 channel select 复杂度 |
| Reader Fiber 存在 | 必须有一个地方解析 protocol_id 并路由到正确队列 |
| `zio.Channel([]const u8)` | 内建阻塞/唤醒 + cancel 支持 |
| stack 编码 buffer | send 先编码到栈上 buf，拿到 len，再写 wire header + data |

### 内存模型

```
Reader Fiber:    alloc.dupe → channel.send(slice)
Protocol Fiber:  channel.receive() → codec.decode → allocator.free
```

每个消息在堆上短暂存在一次——reader 分配，protocol 释放。

### SubChannel 接口

与 `StreamChannel` 完全一致：

```zig
pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void
pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T
```

`symmetric_run` 零改动。

### 生命周期

- `init(allocator, stream)` — 创建 SubChannel 数组 + spawn Reader Fiber
- `subChannel(id)` — 返回 `*SubChannel`
- 协议 Exit → 对应 SubChannel 标记 closed → Reader 跳过该 ID 的路由
- 全部 SubChannel closed → Reader Fiber 退出 → `deinit()` 回收

## 5. FamilyRunner（调度层）

```zig
pub fn FamilyRunner(comptime states: anytype) type {
    // states = .{ ProtocolA.State, ProtocolB.State, ... }
    return struct {
        pub fn run(
            allocator: std.mem.Allocator,
            comptime role: Role,
            contexts: anytype,
            mux: *MultiplexChannel(states.len),
            recv_timeout_ms: ?u64,
        ) !void { ... }
    };
}
```

**职责**：
- 为每个子协议创建 fiber（通过 `zio.Group.spawn`）
- 传递 `SubChannel` 给各协议的 `symmetric_run`
- 某协议 Exit → 关闭对应 SubChannel
- 全部 Exit → `run()` 返回

**不管的事**：TCP、wire 格式、消息路由——这些全是 `MultiplexChannel` 的。

## 6. 与单协议 Runner 的对比

| | Runner(State) | FamilyRunner(.{A, B, C}) |
|---|---|---|
| 驱动对象 | 单个状态机 | 多个状态机（并发） |
| Channel 类型 | `StreamChannel` / `TlsChannel` | `MultiplexChannel.subChannel(id)` |
| 并发模型 | 同步循环 | zio fiber 并发 |
| send/recv | 独占 TCP | 共享 TCP（SubChannel 代理） |
| 退出 | Exit 状态 → 返回 | 所有协议 Exit → 返回 |

## 7. API 示例

```zig
const Mux = MultiplexChannel(2);
var mux = try Mux.init(allocator, tcp_stream);
defer mux.deinit();

const Fr = FamilyRunner(.{ ProtocolA.State, ProtocolB.State });

// 服务端
try Fr.run(.server, .{ &ctx_a, &ctx_b }, &mux, null);

// 客户端
try Fr.run(.client, .{ &ctx_a, &ctx_b }, &mux, null);
```

内部自动为 ProtocolA 和 ProtocolB 各 spawn 一个 fiber，共享同一 TCP。

## 8. 文件结构

```
src/
├── family_mux_channel.zig    ← 传输层：MultiplexChannel(N)
├── family_runner.zig         ← 调度层：FamilyRunner(states) + 测试
├── root.zig                  ← pub const + comptime test import
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```

## 9. 测试

在 `family_runner.zig` 中，两个 `CreateTestProtocol` 在一个 `MultiplexChannel(2)` 上并发运行，验证消息不串扰。
