# 协议族 (Protocol Family) 设计

## 1. 概述

单协议模型每连接跑一个协议，浪费端口资源。协议族让多个协议共享一个 TCP 连接，消息按协议 ID 多路复用。

两层架构：

```
runner.zig                          family_test.zig
  Runner(State)                       zio.spawn + symmetric_run
  驱动单个状态机                      多协议并发
  ↑ 不关心底层传输                     ↑ 不关心底层传输
  ↓                                    ↓
channel.zig                           family_mux_channel.zig
  StreamChannel                        MultiplexChannel(N)
```

## 2. Wire 格式

每条消息前缀协议 ID 和长度：

```
┌──────────────┬──────────────────┬─────────────────────────┐
│ protocol_id  │  payload_len     │  payload                │
│    u8        │  u16 BE          │  payload_len bytes      │
└──────────────┴──────────────────┴─────────────────────────┘
```

`payload` 是 `codec.encode()` 的输出，完全不动。

## 3. MultiplexChannel —— 传输层

单 Reader Fiber 架构。每端 TCP 连接有一个独立 fiber（Reader）从 TCP 读取数据并分发；写路径无独立 fiber——各协议 fiber 直接写 TCP，通过 Mutex 串行化：

```
                     MultiplexChannel(N)
                     ═══════════════════

  Reader Fiber [R]
  ┌──────────────────┐
  │ loop:            │
  │  id = takeByte() │
  │  len = takeInt() │
  │  data = take(len)│    写路径（无独立 fiber）：
  │  alloc.dupe      │    SubChannel.send()
  │  → rb.trySend()  │      write_mu.lock()
  └──┬──┬──┬─────────┘      write protocol_id/len/data
     │  │  │                 write_mu.unlock()
     ▼  ▼  ▼                 flush()
  ┌──────────────┐
  │ SubChannel[0]│   ← recv() = rb.receive() → codec.decode
  │ rb: Channel◇ │
  └──────────────┘
  ┌──────────────┐
  │ SubChannel[1]│
  │ rb: Channel◇ │
  └──────────────┘
```

◇ = `zio.Channel([]const u8)`，有界阻塞队列（MVar 语义）

### 3.1 数据流

**上行（读）：**
1. Reader Fiber 从 TCP 读帧：`[id][len][data]`
2. `allocator.dupe(data)` → 堆上复制
3. `sub_channels[id].rb.trySend(copy)` → 推入协议读队列（满则关闭该协议 rb）
4. 协议 fiber：`SubChannel.recv()` → `rb.receive()` 阻塞取 → `codec.decode`

**下行（写）：**
1. 协议 fiber：`SubChannel.send()` → `codec.encode` → `write_mu.lock()` → 写 `[id][len][data]` 到 TCP → `write_mu.unlock()` → `flush()`
2. 无独立 Writer Fiber——直接写 TCP，zio 调度器在 fiber 间自然交错执行

### 3.2 架构决策

| 决策 | 理由 |
|------|------|
| **Reader Fiber 独立** | 单一入口从 TCP 读，解析 `protocol_id` 后路由到正确队列。协议代码不接触 TCP |
| **无 Writer Fiber** | 所有协议 fiber 直接写 TCP + Mutex 串行化。公平性由 zio 调度器保证——每个协议短暂持锁写完后释放，调度器自动切换到下一个有数据的 fiber。比独立 Writer + 共享队列更简单，延迟更低 |
| **`zio.Channel` MVar 语义** | 满则阻塞生产者，空则阻塞消费者。天然提供背压和流量控制 |
| **Reader 用 `trySend`** | 某协议 `rb` 满时不阻塞 Reader——满则关闭该协议，其他协议不受影响 |
| **每协议独立 `rb`** | 接收队列按协议隔离，Reader 按 `protocol_id` 分发，互不干扰 |

### 3.3 内存模型

```
发送路径:
  Protocol Fiber:    encode → write_buf → write TCP → 复用 write_buf

接收路径:
  Reader Fiber:      read TCP → alloc.dupe() → rb.trySend()
  Protocol Fiber:    rb.receive() → codec.decode → allocator.free()（下次 recv 时）
```

接收路径消息在堆上短暂存在，所有权通过 `rb` 队列传递。`last_recv_data` 管理解码引用的生命周期——下次 `recv()` 时释放上一帧。

### 3.4 SubChannel 接口

与 `StreamChannel` 完全一致，`symmetric_run` 零改动：

```zig
pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
    var w = Io.Writer.fixed(self.send_buf);
    try codec.encode(&w, state_id, val);
    self.mux.write_mu.lockUncancelable();
    defer self.mux.write_mu.unlock();
    try self.mux.writer.writeByte(self.protocol_id);
    try self.mux.writer.writeInt(u16, @intCast(w.end), .big);
    try self.mux.writer.writeAll(self.send_buf[0..w.end]);
    try self.mux.writer.flush();
}

pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
    if (self.last_recv_data) |old| self.mux.allocator.free(old);
    const data = self.rb.receive() catch |err| {
        self.last_recv_data = null;
        return err;
    };
    self.last_recv_data = data;
    var r = Io.Reader.fixed(data);
    return codec.decode(&r, state_id, T);
}
```

### 3.5 生命周期

**init**（`initFromChannel`）：分配每个 SubChannel 的 `send_buf`，初始化所有 `rb` channel，spawn Reader Fiber。

**deinit**（顺序严格）：

```
1. close all rb           → 协议 fiber recv() 返回 ChannelClosed → 协议退出
2. free send_buf, last_recv_data
3. shutdown(.receive)     → Reader 的 takeByte() 返回 EOF → 退出循环
4. join Reader
5. stream.close()
```

### 3.6 公平性

写公平性由 zio 调度器保证——`write_mu` 短暂持有，持锁时间仅为一个帧的序列化 + writeAll，释放后调度器切换到下一个 ready fiber。

读公平性由 Reader Fiber 的逐帧分派保证——每读完一帧就分派到对应 `rb`，不累积。`trySend` 防止某协议 `rb` 满时阻塞 Reader 导致其他协议饿死。

详见下方测试示例。

## 4. 并发模型

没有 FamilyRunner。多协议并发直接用 `zio.spawn` + `symmetric_run`：

```zig
// Server 端：每个协议 spawn 独立 fiber
var sh1 = try zio.spawn(struct {
    fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
        try R1.symmetric_run(.server, ctx, ch, P1.A, null);
    }
}.run, .{m.subChannel(0), &srv_ctx1});
var sh2 = try zio.spawn(struct {
    fn run(ch: *M.SubChannel, ctx: *i32) anyerror!void {
        try R2.symmetric_run(.server, ctx, ch, P2.A, null);
    }
}.run, .{m.subChannel(1), &srv_ctx2});

// Client 端（对称）
var h1 = try zio.spawn(R1.symmetric_run, .{.client, &c1, m.subChannel(0), P1.A, null});
var h2 = try zio.spawn(R2.symmetric_run, .{.client, &c2, m.subChannel(1), P2.A, null});
h1.join() catch {};
h2.join() catch {};
```

`recv` 超时通过 `symmetric_run` 的最后一个参数指定（毫秒）：

```zig
// 100ms 超时，超时返回 error.Canceled
R2.symmetric_run(.server, &ctx, m.subChannel(1), P2.A, 100);
```

## 5. 与单协议的对比

| | Runner(State) + StreamChannel | MultiplexChannel + symmetric_run |
|---|---|---|
| 驱动对象 | 单个状态机 | 多个状态机 |
| Channel 类型 | `StreamChannel` | `MultiplexChannel.subChannel(id)` |
| 并发模型 | 同步循环 | zio fiber |
| 读写方式 | 独占 TCP | 共享 TCP，Reader Fiber + write_mu |
| 退出 | Exit 状态 → 返回 | 协议 Exit → fiber 结束 |

## 6. 测试

三个测试验证 Mux 行为（详见 `src/family_test.zig`）：

1. **单协议握手** — 1 个 SubChannel，明文，验证 ctx 从 0 递增到 3 后 Exit
2. **recv 超时** — 协议 1 正常完成，协议 2 等 100ms 后超时返回 `error.Canceled`
3. **双协议并发** — 2 个 SubChannel 各自独立完成 3 轮交互，验证 ctx 值互不干扰

```zig
const M = Mux(2, 1024, 8);
// 双协议示例见第 4 节
```

## 7. 文件结构

```
src/
├── family_mux_channel.zig    ← 传输层：MultiplexChannel + Reader Fiber + write_mu
├── family_test.zig           ← 测试
├── root.zig                  ← pub const 导出
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```
