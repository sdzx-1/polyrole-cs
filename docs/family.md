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

Mux 把多个协议的消息合并进一个**多段帧**；传输层（StreamChannel 前缀 u16 长度，TlsChannel 作为一条 AEAD 记录）负责帧定界：

```
┌──────────────────────────────────────────────────────────────┐
│ [seg_count u8]                                               │
│ [段 1: protocol_id u8][payload_len u16 BE][payload ...]      │
│ [段 2: ...]                                                  │
└──────────────────────────────────────────────────────────────┘
```

每个段是一条**完整**协议消息，`payload` 是 `codec.encode()` 的输出，Mux 不解析。消息按段边界切分，不做跨帧重组；帧的大小由 `frame_budget` 限制。

## 3. MultiplexChannel —— 传输层

双 Fiber 架构。每端连接有**一个 Reader fiber**（读帧、按段分发给各协议 rb）和**一个 Writer fiber**（从各协议 wb 收集消息、合并成一个帧写出）：

**协议族成员可配置**。每个子通道独立声明队列深度、消息上限和溢出策略：

```zig
pub const SubChannelConfig = struct {
    capacity: u8 = 1,                        // 接收侧固定槽位数
    max_message_size: usize = 1024,
    overflow: OverflowPolicy = .close_channel,
};

const M = MultiplexChannel(&.{
    .{ .max_message_size = 4096 },                                   // 锁步控制协议：容量 1
    .{ .capacity = 16, .overflow = .backpressure },                   // 推送协议：窗口 16 + 背压
}, 4096);                                                            // 帧预算
```

**wb/rb 是每协议固定大小的槽位**：发送侧 `wb` 单槽（在途消息恒为 1），接收侧 `rb` 按 `capacity` 配置（锁步取 1，推送取发送方在途窗口 W）。所有权通过通道交接，不做每消息堆分配。

**传输层可替换**。Mux 只依赖 `Transport` 的帧级读写契约，不绑定 StreamChannel：

- `initFromChannel`：明文，直接在 StreamChannel 上读写帧；
- `TlsChannel.transport()`：每一帧作为一个 AEAD 记录加密发送——整个协议族共享一次 TLS 握手和一套密钥。

```
                     MultiplexChannel(configs, frame_budget)
                     ══════════════════════════════════

  Reader Fiber [R]                         Writer Fiber [W]
  ┌───────────────────────┐                ┌───────────────────────┐
  │ loop:                 │                │ loop:                 │
  │  readFrame()          │                │  wb_data.receive()    │
  │  解析 seg_count/段     │                │  收集各协议 wb         │
  │  按段 → free 槽位 → rb │                │  合并成一个帧          │
  └───┬───────┬───────┬───┘                │  writeFrame()         │
      ▼       ▼       ▼                    └───▲───────▲───────▲───┘
  ┌───────────┐ ┌───────────┐ ┌───────────┐   │       │       │
  │ SubChannel│ │ SubChannel│ │ SubChannel│ ───┘       │       │
  │ wb ▸ rb   │ │ wb ▸ rb   │ │ wb ▸ rb   │ ←─────────┘       │
  └───────────┘ └───────────┘ └───────────┘ ←─────────────────┘
      p: send()=wb 交接        p: recv()=rb 交接
```

### 3.1 数据流

**发送（写）路径：**
1. 协议 fiber：`send()` 等待 `wb_free`（wb 空闲）→ `codec.encode` 写入 wb → `wb_data.send(id)`（**无缓冲通道**，阻塞到 Writer 取走，保证消息不会被覆盖）
2. Writer Fiber：收到通知 → 把各协议 wb 的完整消息合并进 `frame_buf`（装不下的留待下一帧）→ `transport.writeFrame()` → 归还 `wb_free`

**接收（读）路径：**
1. Reader Fiber：`transport.readFrame()` 读入一帧 → 解析段 → 从 `free` 取槽位 → 拷贝段到槽位 → 投递到 `rb`
2. 协议 fiber：`recv()` → 归还上一个槽位 → `rb.receive()` 阻塞取槽位 → `codec.decode`（切片有效到下次 recv）

`close_channel` 溢出时：Reader 先投递同帧其他协议的段，再关闭出错协议（排空 rb、返回 `error.ProtocolOverflow`）。`backpressure` 溢出时：Reader 阻塞在 `free` 上，把压力经 TCP 传导回发送方。

### 3.2 架构决策

| 决策 | 理由 |
|------|------|
| **单 Reader Fiber** | 顺序读帧、按段路由，协议代码不接触传输层；每协议帧序天然有序 |
| **单 Writer Fiber** | 唯一的传输写者，帧原子性由架构保证（无需锁）；顺带把多条消息合并成一个帧，减少 syscall |
| **wb 单槽 + 无缓冲通知** | 锁步协议在途消息恒为 1，单槽是精确值；无缓冲 `wb_data` 让 `send()` 阻塞到 Writer 真正取走，杜绝消息丢失 |
| **rb 固定槽位按协议配置** | 锁步取 1；推送协议取在途窗口 W，配合 `backpressure` 平滑突发 |
| **溢出策略可配置** | `close_channel`：只关该协议，返回可区分的 `ProtocolOverflow`；`backpressure`：阻塞 Reader，保留有序可靠但可能拖慢整个连接 |
| **`frame_budget` 帧预算** | 每帧最多合并多少字节；单条消息必须放得下（编译期校验），超限消息在编码时报错 |

> **为什么没有丢帧策略**：框架的状态机是锁步严格交替的，每一帧就是一次协议转移，且对端用 `state_id` 严格校验次序。本地丢弃一帧等于让对端永远等不到下一个状态——这不是"容忍丢失"，而是直接毁掉协议。Mux 对每个子通道提供有序、可靠的传输语义（等价于"连接内的 TCP"），溢出只可能来自恶意对端或非锁步驱动，因此处理方式是 fail-fast（关闭）或背压，绝不静默丢帧。

### 3.3 内存模型

每协议静态分配：`wb_buf`（1 块）+ `slots`（capacity 块），均为 `max_message_size`。**稳态零堆分配**——消息在固定槽位间通过通道交接所有权，Reader 只做一次 memcpy。

**切片生命周期契约**（所有通道一致）：`recv` 返回的消息中 `[]const u8` 字段指向通道内部缓冲区（`last_recv_data` / TlsChannel 的 `decode_buf`），**在下一次 `recv` 之前必须消费完**。

### 3.4 SubChannel 接口

与 `StreamChannel` 完全一致，`symmetric_run` 零改动：

```zig
pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
    _ = self.wb_free.receive() catch |err| return err;   // 等待 wb 空闲
    errdefer self.wb_free.trySend(0) catch {};
    var w = Io.Writer.fixed(self.wb_buf);
    try codec.encode(&w, state_id, val);
    self.wb_msg_len = w.end;
    try self.mux.wb_data.send(self.protocol_id);          // 阻塞到 Writer 取走
}

pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
    if (self.last_idx) |old| self.free.trySend(old) catch {};  // 归还上一个槽位
    const idx = self.rb.receive() catch |err| {
        if (err == error.ChannelClosed) {
            if (self.closed_reason) |reason| return reason;
        }
        return err;
    };
    self.last_idx = idx;
    var r = Io.Reader.fixed(self.slots[idx][0..self.slot_lens[idx]]);
    return codec.decode(&r, state_id, T, self.config.max_message_size);
}
```

### 3.5 生命周期

**init**（`initFromChannel` / `initFromTransport`）：分配每协议 `wb_buf` + `capacity` 个槽位，初始化 wb/rb 交接通道，spawn Reader 与 Writer 两个 Fiber。

**deinit**（顺序严格）：

```
1. close wb_data / wb_free / rb / free → 解除 Reader、Writer 与协议 fiber 的阻塞
2. transport.shutdownReceive() → Reader 的 readFrame() 返回错误 → 退出循环
3. join Reader → join Writer
4. free 全部固定缓冲
5. owns_stream 时 stream.close()（Mux over TLS 场景由上层拥有连接）
```

### 3.7 协议族加密：Mux over TLS

一次 TLS 握手（`Runner(tls.ClientHello).symmetric_run`）之后，把 `TlsChannel` 的记录层作为 Mux 的传输：

```zig
var tc: TlsChannel = undefined;
try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 1024);

var m = Mux(configs);
try m.initFromTransport(allocator, tc.transport());
defer m.deinit();

// 每个协议独立 fiber 运行，共享同一条加密连接
try R1.symmetric_run(.server, &ctx1, m.subChannel(0), P1.A, null);
try R2.symmetric_run(.server, &ctx2, m.subChannel(1), P2.A, null);
```

每一条 Mux 帧作为一条 AEAD 记录（`nonce(24) || tag(16) || ct_len(2) || ct`）发送，帧边界即记录边界；nonce 单调递增并做反重放校验。`TlsChannel` 的 `decode_buf` 必须不小于 `frame_budget + 2`（2 字节长度前缀），由调用方保证。

### 3.6 公平性

写公平性由 Writer Fiber 保证——每轮按协议 id 顺序各取一条消息，装不下的留待下一帧，任何协议不会被饿死超过一轮。

读公平性由 Reader Fiber 的逐段分派保证——一帧内先投递可投递的段，`close_channel` 协议的溢出不会波及同帧其他协议。

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
| 读写方式 | 独占 TCP | 共享 TCP，Reader Fiber 读 + Writer Fiber 写 |
| 退出 | Exit 状态 → 返回 | 协议 Exit → fiber 结束 |

## 6. 测试

六个测试验证 Mux 行为（详见 `src/family_test.zig`）：

1. **单协议握手** — 1 个 SubChannel，明文，验证 ctx 从 0 递增到 3 后 Exit
2. **recv 超时** — 协议 1 正常完成，协议 2 等 100ms 后超时返回 `error.Canceled`
3. **双协议并发** — 2 个 SubChannel 各自独立完成 3 轮交互，验证 ctx 值互不干扰
4. **溢出隔离** — 容量 1 的子通道被灌帧后返回 `error.ProtocolOverflow`，其他协议不受影响
5. **背压保序** — `backpressure` 策略下容量 1 的队列不丢帧、不关闭
6. **TLS 协议族** — 一次握手后两个协议在加密的 Mux 上各自完成 3 轮交互

```zig
const M = Mux(2, 1024, 8); // 兼容 shim：N 个相同配置，帧预算 = max_size + 4
// 双协议示例见第 4 节
```

## 7. 文件结构

```
src/
├── family_mux_channel.zig    ← 传输层：MultiplexChannel + Reader/Writer 双 Fiber
├── family_test.zig           ← 测试
├── root.zig                  ← pub const 导出
└── runner.zig                ← 不动
docs/
└── family.md                 ← 本文档
```
