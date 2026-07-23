# 聊天室协议族（第二版）

## 1. 架构总览

```
Server
│
├─ SharedBoard（跨连接，无锁读 / 有锁写）
│   ├─ items: ArrayList(Message)     ← ensureTotalCapacity 后永不搬迁
│   ├─ mu: Mutex                     ← 仅 Writer 竞争
│   └─ committed: atomic(usize)      ← release/acquire 屏障
│
├─ Conn 1 ──TCP── Mux(3)
│               ├─ Init ──── 协议内 retry 循环，注册完退出
│               ├─ Chat ──── persistent loop，Writer 角色
│               └─ Push ──── persistent loop，Reader 角色
│
├─ Conn 2 ──TCP── Mux(3) ── Init / Chat / Push
└─ Conn 3 ──TCP── Mux(3) ── Init / Chat / Push
```

**两层并发：**
- 第一层：一条连接内三个协议独立 fiber，各自阻塞在 Runner 驱动循环中
- 第二层：多个连接共享 SharedBoard，Writer 互斥写，Reader 无锁读

**核心转变：** 旧版用 BroadcastChannel（带锁的 channel 列表 + pub/sub）把消息从 Chat 推给 Push。新版把 board 做成无锁读的安全数据结构，Push 定期批量拉取。消除了额外抽象层和一轮 mutex。

---

## 2. SharedBoard — 无锁读 / 有锁写

```zig
const SharedBoard = struct {
    items: std.ArrayList(Message),
    mu: zio.Mutex,
    committed: std.atomic.Value(usize),

    fn init(gpa: Allocator, capacity: usize) @This() {
        var self: @This() = .{ .items = .empty, .mu = .{}, .committed = .init(0) };
        self.items.ensureTotalCapacity(capacity) catch unreachable;
        return self;
    }

    fn append(self: *@This(), msg: Message) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        self.items.appendAssumeCapacity(msg);
        self.committed.store(self.items.items.len, .release);
    }
};
```

**Writer（Chat.preprocess）：** lock → appendAssumeCapacity → committed.store(.release) → unlock

**Reader（Push）：** committed.load(.acquire) → 直接读 items[cursor..end]，全程无锁

atomic release/acquire 保证：Reader 看到 committed 的新值时，必然也看到对应的数据写入。

`ensureTotalCapacity` 消除 reallocation：items.ptr 在 init 后永不改变，Reader 手上无悬空指针风险。

---

## 3. Init — 协议内 retry 循环

```
Send ──propose──→ Reply ──accept──→ Exit
  ↑                 │
  └──reject─────────┘
```

Client 先 push 所有候选名进 input channel，然后一次 `symmetric_run`。如果第一个名字被拒，Runner 自动沿 reject→Send 回到 `Send.process`，阻塞在 `input_ch.receive()` 等待下一个候选。

```zig
init_ch.send("alice") catch {};
init_ch.send("a1ice") catch {};
init_ch.close(.graceful);

try Runner(init.Send).symmetric_run(.client, &ic, sub, init.Send, null);
```

> **结构不变。** Init 的 retry 循环是现有设计，不是新引入的。

---

## 4. Chat — 单状态自循环

```
Say ──send: Data(MsgPayload, @This())──→ Say (self-loop)
  │
  └──quit──→ Exit
```

`Data(MsgPayload, @This())` 的自循环就是确认——Runner 把帧发给 server 端，两端转到 Say，server 的 preprocess 处理数据。不需要额外的 Ack 状态。

```zig
pub const ServerContext = struct {
    gpa: Allocator,
    board: *SharedBoard,
    username: []const u8,
};
```

**Say.preprocess** 只做一件事：board.append。

**移除内容：** BroadcastChannel、BcMsg 别名、Ack 状态。ServerContext 5 字段 → 3 字段。

---

## 5. Push — 决策与执行分离

### 状态图

```
Sync ──0 < committed ≤ CHUNK──→ Poll
  │
  ├──committed > CHUNK──→ Chunk ──items──→ Chunk (self-loop)
  │                           │
  │                           └──last──→ Poll
  │
  └──committed == 0──→ Poll

Poll ──0 < Δ ≤ CHUNK──→ Poll (self-loop)
  │
  ├──Δ > CHUNK──→ Chunk (同上)
  │
  └──quit──→ Exit
```

`direct: Data(ChunkPayload, Poll)` 和 `items: Data(ChunkPayload, Chunk)` 的 self-loop 模式消除了所有 Ack 状态——Runner 发完帧后两端自然进入同一状态，不需要应用层额外确认。

### 分离原则

| 状态 | 职责 | 禁止 |
|------|------|------|
| Sync | 检查是否有历史数据 | 不发送数据 |
| Poll | 检查是否有新数据 | 不发送数据 |
| Chunk | 发送一块数据 | 不检查 board |

**Sync 不自循环发数据。Poll 不自循环发数据。** 发送是 Chunk 的唯一职责。小数据走 Poll self-loop 直发，大数据委托 Chunk self-loop 分块。

### Chunk 出口

Chunk 不再参数化——出口总是 Poll。`items → Chunk` 自循环分块，`last → Poll` 结束。

### 为什么需要 CHUNK_SIZE

Mux 在一条 TCP 连接上复用三个协议。如果 Push 一个 frame 包含 10000 条消息，Chat 的帧在此期间被阻塞——**Mux 队头阻塞**。分块后每块 ≤ CHUNK_SIZE 条，单帧可控，Mux 公平交替调度。

---

## 6. 时序

```
alice: connect → Init("alice") ✓
bob:   connect → Init("bob") ✓
charlie: connect → Init("charlie") ✓
         │
alice: spawn Push fiber
       Sync: committed=0 → .done → Poll (sleep)
bob:   spawn Push fiber — 同上
charlie: spawn Push fiber — 同上
         │
         │  spawn Chat fiber（无时序依赖，不需要 sleep）
         │
alice: cc.send("hello") → Chat fiber → board.append("hello") + committed=1
       alice Poll 醒来: committed=1 > cursor=0, Δ=1 ≤ CHUNK → .direct([hello])
       bob   Poll 醒来: 同上
       charlie Poll 醒来: 同上

bob: cc.send("hi") → committed=2, 各 Poll: Δ=1 → .direct([hi])
charlie: cc.send("hey") → committed=3, 各 Poll: Δ=1 → .direct([hey])
         │
         │  cc.close() → Chat exit
         │  Join fibers
         │
         │  验证: board.items.len = 3, 每个 recv = [hello, hi, hey]
```

---

## 7. 旧版 → 新版对照

| 维度 | 旧版 | 新版 |
|------|------|------|
| 广播机制 | BroadcastChannel（带锁 channel 列表 + pub/sub） | SharedBoard（无锁读，atomic commit） |
| Chat 写 board | lock → append → bc.publish（嵌套锁） | lock → append → committed.store |
| Push 读 board | 通过 bc 被动接收 channel 推送 | 主动定期 snapshot board[cursor..] |
| Push 数据方式 | 逐条 item → ack 往返 | 批量 self-loop，一次发一包 |
| Push 分块 | 无 | Chunk self-loop，小数据 Poll self-loop |
| Ack 状态 | Chat: Ack, Push: AckSmall + AckChunk | 全部删除 — self-loop 即是确认 |
| Mux write 路径 | 共享 channel FIFO + writerLoop | 各协议直接写 TCP + write_mu |
| Mux read 路径 | rb.send() 阻塞（一个满全卡） | rb.trySend() 非阻塞（满则终止该协议） |
| 状态数 | Chat 2, Push 5 | Chat 1, Push 3 |
