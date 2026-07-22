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

## 4. Chat — 简化为单职责 Writer

```
Say ──send──→ Ack ──ok──→ Say
  │
  └──quit──→ Exit
```

```zig
pub const ServerContext = struct {
    gpa: Allocator,
    board: *SharedBoard,
    username: []const u8,
};
```

**Say.preprocess** 只做一件事：board.append。

**移除内容：** BroadcastChannel、BcMsg 别名、push import。ServerContext 5 字段 → 3 字段。

---

## 5. Push — 决策与执行分离

### 状态图

```
Sync ──0 < committed ≤ CHUNK──→ Ack.small ──ok──→ Poll
  │
  ├──committed > CHUNK──→ Chunk(Poll)
  │
  └──committed == 0──→ Poll

Poll ──0 < Δ ≤ CHUNK──→ Ack.small ──ok──→ Poll
  │
  ├──Δ > CHUNK──→ Chunk(Poll)
  │
  └──Δ == 0──→ Poll (自循环, sleep)

Chunk(Done) ──items──→ Ack.chunk ──ok──→ Chunk(Done)
  │
  └──last──→ Ack.chunk ──into──→ Done
```

### 分离原则

| 状态 | 职责 | 禁止 |
|------|------|------|
| Sync | 检查是否有历史数据 | 不发送数据 |
| Poll | 检查是否有新数据 | 不发送数据 |
| Chunk(Done) | 发送一块数据 | 不检查 board |
| Ack.chunk | 响应分块 | 不检查 board |
| Ack.small | 响应小数据 | 不检查 board |

**Sync 不自循环发数据。Poll 不自循环发数据。** 发送是 Chunk 的唯一职责。

### 参数化状态：Chunk(Done)

参照 sendfile 的 `CheckHash(A, B)` 模式。模板复用一次（两个入口同一出口），逻辑写在一处。

### 小数据 shortcut

当 `Δ ≤ CHUNK_SIZE` 时，Sync/Poll 的 `direct` 分支直接发，不经过 Chunk → Ack 绕路。

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
| Push 数据方式 | 逐条 item → ack 往返 | 批量 direct/chunk，一次发一包 |
| Push 分块 | 无（依赖 channel 背压） | Chunk(Done) 参数化状态，小数据 shortcut |
| Mux 队头阻塞 | 单帧可能包含全部历史消息 | CHUNK_SIZE 限制，公平调度 |
| chat.zig 职责 | 包含 BroadcastChannel 实现 | 纯协议，不依赖 push.zig |
| 抽象数量 | 3 个 Mutex（board, bc, users） | 2 个 Mutex（board write, users） |
