# 聊天室协议族

## 1. 设计：两层并发，持久循环

聊天室服务器的完整架构是两层并发：

```
Server
│
├─ SharedState（跨连接，锁保护）
│   ├─ users  [StringHashMap + Mutex]
│   └─ board  [ArrayList + Mutex]
│
├─ Conn 1 ──TCP── Mux(3)
│               ├─ Init ──── 一次性，注册完退出
│               ├─ Chat ──── persistent loop，阻塞在输入 channel
│               └─ Push ──── persistent loop，阻塞在 board channel
│
├─ Conn 2 ──TCP── Mux(3) ── Init / Chat / Push
└─ Conn 3 ──TCP── Mux(3) ── Init / Chat / Push
```

**第一层并发：** 一个连接内三条独立对话流同时运行——Init 跑完就退出，Chat 和 Push 各自阻塞在自己的 channel 上，有数据时醒来处理。

**第二层并发：** 多个客户端共享 `users` 和 `board`，用 `zio.Mutex` 保护。

### 关键区别

Chat 和 Push 是**持久循环**——不是"一条消息跑一次 symmetric_run"，而是"一次 symmetric_run 跑永久"。Process 通过 `zio.Channel.receive()` 阻塞等待外部输入，无新消息时 fiber 让出 CPU。

```
外部注入数据 → channel ---→ process.receive() 醒来 → 处理 → 发往对端 → 收 ack → 回到 process.receive() 阻塞
                              ↑
                        永远不退出，除非 channel 关
```

这个循环不在外部 for 循环中——它在 `symmetric_run` 内部，通过 Ack → Say 的转移自然形成。

## 2. 三个协议

### Init — 一次性用户名注册

```
Send(client, "alice") ──→ Reply.accept → Exit
                        └─→ Reply.reject → Exit
```

Client 发一次名字，Server 回复一个结果，协议结束。外部检测 `accepted` 的值，失败可重新注册（启动新的 Init）。

### Chat — 持续发消息（Client → Server）

```
state Say(client):
    process:    阻塞在 input_ch.receive() → 拿到 text → .send(text)
    
state Ack(server):
    preprocess: board.append( {from: username, text: msg} )
    process:    return .ok

loop: Say.send → Ack.ok → Say.send → Ack.ok → ...
```

Client 端 `input_ch` 由外部填充（用户输入）。无消息时 process 阻塞，fiber 让出。

Server 端 `Say.preprocess` 把消息写入 board。之后 `Ack.process` 发回确认。然后回到 Say 等待下一条。

`input_ch.close()` → Say.process 收到 ChannelClosed → `.quit` → Chat 退出。

### Push — 持续推送（Server → Client）

```
state Push(server):
    process:    阻塞在 board_ch.receive() → 拿到 msg → .item(msg)
    
state Ack(client):
    preprocess: recv.append(msg)
    process:    return .ok

loop: Push.item → Ack.ok → Push.item → Ack.ok → ...
```

Server 端 `board_ch` 由外部填充（board 消费者）。无消息时 process 阻塞。

Client 端 `Push.preprocess` 把收到的消息写入 recv 列表。

`board_ch.close()` → Push.process 收到 ChannelClosed → `.kick` → Push 退出。

## 3. 生命周期

```
Client fiber                              Server fiber
│  connect                                 │  accept
│  Init("alice") ──────────────────────→   Init.accept
│                                          │
│  spawn Chat fiber                        │  spawn Chat server fiber
│    Say.process ← input_ch                │    Say.preprocess → board.append
│    → send("hello") ─────────────────→    │    Ack.process → ok
│    ← recv Ack ───────────────────────    │    → Say (下一轮)
│    Say.process ← input_ch                │
│    → send("world") ─────────────────→   │    同上
│                                          │
│  spawn Push fiber                        │  spawn Push server fiber
│    ← recv Push.item ─────────────      │    Push.process ← board_ch
│    recv.append()                        │    → send(item) ─────────→
│    Ack.process → ok ──────────────→    │    ← recv Ack
│                                          │
│  close input_ch → Chat 退出              │  board_ch.close → Push 退出
│  Push 收到 kick → 退出                   │
│                                          │
│  join Chat + Push                        │  join Chat + Push
```

## 4. 共享状态

```zig
const SharedBoard = struct {
    list: std.ArrayList(Message),
    mu: zio.Mutex,

    fn append(msg)     { mu.lock(); defer mu.unlock(); list.append(msg); }
    fn takeAll() []Msg { mu.lock(); defer mu.unlock(); const items = list; list = .empty; return items; }
};

const SharedUsers = struct {
    map: std.StringHashMap(void),
    mu: zio.Mutex,

    fn tryPut(name) bool { mu.lock(); defer mu.unlock(); if (map.contains(name)) return false; map.put(name, ...); return true; }
};
```

## 5. 协议族在此处的价值

单协议聊天室必须轮询——"有新消息吗？没有。有吗？没有。"——因为 `agent` 模型只允许一个人说话。

协议族把它拆成两个独立持久循环，每个只有一方说话，没有轮询。

Chat（Client → Server）：每个 client 一个独立对话流，消息沿着自己的 state machine 流向 server。
Push（Server → Client）：每个 client 一个独立对话流，消息从 server 流向 client，由 board channel 驱动。

两者共享一条 TCP 连接，各自 fiber 独立阻塞，调度器在有数据时唤醒对应的 fiber。这正是协议族的产品价值——**在单 agent 约束下实现双向无轮询通信**。
