# 聊天室协议族（重构）

## 设计原则

不再把 Chat 和 Push 硬塞进 `symmetric_run` 的**一次循环**里——每个协议都是一个**外部驱动的消息循环**，每次外部输入执行一次 `symmetric_run`。

## 协议

### Init — 注册用户名

```
Send(client, "alice") → Server: users.tryPut("alice")
                              ├─ true → Reply.accept → Exit
                              └─ false → Reply.reject → Exit
```

一次性请求-响应。**没有重试循环**——client 收到 reject 就退出，外部会重新尝试。

### Chat — 发送消息（Client → Server）

```
Say(client, "hello") → Server: board.append → Ack(server) → Exit
```

**client 端：** 外部循环准备一条消息 → `symmetric_run(.client, Say, text)` 发送并等 ack → 外部准备下一条。

**server 端：** `Say.preprocess` 把收到的消息写入 board（加锁）。

### Push — 推送消息（Server → Client）

```
Push(server, msg) → Client 收到 → Ack(client) → Exit
```

**server 端：** 外部循环从 board 取最新消息 → `symmetric_run(.server, Push, msg)` 发送并等 ack → 外部取下一条。无消息时阻塞。

**client 端：** `Push.preprocess` 把收到的消息写入 recv 列表。

## 运行流程

```
Client fiber                          Server fiber
│                                       │
├─ Init("alice") ──────────────────→  Init.accept
│                                       │
│  while(有用户输入):                    │
│    Chat("hello") ───────────────→   board.append, Ack
│                                       │
│                                       while(有新消息):
│                                         Push(msg) ──→ recv.append, Ack
```

Chat 和 Push 跑在独立 fiber 中。Chat fiber 每发完一条回到外部循环，Push fiber 每次推完也回到外部循环。不再有 `for (msg)` 重启 `symmetric_run`。

## 并发与锁

- `users: StringHashMap` — `SharedUsers.tryPut` 带 `zio.Mutex`
- `board: ArrayList` — `SharedBoard.append` / `snapshot` 带 `zio.Mutex`

两层并发不变——同连接内 Chat/Push fiber 并发，跨连接多个 Init 并发。
