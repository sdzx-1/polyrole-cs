# 聊天室协议族

## 1. 设计模型

聊天室服务器的完整架构是**两层并发**：

```
Server
│
├─ 共享状态（跨所有连接，需要锁保护）
│   ├─ users:  StringHashMap   ← 多个 Init 协议并发读写
│   └─ board:  ArrayList       ← Chat 协议写, Push 协议读
│
├─ [连接 1]  ──TLS── Mux(3) ── Init / Chat / Push   ← 协议族内并发
├─ [连接 2]  ──TLS── Mux(3) ── Init / Chat / Push
└─ [连接 3]  ──TLS── Mux(3) ── Init / Chat / Push
```

**第一层并发**——协议族内：三个协议共享一条 TCP 连接。`MultiplexChannel(3)` 把它们拆成独立 SubChannel，各自运行 `symmetric_run`。Init、Chat、Push 三个协议的 fiber 互不阻塞。

**第二层并发**——跨连接：N 个客户端连接各自有自己的协议族实例，共享服务端的 `users` 和 `board`。Init fiber 检查用户名时可能另一个 fiber 同时在注册，Chat fiber 写消息时 Push fiber 可能在读。

两层都是真并发。聊天室天然面对多客户端，共享数据结构从设计的第一天就需要同步保护。

## 2. 协议设计

### 2.1 Init — 注册用户名（一次性）

Client 提议用户名，Server 检查重复，通过或拒绝。

```
Send(client, "alice") ──→ Server: users.contains("alice")?
                              ├─ true  → Reply.reject → 回到 Send
                              └─ false → users.put → Reply.accept → Exit
Send(client, quit)  ──→ Exit
```

**并发安全**：多个 Init fiber 同时操作 `users: *StringHashMap`。`contains` + `put` 不是原子操作——两个客户端可能同时检查"alice"不存在，然后同时 `put`。必须用 `zio.Mutex` 保护整个 check-then-put 序列。

### 2.2 Chat — 客户端发消息（Client → Server）

单向协议。外部往 `ctx.pending_text` 放消息，`process` 发送。

```
Say(client, msg) ──→ Server: board.append({from: username, text: msg}) → Ack → Say → 循环...
Say(client, quit) ──→ Exit
```

**并发安全**：多个 Chat fiber 同时 `board.append()`。`ArrayList.append` 不是 fiber 安全的——内部可能触发 realloc，另有读取者时指针失效。必须用 `zio.Mutex` 保护。

### 2.3 Push — 服务端推消息（Server → Client）

单向协议。外部往 `ctx.pending` 放待推送消息，`process` 发送。

```
Push(server, msg) ──→ Client 收到 → Ack → Push → 循环...
Push(server, kick) ──→ Client 收到 → Exit
```

**并发安全**：Push fiber 遍历 `board.items` 时 Chat fiber 可能在 `append`。`items()` 返回的切片在 `append` 触发 realloc 后失效。必须用 `zio.Mutex` 保护读操作。

## 3. 共享状态

```zig
/// 线程安全的消息板。Chat fiber 写，Push fiber 读。
const SharedBoard = struct {
    list: std.ArrayList(Message) = .empty,
    mu: zio.Mutex = .{},

    fn append(self: *@This(), gpa: std.mem.Allocator, msg: Message) !void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        try self.list.append(gpa, msg);
    }

    fn snapshot(self: *@This()) []Message {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        return self.list.items;
    }
};

/// 线程安全的用户名集合。多个 Init fiber 并发读写。
const SharedUsers = struct {
    map: std.StringHashMap(void),
    mu: zio.Mutex = .{},

    fn contains(self: *@This(), name: []const u8) bool {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        return self.map.contains(name);
    }

    fn tryPut(self: *@This(), name: []const u8) bool {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        if (self.map.contains(name)) return false;
        self.map.put(name, {}) catch unreachable;
        return true;
    }
};
```

`tryPut` 把 check-then-put 放在同一个锁区间内，消除 TOCTOU 竞态。

## 4. 运行流程

每个客户端连接上的完整流程：

```
Client                              Server
── connect ──────────────────────→  accept
── Init: propose("alice") ──────→  users.tryPut → accept
                                       ↓
                              spawn Chat fiber + Push fiber (并发)
                                      │              │
── Chat: Say("hello") ──────→  board.append ←──┘              │
── Chat: recv Ack ←─────────                                   │
                                      │              Push: board.snapshot()
                                      │              Push: item("hello") ──→
── Push: recv "hello" ←────────────────────────────────────────┘
```

**Init** 先跑完（同步），完成后 spawn Chat 和 Push 两个 fiber 并发运行。Chat 阻塞在 recv（等用户输入），Push 阻塞在消费者端（等 board 有新消息）。两者完全独立，不互相阻塞。

## 5. 协议族在此处的价值

单协议时，Chat 和 Push 必须挤在一个状态机的 agent 模型里——client 发消息是 client agent，server 推消息是 server agent。两个方向互斥，必须轮询。

协议族不发明"双向状态"——它保持每个协议只有一个 agent，只是让两个协议各跑各的。共享状态（board）的同步是另一层问题，用锁解决。这两层分离是干净的：**协议管通信语义，锁管共享内存**。
