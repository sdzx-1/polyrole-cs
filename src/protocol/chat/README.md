# 聊天室协议族

## 1. 为什么是协议族

### 1.1 单协议的不给力

最初尝试用一个协议描述聊天室：

```
Client: "发送消息" → Server: "确认" → Client: "有新消息吗？" → Server: "没有"
                                         ↑
                                    轮询—浪费
```

核心矛盾：**聊天室有两个独立的对话方向——用户发消息和服务器推消息——但单协议状态机只有一个 agent。** 你被迫在 client 的轮询循环里检查服务器有没有新消息，这感觉不对。

### 1.2 分解而非修改

不要改协议，加一个协议：

```
SubChannel[0]: Init  — Client → Server（注册用户名，一次性）
SubChannel[1]: Chat  — Client → Server（用户发消息）
SubChannel[2]: Push  — Server → Client（服务器推送消息/通知）
```

每个协议只做一个方向的通信，状态机的 `agent` 模型无需修改。三个协议跑在一个 `MultiplexChannel(3, false)` 上，共享一条 TCP 连接，互不干扰。

### 1.3 概念闭合

polyrole-cs 建模一件事：「一个 agent 对一个人说话」。协议族没有修改这个规则——它只是让这个规则应用了 N 次。**不发明「双向模式」，不改状态机的 agent 定义，不改 Runner 的调度逻辑。** 同一个 peer，N 个 agent 各自说话，共享一条线。

---

## 2. 协议详解

### 2.1 Init — 初始化协议

Client 发送用户名，Server 检查是否重复。不通过可以重试，也可以放弃退出。

```
Send(client, "alice") ──→ Server 检查
                               │
                     ┌─ taken ─┼─ free ─┐
                     ↓                  ↓
               Reject(server)      Accept(server)
                     ↓                  ↓
               ← 回到重试             Exit(成功)
               
Send(client, quit) ──→ Server 记录退出 → Exit
```

**实现**：`src/protocol/chat/init.zig`

```zig
pub const Send = union(enum) {
    propose: Data(NamePayload, Reply),  // 提议用户名
    quit: Data(void, Exit),             // 放弃

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.name_len == 0) return .quit;
        return .{ .propose = .{ .data = .{ .name = ctx.username, .name_len = ctx.name_len } } };
    }
};

pub const Reply = union(enum) {
    accept: Data(void, Exit),            // 通过
    reject: Data(void, Send),            // 拒绝 → 回到 Send 重试

    pub fn process(ctx: *ServerContext) @This() {
        const name = ctx.pending_name[0..ctx.pending_len];
        if (ctx.users.contains(name)) return .reject;
        ctx.users.put(name, {}) catch unreachable;
        return .accept;
    }
};
```

`Send.process` 是 client 端的「决定发送什么」——检查 `name_len`，有名字就提议，没有就退出。

`Reply.process` 是 server 端的「根据检查结果回复」——名字已存在就拒绝（client 可以重试），不存在就接受并注册。

### 2.2 Chat — 用户发消息协议

单向 Client → Server。外部往 `ctx.pending_text` 放消息文本，`process` 发送。

```
Say(client, "hello") ──→ Server 追加到消息列表 → Ack(server) → Say(client) → 循环...
Say(client, quit)   ──→ Server 记录退出 → Exit
```

**实现**：`src/protocol/chat/chat.zig`

```zig
pub const Say = union(enum) {
    send: Data(MsgPayload, Ack),
    quit: Data(void, Exit),

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.done) return .quit;
        if (ctx.pending_text) |text| {
            var buf: [MaxTextLen]u8 = undefined;
            const copy_len = @min(text.len, MaxTextLen);
            @memcpy(buf[0..copy_len], text[0..copy_len]);
            ctx.pending_text = null;
            return .{ .send = .{ .data = .{ .text = buf, .text_len = copy_len } } };
        }
        return .quit;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const text = d.data.text[0..d.data.text_len];
                const dup = ctx.gpa.dupe(u8, text) catch return;
                ctx.messages.append(ctx.gpa, .{ .from = ctx.username, .text = dup }) catch {};
            },
            .quit => {},
        }
    }
};
```

**关键设计：** `pending_text` 是外部写入的 `?[]const u8`。`process` 消费它（设为 null 并返回 `.send`）。如果有新消息，外部重新设置 `pending_text`。

**注意：** `ctx.done` 必须在消息发送后设置，否则 `process` 先判断 `done` 导致消息还没发就退出。

### 2.3 Push — 服务器推送协议

单向 Server → Client。外部往 `ctx.pending` 放消息，`process` 发送。`ctx.kick = true` 可踢人。

```
Push(server, ".msg") ──→ Client 收到广播 → Ack(client) → Push(server) → 循环...
Push(server, kick)  ──→ Client 收到踢出 → Exit
```

**实现**：`src/protocol/chat/push.zig`

```zig
pub const Push = union(enum) {
    item: Data(ItemPayload, Ack),
    kick: Data(void, Exit),

    pub fn process(ctx: *ServerContext) @This() {
        if (ctx.kick) return .kick;
        if (ctx.pending) |msg| {
            // 复制到定长数组（codec 不支持 []const u8）
            var from_buf: [MaxNameLen]u8 = undefined;
            const from_len = @min(msg.from.len, MaxNameLen);
            @memcpy(from_buf[0..from_len], msg.from[0..from_len]);
            var text_buf: [MaxTextLen]u8 = undefined;
            const text_len = @min(msg.text.len, MaxTextLen);
            @memcpy(text_buf[0..text_len], msg.text[0..text_len]);
            ctx.pending = null;
            return .{ .item = .{ .data = .{
                .kind = msg.kind, .from = from_buf, .from_len = from_len,
                .text = text_buf, .text_len = text_len,
            } } };
        }
        return .kick;
    }
};
```

---

## 3. 协调整合

server 端初始化 Chat 和 Push 的上下文，注入共享资源：

```zig
// Server 端
var users = std.StringHashMap(void).init(allocator);
var chat_msgs: std.ArrayList(chat.Message) = .empty;
var push_msgs: std.ArrayList(push.Message) = .empty;

// Init: 检查用户名
var init_srv = init.ServerContext{ .users = &users };

// Chat: server 收到消息后追加到 chat_msgs
var chat_srv = chat.ServerContext{
    .messages = &chat_msgs,
    .username = "alice",
    .gpa = allocator,
};

// Push: 外部从 chat_msgs 取消息，放入 pending
push_srv.pending = push.Message{
    .kind = push.KIND_MSG,
    .from = chat_msgs.items[0].from,
    .text = chat_msgs.items[0].text,
};
```

三个协议的 context 共享同一个 `users` set 和 `chat_msgs` 列表。Chat 协议往 `chat_msgs` 里放消息，外部协调代码从 `chat_msgs` 取消息放入 Push 的 `pending`。这是刻意的手动协调——协议本身不耦合，数据的流动由调用方决定。

---

## 4. 运行流程

```
Server:                          Client:
  accept                            connect
  init(.server)      ←──→           init(.client)     // 注册 "alice"
  chat(.server)      ←──→           chat(.client)     // 发送 "hello"
  push(.server)      ←──→           push(.client)     // 收到广播
```

- **Init** 是同步的——先完成再启动后续协议
- **Chat** 是同步的——client 发送一条消息后退出（生产环境中可以循环）
- **Push** 是同步的——server 发送待推送消息后退出

三种协议的生产版本应该并发运行：Init 完成后，Chat 和 Push 各自在 fiber 中跑 `symmetric_run`，Push 的 context 由外部持续填入待推送消息。

---

## 5. 设计约束与未完善处

### codec 不支持 `[]const u8`
协议 payload 中不能直接用 `[]const u8`，必须用定长数组 `[MaxTextLen]u8` + 长度字段。`codec` 需要支持 slices。

### codec 不支持 enum
`push.Kind` 应该是 `enum { msg, join, leave, kick }`，但 codec 只能序列化 `u8`。枚举被降级为 `u8` 常量。

### 消息内存管理
`server_chat_msgs` 中的 `Message.from` 和 `Message.text` 是堆分配的（`gpa.dupe`），需要调用方手动释放。理想方案是所有消息都放入 arena allocator。

### `done` 判断顺序
`Say.process` 中 `ctx.done` 在 `ctx.pending_text` 之前判断会导致「标记退出后消息未发送」。设计上应该让调用方先发送消息再标记退出，或修改 `process` 的检查顺序。
