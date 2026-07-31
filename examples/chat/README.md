# 多人聊天室 demo（polyrole-cs）

基于 polyrole-cs 框架的多人聊天室示例：**Mux 双协议 + 明文 TCP**。
广播采用 **SharedBoard（共享消息板）+ 游标拉取**（见 `docs/chat-scale-10000.md` §3.5）。

## 架构

每个客户端一条 TCP 连接，跑 `MultiplexChannel(2, ...)`（见 `docs/family.md`），
两个子通道承载两个协议：

| 子通道 | 协议 | 模型 | Mux 配置 |
|---|---|---|---|
| 0 | **Ctrl 控制协议**（上行） | 锁步，容量 1 | `close_channel` 溢出 |
| 1 | **Push 推送协议**（下行） | 游标拉取，8 条/帧 | `capacity 16 + backpressure` |

```
客户端                                  服务器
stdin fiber: 异步读行 → 输入队列         accept loop → 每连接监督 fiber
Ctrl 主循环: 注册/发消息/心跳/退出  ◀───▶  Ctrl 协议（监督 fiber 亲自驱动）
Push fiber:  消费推送并打印        ◀───▶  Push 协议（按游标拉取 SharedBoard）
                                          Room fiber: 成员表 + 消息板唯一写者
```

## 协议状态机

**Ctrl（锁步，客户端每 ~100ms 一轮，心跳内建）：**

```
Login(client) ─Register{nickname}─▶ Welcome(server) ─Welcome{id,count}─▶ Send(client)
Send(client) ─Msg{heartbeat,who}/Quit─▶ Ack(server) ─Ack(void)─▶ Send(client)
Send.quit ─▶ Exit（两端同时终止）
```

- 客户端 `Send.process`：每 100ms 查一次输入（消息/`/who`/`/quit`），累计 1s 无输入发心跳
- 服务器 `Send.preprocess`：聊天消息/加入离开通知/`/who` 响应全部 `SharedBoard.append`（O(1)）
- 服务器 Ctrl recv 超时 20s（客户端心跳 1s）→ 判定掉线，清理并广播离开

**Push（SharedBoard 游标拉取）：**

```
Poll(server) ─batch{8条}/idle─▶ Poll；kick ─▶ Exit
```

- 服务器 `Poll.process`：游标后有新消息则批量拉取推送（8 条/帧），否则空转 100ms 发 idle
- 客户端纯消费（收件箱/打印）；慢消费者只是游标落后，不会被断开
- 服务器死亡由 Ctrl 心跳检测（客户端 Push 不设超时）

## 载荷（全部定长，codec 原生支持，wire 路径零分配）

| 载荷 | 字段 | 大小 |
|---|---|---|
| `RegisterPayload` | `nickname: [32]u8` | 32B |
| `WelcomePayload` | `client_id: u32, member_count: u32` | 8B |
| `MsgPayload` | `seq: u64, text: [256]u8` | 264B |
| `ChunkPayload` | `msgs: [8]PushPayload, count: u8` | ~2.5KB/帧 |
| `PushPayload` | `kind: u8, seq: u64, from_id: u32, from_name: [32]u8, text: [256]u8, ts_ms: u64` | ~310B |

`kind`：0 = 聊天消息，1 = 系统通知（加入/离开，`from_id=0`），2 = `/who` 响应。
`ts_ms` 为服务器单调时钟，仅作显示参考，跨机不可比。

## 并发与一致性

- **Room 成员表 + 消息板只有一个写者**：所有成员操作（register/remove/broadcast/who）
  投递到 ops 队列，由 Room fiber 串行处理；广播 = `SharedBoard.append`（O(1)）。
  不用 OS 互斥锁——zio 协作式运行时中 fiber 持锁跨阻塞点会死锁；
  Channel 串行化不依赖线程模型。`Room.drain()` 是同步可测的纯逻辑。
- **SharedBoard 无锁读/有锁写**：预分配永不搬迁 + 原子 `committed` 游标 +
  release/acquire 屏障；各连接的 Push 按游标批量拉取，**慢消费者只是游标落后，
  不会被断开**。
- **`--silent-join`**：服务器可关闭加入/离开通知（大群或压测用，
  避免注册风暴的 O(N²) 加入通知刷板）。

## 服务器连接生命周期（监督 fiber）

Ctrl 协议由监督 fiber 亲自驱动（它是连接生命线），Push 跑在独立 fiber：

```
Ctrl 结束（/quit / 掉线 / 注册失败 / 连接断开）
  → ctrl_ctx.leave()（Room.remove + 广播离开，幂等）
  → push_ctx.kick = true（Push 协议走 .quit 退出）
  → push_h.join() → mux.deinit() → sc.deinit()
```

## 运行

```bash
zig build

# 终端 1：服务器（默认端口 7788；--silent-join 关闭加入通知）
zig-out/bin/chat-server [port] [--silent-join]

# 终端 2/3：两个客户端
zig-out/bin/chat-client alice
zig-out/bin/chat-client bob
```

- 输入一行回车发送消息；`/who` 查看在线成员；`/quit` 退出
- 消息长度上限 255 字节，昵称上限 31 字节（超长截断）
- 客户端：服务器 20s 无响应判定连接死亡（Ctrl 心跳检测）
- 压测：`tools/chat_loadtest.zig`（见 `docs/chat-scale-10000.md` §5，10000 并发验证）

## 测试

```bash
zig build test
```

覆盖（`examples/chat/test.zig`）：

1. **Room 纯逻辑**：register/remove/broadcast（消息板断言）、`notify_joins` 关闭
2. **Ctrl simulate**：注册 → 欢迎 → 消息广播 → `/who` → 退出广播（消息板断言）
3. **双客户端网络集成**：加入/离开通知、消息互达（board 模式含发送者回显）、`/who`
4. **掉线检测**：客户端注册后断开，服务器清理并广播离开通知

## 设计取舍

| 决策 | 理由 |
|---|---|
| 明文而非 TLS | demo 简化；生产可先跑库内置 tls 握手再建 TlsChannel（见 `src/protocol/tls/`） |
| 双协议 Mux 而非单协议轮询 | Push 真推送零轮询延迟；控制与推送互不阻塞 |
| 定长载荷 | codec 原生支持，wire 路径零分配；昵称/消息超长截断 |
| 掉线靠 Ctrl 心跳 | 客户端 100ms 心跳 = 天然 liveness 信号；服务器 2s 超时踢人 |
| 服务器不 echo 给发送者 | 客户端本地回显，省一半广播带宽 |
| 消息延迟上限 ~200ms | 发送方轮询等待 ≤100ms + 接收方轮询 ≤100ms + RTT |

## 文件

```
examples/chat/
├── protocol.zig    # 两个协议状态机 + 上下文 + Room + 载荷
├── server.zig      # 服务器：accept loop + 监督 fiber + Room fiber
├── client.zig      # 客户端：Ctrl 主循环 + Push 消费 + stdin 异步读行
├── test.zig        # 上述四组测试
└── README.md       # 本文档
```
