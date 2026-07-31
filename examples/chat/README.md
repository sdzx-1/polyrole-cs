# 多人聊天室 demo（polyrole-cs）

基于 polyrole-cs 框架的多人聊天室示例：**Mux 双协议推送 + 明文 TCP**。

## 架构

每个客户端一条 TCP 连接，跑 `MultiplexChannel(2, ...)`（见 `docs/family.md`），
两个子通道承载两个协议：

| 子通道 | 协议 | 模型 | Mux 配置 |
|---|---|---|---|
| 0 | **Ctrl 控制协议**（上行） | 锁步，容量 1 | `close_channel` 溢出 |
| 1 | **Push 推送协议**（下行） | 服务器 → 客户端真推送 | `capacity 16 + backpressure` |

```
客户端                                  服务器
stdin fiber: 异步读行 → 输入队列         accept loop → 每连接监督 fiber
Ctrl 主循环: 注册/发消息/心跳/退出  ◀───▶  Ctrl 协议（监督 fiber 亲自驱动）
Push fiber:  消费推送并打印        ◀───▶  Push 协议（独立 fiber）
                                          Room fiber: 成员表唯一写者（Channel 串行化）
```

## 协议状态机

**Ctrl（锁步，客户端每 ~100ms 一轮，心跳内建）：**

```
Login(client) ─Register{nickname}─▶ Welcome(server) ─Welcome{id,members}─▶ Send(client)
Send(client) ─Msg{heartbeat}/Quit─▶ Ack(server) ─Ack(void)─▶ Send(client)
Send.quit ─▶ Exit（两端同时终止）
```

- 客户端 `Send.process`：从输入队列取一条（消息/退出）；无输入则空转 100ms 发心跳
- 服务器 `Send.preprocess`：聊天消息走 Room 广播，退出则移除成员并广播离开
- 服务器 Ctrl recv 超时 2s（客户端心跳 100ms）→ 判定掉线，清理并广播

**Push（单状态自环，真推送）：**

```
Deliver(server) ─Push{...}─▶ Deliver(server)
```

- 服务器 `Deliver.process` 阻塞在本连接的广播队列（`zio.Channel(PushPayload)` 容量 16）
- 客户端纯消费，无轮询延迟；服务器死亡由 Ctrl 心跳检测（客户端 Push 不设超时）

## 载荷（全部定长，codec 原生支持，wire 路径零分配）

| 载荷 | 字段 | 大小 |
|---|---|---|
| `RegisterPayload` | `nickname: [32]u8` | 32B |
| `WelcomePayload` | `client_id: u32, member_count: u8, members: [64][32]u8` | ~2KB（一次性） |
| `MsgPayload` | `seq: u64, text: [256]u8` | 264B |
| `PushPayload` | `kind: u8, seq: u64, from_id: u32, from_name: [32]u8, text: [256]u8, ts_ms: u64` | ~310B |

`kind`：0 = 聊天消息，1 = 系统通知（加入/离开，`from_id=0`）。
`ts_ms` 为服务器单调时钟，仅作显示参考，跨机不可比。

## 并发与一致性

- **Room 成员表只有一个写者**：所有成员操作（register/remove/broadcast）投递到
  ops 队列，由 Room fiber 串行处理。不用 OS 互斥锁——zio 协作式运行时中
  fiber 持锁跨阻塞点会死锁；Channel 串行化不依赖线程模型。
  `Room.drain()` 是同步可测的纯逻辑，网络模式下只是包一层 fiber 循环。
- **慢消费者 fail-fast**：广播时目标收件箱已满 → 关闭其收件箱 → 该连接
  Push fiber 收到 `ChannelClosed` 退出 → 监督 fiber 收拾连接并广播断开通知。
- **背压链**：客户端显示慢 → 收件箱满 → Mux backpressure → 服务器广播队列满 → 断开。

## 服务器连接生命周期（监督 fiber）

Ctrl 协议由监督 fiber 亲自驱动（它是连接生命线），Push 跑在独立 fiber：

```
Ctrl 结束（/quit / 掉线 / 注册失败 / 连接断开）
  → ctrl_ctx.leave()（Room.remove + 广播离开，幂等）
  → inbox.close(.graceful)（Push fiber 排空后自然退出）
  → push_h.join() → mux.deinit() → sc.deinit()
```

## 运行

```bash
zig build

# 终端 1：服务器（默认端口 7788，可传参）
zig-out/bin/chat-server [port]

# 终端 2/3：两个客户端
zig-out/bin/chat-client alice
zig-out/bin/chat-client bob
```

- 输入一行回车发送消息，`/quit` 退出
- 消息长度上限 255 字节，昵称上限 31 字节（超长截断）
- 客户端：服务器 5s 无响应判定连接死亡（Ctrl 心跳检测）

## 测试

```bash
zig build test
```

覆盖（`examples/chat/test.zig`）：

1. **Room 纯逻辑**：register/remove/broadcast（同步 `drain`，无需 runtime）、慢消费者断开
2. **Ctrl simulate**：注册 → 欢迎 → 消息广播 → 退出广播（上下文与收件箱断言）
3. **双客户端网络集成**：互见成员表、A 发消息 B 收到（A 收不到自己消息）、
   加入/离开通知、`/quit` 广播
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
