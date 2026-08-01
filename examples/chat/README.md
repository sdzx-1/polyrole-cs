# 多人聊天室 demo(polyrole-cs)— 设计文档

> **状态:P0/P1 已实现,10000 并发压测通过**(见 §8)。
> 本文档合并自原架构推演文档（`docs/chat-scale-10000.md`，已并入本文后删除）与旧版 `examples/chat/README.md`(实现说明),
> 是 chat 的唯一设计文档;推演过程保留供对照,"✅ 已实现"标注与最终实现的差异见 §5.3 说明。

基于 polyrole-cs 框架的多人聊天室示例:**Mux 双协议 + 明文 TCP**。
广播采用 **SharedBoard(共享消息板)+ 游标拉取**(见 §4)。
目标:支撑 **10000 同时在线**(内存/连接管理优先,广播延迟允许百毫秒级)。

---

## 1. 架构

每个客户端一条 TCP 连接,跑 `MultiplexChannel(2, ...)`(见 `docs/family.md`),
两个子通道承载两个协议:

| 子通道 | 协议 | 模型 | Mux 配置 |
|---|---|---|---|
| 0 | **Ctrl 控制协议**(上行) | 锁步,容量 1 | `close_channel` 溢出 |
| 1 | **Push 推送协议**(下行) | 游标拉取,8 条/帧 | `capacity 16 + backpressure` |

```
客户端                                  服务器
stdin fiber: 异步读行 → 输入队列         accept loop → 每连接监督 fiber
Ctrl 主循环: 注册/发消息/心跳/退出  ◀───▶  Ctrl 协议(监督 fiber 亲自驱动)
Push fiber:  消费推送并打印        ◀───▶  Push 协议(按游标拉取 SharedBoard)
                                          Room fiber: 成员表 + 消息板唯一写者
```

### 执行单元账目

服务器每连接 4 个 fiber,另有 1 个全局 Room fiber:

```
服务器(每连接 4 fiber + 全局 1)
├─ 监督 fiber      驱动 Ctrl 协议(锁步:register/msg/heartbeat/quit)—— 连接生命线
├─ Push fiber      按游标从 SharedBoard 拉取推送(8 条/帧,100ms 轮询)
├─ Mux Reader      (框架固有)顺序读帧、按段路由到各协议 rb
├─ Mux Writer      (框架固有)收集各协议 wb、合并成帧写出
└─ Room fiber      (全局共享)串行处理成员操作(register/remove/broadcast)
```

客户端侧 2 个执行单元(Ctrl 主循环 + Push 消费 + stdin 读行)。

---

## 2. 协议状态机

### Ctrl(锁步,客户端每 ~100ms 一轮,心跳内建)

```
Login(client) ─Register{nickname}─▶ Welcome(server) ─Welcome{id,count}─▶ Send(client)
Send(client) ─Msg{heartbeat,who}/Quit─▶ Ack(server) ─Ack(void)─▶ Send(client)
Send.quit ─▶ Exit(两端同时终止)
```

- 客户端 `Send.process`:每 100ms 查一次输入(消息/`/who`/`/quit`),累计 1s 无输入发心跳
- 服务器 `Send.preprocess`:聊天消息/加入离开通知/`/who` 响应全部 `SharedBoard.append`(O(1))
- 服务器 Ctrl recv 超时 20s(客户端心跳 1s)→ 判定掉线,清理并广播离开

### Push(SharedBoard 游标拉取)

```
Poll(server) ─batch{8条}/idle─▶ Poll;kick ─▶ Exit
```

- 服务器 `Poll.process`:游标后有新消息则批量拉取推送(8 条/帧),否则空转 100ms 发 idle
- 客户端纯消费(收件箱/打印)
- 服务器死亡由 Ctrl 心跳检测(客户端 Push 不设超时)

### 载荷(全部定长,codec 原生支持,wire 路径零分配)

| 载荷 | 字段 | 大小 |
|---|---|---|
| `RegisterPayload` | `nickname: [32]u8` | 32B |
| `WelcomePayload` | `client_id: u32, member_count: u32` | 8B |
| `MsgPayload` | `seq: u64, text: [256]u8` | 264B |
| `ChunkPayload` | `msgs: [8]PushPayload, count: u8` | ~2.5KB/帧 |
| `PushPayload` | `kind: u8, seq: u64, from_id: u32, from_name: [32]u8, text: [256]u8, ts_ms: u64` | ~310B |

`kind`:0 = 聊天消息,1 = 系统通知(加入/离开,`from_id=0`),2 = `/who` 响应。
`ts_ms` 为服务器单调时钟,仅作显示参考,跨机不可比。

---

## 3. 并发与一致性

- **Room 成员表 + 消息板只有一个写者**:所有成员操作(register/remove/broadcast/who)
  投递到 ops 队列,由 Room fiber 串行处理;广播 = `SharedBoard.append`(O(1))。
  不用 OS 互斥锁——zio 协作式运行时中 fiber 持锁跨阻塞点会死锁;
  Channel 串行化不依赖线程模型(注:`kick` 除外,见 §9 不变式 #5)。
  `Room.drain()` 是同步可测的纯逻辑。
- **SharedBoard 无锁读/有锁写**:预分配永不搬迁 + 原子 `committed` 游标 +
  release/acquire 屏障;各连接的 Push 按游标批量拉取。慢消费者只是游标落后,
  不会被断开(Push 方向;客户端侧 HOL 见 §9 不变式 #7)。
- **水位**:新连接只拉取最近 `BOARD_KEEP=1024` 条历史(`watermark = committed - 1024`,
  不足 1024 条取 0),之后的一切实时可达——加入通知与聊天消息同规则,不区分。
- **`--silent-join`**:服务器可关闭加入/离开通知(大群或压测用,
  避免注册风暴的 O(N²) 加入通知流量)。

## 4. 服务器连接生命周期(监督 fiber)

Ctrl 协议由监督 fiber 亲自驱动(它是连接生命线),Push 跑在独立 fiber:

```
Ctrl 结束(/quit / 掉线 / 注册失败 / 连接断开)
  → ctrl_ctx.leave()(Room.remove + 广播离开,幂等)
  → push_ctx.kick = true(Push 协议走 .quit 退出)
  → push_h.join() → mux.deinit() → sc.deinit()
```

> 已知行为:Push fiber 在 Ctrl 注册**之前**即 spawn,游标起点为连接建立时的水位——
> 新连接会收到"注册完成前"的消息(见 §9 不变式 #3)。

---

## 5. 设计推演史(瓶颈 → 方案)

### 5.1 10000 并发瓶颈量化

| # | 瓶颈 | 旧架构量级 | 10000 连接 | 性质 |
|---|---|---|---|---|
| 1 | 成员表定长 | `[64]Slot` | 10000 人无法注册(RoomFull) | 硬限制 |
| 2 | fiber 栈内存 | 4 × 256KB/连接 | 40000 × 256KB ≈ 10GB | 内存 |
| 3 | 广播串行 O(N) | 每次广播遍历全部成员 trySend | 10000 × ~100-200ns ≈ 1-2ms/广播 | 延迟 |
| 4 | 心跳流量 | 100ms/连接 | 10 万 poll/s + 10 万 ack/s = 20 万帧/s | syscall/中断 |
| 5 | 消息延迟耦合 | Send.process sleep(interval) 后才查输入 | 放宽心跳 → 消息延迟上界同步变大 | 协议耦合 |
| 6 | ops 队列 64 | 注册/离开入队 | 连接风暴(万级同时建立)时阻塞 | 易修 |
| 7 | inbox 16 | 广播高峰慢消费者断开 | 网络抖动者被频繁踢 | 稳定性 |
| 8 | WelcomePayload 2KB | 全量成员表 | 10000 人全量 = 320KB/欢迎包 | wire 膨胀 |

### 5.2 分方向推演

**5.2.1 成员表:定长数组 → 动态结构(P0,✅ 已实现)**

`std.ArrayList(Slot)` 动态扩容 + `std.ArrayList(u32)` 空闲槽位栈(O(1) 复用);
client_id = 槽位索引(u32),删除的槽位进 free 列表;`count: usize`。
广播仍为 O(N) 遍历 `slots.items`,紧凑数组缓存友好。

**5.2.2 Welcome 协议:全量成员表 → 按需获取(P0,✅ 已实现)**

- `WelcomePayload` 只带 `client_id + member_count`(8B),去掉定长成员数组
- 成员列表按需获取:Ctrl 新增 `Send.who` 变体(客户端 `/who` 命令),
  服务器经 Room 查询(`MemberListReply`,截断 `WHO_LIST_LIMIT=32` 个名字)后
  走 **Push 通道**回 `kind=member_list` 响应(避免锁步通道的大帧阻塞)

**5.2.3 心跳:放宽间隔 + 消息延迟解耦(P1,✅ 已实现,方案有修正)**

- 间隔 100ms → 1000ms;服务器 Ctrl recv 超时 2s → **20s**(TCP 断开时 recv 立即报错,超时只兜底"活着但静默")
- 消息延迟解耦:`Send.process` 改为**内部节流**——每 `PROCESS_SLICE_MS=100ms`
  检查一次输入队列(消息延迟上界 100ms),累计 1000ms 无输入则发 heartbeat。
  **不用 select**:实测 zio 的 `select` + `Channel.asyncReceive` 对 timer 分支
  不消费队列值(tick 永远在队列,每次 select 都立即成功),会导致心跳忙循环
  (曾实测 39 万次心跳/次连接)。内部节流绕开该 bug。
- 心跳不再依赖额外 fiber:`ClientContext.heartbeat` 通道与 `heartbeatClock` 已移除

**5.2.4 栈内存:配置栈池(P1,✅ 已实现)**

`Runtime.init(init.gpa, .{ .stack_pool = .{ .maximum_size = 8MB, .committed_size = 64KB } })`。
- server/client 各配 64KB 初始提交;压测客户端(跑完整协议)保持默认 256KB(栈深)
- `stack_pool.committed_size` 是**初始提交**而非上限,栈按需增长到 `maximum_size`(8MB)
- **每连接缓冲**:StreamChannel 4KB×2 + Mux wb/slots + inbox `[64]PushPayload` ≈ 25-30KB

**5.2.5 广播:逐连接 inbox → 共享板 + 游标(P2,✅ 已实现,架构升级)**

旧架构:Room fiber 遍历全部成员 `inbox.trySend`(O(N)/消息);
注册风暴的加入通知 `notifyAllExcept` 产生 O(N²) trySend 总量,
单连接 inbox 满 → fail-fast 断开 Push 通道(实测 1000 人同时注册仅 ~10% 存活)。

新架构(SharedBoard + 游标):
- 广播 = 一次 `board.append`(O(1))——聊天消息、加入/离开通知、`/who` 响应全部 append
- 每个连接的 Push 按自己的游标批量拉取(8 条/帧);无新消息时每 100ms 发 idle 帧
- 慢消费者只是游标落后,不会被断开
- 代价:消息延迟上界 100ms(轮询粒度);board 容量固定(1M 条,见 §9 不变式 #1)
- 行为变化:board 模式无"排除自己"——**发送者也会经板拉回自己的消息**(客户端本地
  也回显,二者一致,不是重复);`/who` 响应也会广播给所有人(demo 简化,可接受)

**5.2.6 容量参数(P1,✅ 已实现)**

| 参数 | 旧架构 | 实现 |
|---|---|---|
| Room ops 队列 | 64 | `[1024]RoomOp` |
| 每连接 inbox | 16 | `[64]PushPayload` |
| client_id / count | u8/u32 | u32 / usize |

### 5.3 关键参数(现状,单一事实来源)

| 参数 | 值 | 位置 |
|---|---|---|
| `BOARD_CAPACITY` | 1M 条(≈310MB,满板行为见 §9 #1) | server.zig |
| `BOARD_KEEP` | 1024 条(新连接水位) | protocol.zig |
| 心跳间隔 | 1000ms | protocol.zig |
| `PROCESS_SLICE_MS` | 100ms | protocol.zig |
| 服务器 Ctrl recv 超时 | 20s | server.zig |
| 客户端 Ctrl recv 超时 | 20s | client.zig |
| Room ops 队列 | 1024 | protocol.zig |
| 每连接 inbox(Push rb) | 16 槽(Mux 层) | server.zig |
| StreamChannel 缓冲 | 4096 × 2(读/写) | server.zig |
| `WHO_LIST_LIMIT` | 32 | protocol.zig |
| zio 运行时 | `.exact(1)` 单线程,栈 64KB 初始提交 | server.zig |

---

## 6. 内存总账

| 项目 | 估算 |
|---|---|
| fiber 栈(服务器 40000 × 64KB) | ≈ 2.5GB |
| 连接缓冲(10000 × ~28KB) | ≈ 0.3GB |
| SharedBoard(1M 条 × ~310B,预分配) | ≈ 0.3GB |
| Room 成员表(10000 条目) | ≈ 1-5MB |
| Mux 帧缓冲 / ops 队列 | < 10MB |
| **合计** | **≈ 3.1GB**(16-32GB 服务器可支撑 10000 同时在线) |

---

## 7. 设计决策与取舍

| 决策 | 理由 |
|---|---|
| 明文而非 TLS | demo 简化;生产可先跑库内置 tls 握手再建 TlsChannel(见 `src/protocol/tls/`) |
| 双协议 Mux 而非单协议轮询 | Push 真推送零轮询延迟;控制与推送互不阻塞 |
| 定长载荷 | codec 原生支持,wire 路径零分配;昵称/消息超长截断 |
| 掉线靠 Ctrl 心跳 | 客户端 1s 心跳 = 天然 liveness 信号;服务器 20s 超时踢人 |
| 服务器经板回显给发送者 | board 模式无"排除自己",客户端本地同时回显;省去逐连接过滤(见 §5.2.5 行为变化) |
| 消息延迟上限 ~200ms | 发送方轮询等待 ≤100ms + 接收方轮询 ≤100ms + RTT |
| 静默注册开关 | 大群/压测关闭加入通知,避免注册风暴的 O(N²) 加入通知流量(§3) |

---

## 8. 验证与压测

### 8.1 重现步骤(可重复执行)

```bash
cd /home/hk/my-zig/polyrole-cs

# 1. 构建全部产物(含 chat-loadtest)
zig build

# 2. 启动服务器(另开一个终端;看到监听日志即就绪)
zig-out/bin/chat-server 7788

# 3. 运行压测:10000 连接,心跳维持 6 秒
zig-out/bin/chat-loadtest 10000 127.0.0.1 7788 6

# 广播压测(服务器需以 --silent-join 重启;期望 10000×10 = 10 万条 100% 送达):
# zig-out/bin/chat-loadtest 10000 127.0.0.1 7788 15 10
```

期望输出:`完成:成功 10000,失败 0,总耗时 ~12000ms`。
压测结束后服务器 Room 应恢复为空(所有成员随 `/quit` 优雅移除,client_id 槽位可复用);
可再跑 `chat-loadtest 1000 ...` 验证可重复。

### 8.2 实测(本机 20 核 / 31GB,127.0.0.1;2025-08 复现一致)

**连接压测**(完整 Ctrl+Push 协议,注册后心跳维持,到期 `/quit` 优雅退出):

| 连接数 | 结果 | 总耗时 |
|---|---|---|
| 100 | 100/100 | 5.1s |
| 1000 | 1000/1000 | 7.3s |
| 5000 | 5000/5000 | 8.6s |
| **10000** | **10000/10000** | **12.1s** |

**广播压测**(`chat-server --silent-join`,客户端 0 连发 msgs 条,期望 N×msgs 全送达):

| 连接数 | 广播 | 送达率 | 首条延迟 | 末条延迟 |
|---|---|---|---|---|
| 100 | 10 条 × 100 | 100% | 4ms | 50ms |
| 1000(分批) | 10 条 × 1000 | 100% | 95ms | 216ms |
| **10000** | **10 条 × 10000** | **100%(10 万条)** | **113ms** | **4458ms** |

- 零失败;10000 人同时注册(silent-join 无加入风暴)+ 10 条消息广播全量送达
- 广播 = SharedBoard.append(O(1)),各连接按游标拉取,无断开
- 末条延迟 4.5s 是**服务器单线程推送 10 万条**的吞吐限制(~2.2 万条/s),
  首条 113ms 满足实时目标;多 executor 并行推送是后续扩展点
- 压测中发现的边界:共享板改造前,"逐连接 inbox + trySend"架构在 1000 人
  同时注册时因 O(N²) 加入通知触发慢消费者断开(仅 ~10% 存活);
  SharedBoard + 游标消除该问题(见 §5.2.5)

### 8.3 压测工具与参数

`tools/chat_loadtest.zig`(`chat-loadtest <N> [host] [port] [duration_s] [msgs] [batch_size]`):
建立 N 个真实 chat 客户端连接(完整 Ctrl+Push 协议),全部注册后维持心跳
duration 秒,然后优雅退出;`msgs > 0` 时客户端 0 连发 msgs 条消息并统计全量送达与延迟;
`batch_size < N` 时按批注册(每批后等待 2s)——旧架构或内存受限场景用,
SharedBoard 架构广播 = O(1) append,无需分批。

**环境前提与失败排查**

压测前自检(10000 连接):

```bash
ulimit -n              # 需 ≥ 20000(服务器 + 压测客户端各需 ~10000+ fd)
free -g                # 需 ≥ 16GB(10000 连接:服务器 ~3GB + 客户端 ~7.7GB)
ss -tln | grep 7788    # 目标端口应空闲(或确认旧服务器已停)
```

- **fd 配额(最常见失败)**:10000 连接需要服务器与压测客户端**各自** ~10000+
  fd;默认软限制 1024 时压测报 `error.ProcessFdQuotaExceeded`,已建连接被服务器
  强制关闭,表现为 `失败 ≈ 1000` + 总耗时极短。提升:`ulimit -n 65535`
  (当前会话生效,bash/fish 均支持;新终端需重设;服务器与压测需在同一
  提升后的会话中启动)
- **内存**:10000 连接约需 3GB 服务器端 + 客户端约 7.7GB(默认 256KB 栈),
  建议 16GB+ 机器;不足可先跑 5000(约 5.5GB 总量)
- **服务器进程保持**:服务器必须在**独立终端或后台任务**中运行——直接 `&`
  丢在命令里,命令退出会连带杀掉服务器(压测报 `ConnectionRefused`)
- **时序**:等服务器日志出现 `聊天室服务器监听` 再跑压测
- **推送必须被消费**:loadtest 内置 `drainPush` 消费收件箱;若自行改脚本不消费,
  注册风暴的加入通知会背压导致慢消费者被服务器断开(曾 100 连接 75% 失败)
- **广播压测**:服务器必须带 `--silent-join` 启动(否则注册风暴的加入通知挤占
  Push 通道,广播延迟统计失真);`msgs` 为第 5 个位置参数,>0 即开启
- **连续广播压测前重启服务器**:SharedBoard 只增不减,上一轮广播消息残留在
  board 上,新压测连接一建立就会拉到(混入统计,且首条收到时间早于本轮
  broadcast_start_ms——loadtest 已用饱和减法避免下溢,但 `实收` 会偏大)

失败排查:

| 现象 | 原因 | 处理 |
|---|---|---|
| `error.ConnectionRefused` + `成功 0 失败 0` | 服务器未运行 / 未就绪 / 端口不匹配 | `ss -tln` 确认 LISTEN;等监听日志;服务器只监听 `127.0.0.1`(IPv4),本机压测连 `127.0.0.1`;跨机/容器需端口映射 |
| `error.ProcessFdQuotaExceeded`,`失败 ≈ 1000` | fd 软限制 1024 耗尽 | `ulimit -n 65535` 后重跑(服务器同会话) |
| `失败 > 0` | 服务器端 `readFrame` 错误 / 慢消费者断开 | 临时在 `src/family_mux_channel.zig` readerLoop 加日志 |
| 服务器启动即 panic(core dump) | 端口被占,bind 失败后 deinit 断言(见 §9 不变式 #4) | 清理残留进程;换端口启动 |

---

## 9. 设计不变式(实现必须遵守,违反即 bug)

> 本节由设计审计(2025-08)逆推而来:以下每一条都是"设计文档当初未显式规定、
> 实现时被最自然假设、审计时发现问题"的边界。**新增代码前先对照本表。**

### #1 固定容量资源必须声明满时行为
- 现状:SharedBoard 容量固定 1M 条,满时 `appendAssumeCapacity` 断言——
  Debug 构建 panic(进程崩溃),Release 构建 assert 移除后为**越界写(UB)**。
- 要求:任何固定容量资源必须三选一——panic / 优雅拒绝 / 物理回收,并在本文档声明。
- 验证:满板行为测试(append 第 1M+1 条)。

### #2 每个阻塞点必须有超时或取消路径
- 现状:`symmetric_run` 的 AutoCancel 只守护 recv 分支,send 无守护;
  `push_h.join()` 无超时守护;`kick` 只能打断"检查点"(sleep 后/process 开头),
  **不能打断阻塞中的 send**。恶意客户端"持续发心跳 + 从不读 TCP"可让服务器
  该连接的 Ctrl/Push fiber 永久挂起(Writer 阻塞在 TCP 写)。
- 要求:任何协议 fiber 的清理路径必须可完成——send/join 需有守护超时或可取消。
- 验证:不读 TCP 的客户端测试(挂起检测)。

### #3 数据流生命周期必须绑定协议状态
- 现状:Push fiber 在 Ctrl 注册**之前** spawn,未注册连接即开始拉取 board
  (收到"注册完成前"的消息,最多 1024 条历史 + 实时)。
- 要求:新增数据流必须声明起点绑定哪个协议状态;注册前不产生数据流
  (或显式声明接受并说明理由)。
- 验证:注册前连接不应收到任何推送的测试。

### #4 每个 spawn 的 fiber 必须有停止契约
- 现状:`Room.stop()` 仅测试使用;服务器 main 错误路径(如 bind 失败)直接
  `rt.deinit()`,Room fiber 仍在运行 → `task_count != 0` 断言 panic(core dump)。
- 要求:错误路径与正常路径走同一清理链——`room.stop()` 必须先于 `room.deinit()`;
  任何 spawn 的 fiber 都要有明确的停止点。
- 验证:端口占用启动 → 进程应优雅报错退出而非 abort。

### #5 跨 fiber 共享的可变状态必须显式枚举
- 现状:共享状态清单——`SharedBoard.committed`(原子,release/acquire,正确)、
  `PushServerContext.kick`(**bool,非原子**)。zio 默认单线程执行器下无竞争,
  但 `executors = .auto`(文档扩展点)下 `kick` 是数据竞争(UB)。
- 要求:新增跨 fiber 共享状态必须入表:谁写谁读、原子性、内存序;
  开多 executor 前先原子化 `kick`。
- 验证:多 executor 构建下跑全量测试。

### #6 常量单一来源
- 现状:Mux 配置(容量/帧预算 4096/4100)、超时等常量在 server.zig / client.zig /
  test.zig 重复定义,改一处忘三处即协议不匹配(当前为手动同步)。
- 要求:参数定义处即文档;新增参数先查是否已有定义。
- 验证:常量引用检查。

### #7 慢消费者的两方向语义必须区分
- 现状:Push 方向(服务器→客户端)游标落后不断连——正确;
  但 Mux backpressure 是**通道级**而非连接级:客户端 Push 消费慢 → 客户端
  Mux Reader 阻塞 → **跨协议 HOL**,Ctrl 的 Ack 收不到 → 客户端 20s 超时自行断开。
- 要求:文档与代码注释表述慢消费者语义时必须区分这两个方向;
  通道级 HOL 是框架特性(`docs/family.md` 已承认),不是 bug。
- 验证:慢消费者测试(客户端不消费推送 → 20s 内连接被清理)。

---

## 10. 运行与测试

```bash
zig build

# 终端 1:服务器(默认端口 7788;--silent-join 关闭加入通知)
zig-out/bin/chat-server [port] [--silent-join]

# 终端 2/3:两个客户端
zig-out/bin/chat-client alice
zig-out/bin/chat-client bob
```

- 输入一行回车发送消息;`/who` 查看在线成员;`/quit` 退出
- 消息长度上限 255 字节,昵称上限 31 字节(超长截断)
- 客户端:服务器 20s 无响应判定连接死亡(Ctrl 心跳检测)

`zig build test` 覆盖(`examples/chat/test.zig`):

1. **Room 纯逻辑**:register/remove/broadcast(消息板断言)、`notify_joins` 关闭
2. **Ctrl simulate**:注册 → 欢迎 → 消息广播 → `/who` → 退出广播(消息板断言)
3. **双客户端网络集成**:加入/离开通知、消息互达(board 模式含发送者回显)、`/who`
4. **掉线检测**:客户端注册后断开,服务器清理并广播离开通知

---

## 11. 文件清单

```
examples/chat/
├── protocol.zig    # 两个协议状态机 + 上下文 + Room + 载荷
├── server.zig      # 服务器:accept loop + 监督 fiber + Room fiber
├── client.zig      # 客户端:Ctrl 主循环 + Push 消费 + stdin 异步读行
├── test.zig        # 上述四组测试
└── README.md       # 本文档(设计 + 推演 + 验证 + 不变式)
tools/chat_loadtest.zig  # 压测工具(见 §8.3)
```
