# chat demo 10000 并发在线架构推演

> **状态：P0/P1 已按本推演实现，10000 并发压测通过**（见 §6）。
> 本文档保留推演过程供对照；"✅ 已实现"标注与最终实现的差异见 §3.3 说明。

目标：支撑 **10000 同时在线**（内存/连接管理优先，广播延迟允许百毫秒级）。
选型：心跳放宽、成员列表按需获取。

## 1. 现状架构与执行单元账目

每个客户端连接（服务器端）由 4 个 fiber 服务，另有 1 个全局 Room fiber：

```
服务器（每连接 4 fiber + 全局 1）
├─ 监督 fiber      驱动 Ctrl 协议（锁步：register/msg/heartbeat/quit）—— 连接生命线
├─ Push fiber      从本连接广播队列（inbox）取消息推送给客户端
├─ Mux Reader      （框架固有）顺序读帧、按段路由到各协议 rb
├─ Mux Writer      （框架固有）收集各协议 wb、合并成帧写出
└─ Room fiber      （全局共享）串行处理成员操作（register/remove/broadcast）
```

客户端侧 3 个执行单元（Ctrl 主循环、Push 消费、stdin 读行），与服务器性能无关。

### 关键参数（现状）

| 参数 | 值 | 位置 |
|---|---|---|
| `MAX_MEMBERS` | 64（定长数组 `[64]Slot`） | protocol.zig |
| `HEARTBEAT_INTERVAL_MS` | 100 | protocol.zig |
| 服务器 Ctrl recv 超时 | 2000ms | server.zig |
| Room ops 队列容量 | 64 | protocol.zig |
| 每连接 inbox 容量 | 16（`[16]PushPayload`） | server.zig |
| WelcomePayload | `[64][32]u8` 成员表 ≈ 2KB | protocol.zig |
| StreamChannel 缓冲 | 4096 × 2（读/写） | server.zig |

### zio 运行时事实（已从 zio v0.16.0 源码确认）

- `RuntimeOptions.executors` 默认 **`.exact(1)`（单线程事件循环）**，多线程需显式 `executors = .auto`（≤ 64）
- `RuntimeOptions.stack_pool` 默认：`maximum_size 8MB`（虚拟）、`committed_size 256KB`（初始提交物理）、`max_unused_stacks 1000`、`max_age 60s`
- 空闲 fiber 栈入池复用，超龄清理；**活跃 fiber 栈不可回收**
- `task_migration` 默认编译开启（多 executor 下任务可跨线程迁移）

## 2. 10000 并发瓶颈量化

| # | 瓶颈 | 现状量级 | 10000 连接 | 性质 |
|---|---|---|---|---|
| 1 | 成员表定长 | `[64]Slot` | **10000 人无法注册（RoomFull）** | 硬限制 |
| 2 | fiber 栈内存 | 4 × 256KB/连接 | 40000 × 256KB ≈ **10GB**（默认配置） | 内存 |
| 3 | 广播串行 O(N) | 每次广播遍历全部成员 trySend | 10000 × ~100-200ns ≈ **1-2ms/广播** | 延迟 |
| 4 | 心跳流量 | 100ms/连接 | 10 万 poll/s + 10 万 ack/s = **20 万帧/s** | syscall/中断 |
| 5 | 消息延迟耦合 | Send.process sleep(interval) 后才查输入 | 放宽心跳 → 消息延迟上界同步变大 | 协议耦合 |
| 6 | ops 队列 64 | 注册/离开入队 | 连接风暴（万级同时建立）时阻塞 | 易修 |
| 7 | inbox 16 | 广播高峰慢消费者断开 | 网络抖动者被频繁踢 | 稳定性 |
| 8 | WelcomePayload 2KB | 全量成员表 | 10000 人全量 = **320KB**/欢迎包 | wire 膨胀 |

## 3. 分方向推演

### 3.1 成员表：定长数组 → 动态结构（P0，✅ 已实现）

**现状**：`slots: [64]Slot` + `allocSlot()` 线性扫描；client_id = 槽位索引。

**实现**：`std.ArrayList(Slot)` 动态扩容 + `std.ArrayList(u32)` 空闲槽位栈（O(1) 复用）；
client_id = 槽位索引（u32），删除的槽位进 free 列表；`count: usize`。
- 广播仍为 O(N) 遍历 `slots.items`，紧凑数组缓存友好
- `Room.init(allocator)` + `Room.deinit()`

### 3.2 Welcome 协议：全量成员表 → 按需获取（P0，✅ 已实现）

**现状**：`WelcomePayload { client_id, member_count, members: [64][32]u8 }` 全量下发。

**实现**：
- `WelcomePayload` 只带 `client_id + member_count`（8B），去掉定长成员数组
- 成员列表按需获取：Ctrl 新增 `Send.who` 变体（客户端 `/who` 命令），
  服务器经 Room 查询（`MemberListReply`，截断 `WHO_LIST_LIMIT=32` 个名字）后
  走 **Push 通道**回 `kind=member_list` 响应（避免锁步通道的大帧阻塞）
- 客户端 `ClientContext` 移除定长成员快照，只维护在线人数

### 3.3 心跳：放宽间隔 + 消息延迟解耦（P1，✅ 已实现，方案有修正）

**现状**：`HEARTBEAT_INTERVAL_MS = 100`；`Send.process` 每轮 `tryReceive 输入队列 → 空则 sleep(100ms) → 发 heartbeat`。**输入检查周期 = 心跳周期**，放宽心跳会让消息延迟上界同步变大。

**实现**：
- 间隔 100ms → **1000ms**；服务器 Ctrl recv 超时 2s → **20s**（TCP 断开时 recv 立即报错，超时只兜底"活着但静默"）
- **消息延迟解耦（原推演用 `zio.select`，实现时发现 zio bug 而修正）**：
  `Send.process` 改为**内部节流**——每 `PROCESS_SLICE_MS=100ms` 检查一次输入队列
  （消息延迟上界 100ms），累计 1000ms 无输入则发 heartbeat（liveness + 锁步填充）。
  **不用 select**：实测 zio 的 `select` + `Channel.asyncReceive` 对 timer 分支
  **不消费队列值**（tick 永远在队列，每次 select 都立即成功），会导致心跳忙循环
  （曾实测 39 万次心跳/次连接）。内部节流绕开该 bug，消息延迟 100ms 对聊天足够。
- 心跳不再依赖额外 fiber：`ClientContext.heartbeat` 通道与 `heartbeatClock`
  已移除，客户端执行单元从 3 降到 2（Ctrl + Push + stdin）

### 3.4 栈内存：配置栈池（P1，✅ 已实现）

**现状**：`zio.Runtime.init(allocator, .{})` 使用默认栈池（256KB committed）。

**实现**：`Runtime.init(init.gpa, .{ .stack_pool = .{ .maximum_size = 8MB, .committed_size = 64KB } })`。
- server/client 各配 64KB 初始提交；压测客户端（跑完整协议）保持默认 256KB（栈深）
- 注意：zio 的 `stack_pool.committed_size` 是**初始提交**而非上限，
  栈按需增长到 `maximum_size`（8MB），64KB 只是降低初始内存占用
- **每连接缓冲**：StreamChannel 4KB×2 + Mux wb/slots + inbox `[64]PushPayload` ≈ 25-30KB

### 3.5 广播：逐连接 inbox → 共享板 + 游标（P2，✅ 已实现，架构升级）

**现状（旧）**：Room fiber 遍历全部成员 `inbox.trySend`（O(N)/消息）；
注册风暴的加入通知 `notifyAllExcept` 产生 O(N²) trySend 总量，
单连接 inbox 满 → fail-fast 断开 Push 通道（实测 1000 人同时注册仅 ~10% 存活）。

**实现（SharedBoard + 游标，借鉴早期 chat 版本的"无锁读/有锁写"设计）**：
- **广播 = 一次 `board.append`（O(1)）**——聊天消息、加入/离开通知、`/who` 响应
  全部 append 到跨连接共享的 `SharedBoard`（预分配永不搬迁 + 原子 `committed` 游标）
- **每个连接的 Push 协议按自己的游标批量拉取**（Poll/Chunk，PUSH_CHUNK=8/帧）；
  无新消息时每 `PUSH_POLL_MS=100ms` 发 idle 帧
- **慢消费者只是游标落后，不会被断开**——消除逐连接 trySend 的 O(N) 广播
  与 O(N²) 加入通知风暴
- **代价**：消息延迟上界 100ms（轮询粒度）；board 容量固定（1M 条，
  消息只增不减，物理回收与无锁读冲突，留作后续）
- **行为变化**：board 模式无"排除自己"——发送者也会经板拉回自己的消息，
  `/who` 响应也会广播给所有人（demo 简化，可接受）
- **静默注册**：`chat-server --silent-join` 关闭加入/离开通知（大群或压测用），
  压测 10000 同时注册时 board 只存 chat 消息，无风暴

**扩展点（保留）**：多 executor 并行推送（单线程推送 10 万条 ~4.5s，
首条 113ms 已达标，全量吞吐受单线程限制）；board 环形物理回收。

### 3.6 容量参数（P1，✅ 已实现）

| 参数 | 现状 | 实现 |
|---|---|---|
| Room ops 队列 | 64 | `[1024]RoomOp` |
| 每连接 inbox | 16 | `[64]PushPayload` |
| client_id / count | u8/u32 | u32 / usize |

## 4. 内存总账（推演后）

| 项目 | 估算 |
|---|---|
| fiber 栈（服务器 40000 × 64KB） | ≈ 2.5GB |
| 连接缓冲（10000 × ~28KB） | ≈ 0.3GB |
| Room 成员表（10000 条目） | ≈ 1-5MB |
| Mux 帧缓冲 / ops 队列 | < 10MB |
| **合计** | **≈ 2.8-3.0GB**（16-32GB 服务器可支撑 10000 同时在线） |

## 5. 验证推演（本机压测）

`tools/chat_loadtest.zig`（`zig build` 产物 `chat-loadtest`）：
- 建立 N 个真实 chat 客户端连接（完整 Ctrl+Push 协议），注册后心跳维持 duration 秒，
  到期投递 `/quit` 优雅退出；打印成功/失败数与总耗时

**实测（本机 20 核 / 31GB，127.0.0.1）**：

| 连接数 | 结果 | 总耗时 |
|---|---|---|
| 100 | 100/100 | 5.1s |
| 1000 | 1000/1000 | 7.3s |
| 5000 | 5000/5000 | 8.6s |
| **10000** | **10000/10000** | **12.1s** |

**广播压测（SharedBoard 架构，`chat-server --silent-join`）**：

| 连接数 | 广播 | 送达率 | 首条延迟 | 末条延迟 |
|---|---|---|---|---|
| 100 | 10 条 × 100 | 100% | 4ms | 50ms |
| 1000（分批） | 10 条 × 1000 | 100% | 95ms | 216ms |
| **10000** | **10 条 × 10000** | **100%（10 万条）** | **113ms** | **4458ms** |

- 零失败；10000 人同时注册（silent-join 无加入风暴）+ 10 条消息广播全量送达
- 广播 = SharedBoard.append（O(1)），各连接按游标拉取，无断开
- 末条延迟 4.5s 是**服务器单线程推送 10 万条**的吞吐限制（~2.2 万条/s），
  首条 113ms 满足实时目标；多 executor 并行推送是后续扩展点
- 压测中发现的边界：共享板改造前，"逐连接 inbox + trySend"架构在 1000 人
  同时注册时因 O(N²) 加入通知触发慢消费者断开（仅 ~10% 存活）；
  SharedBoard + 游标消除该问题（见 §3.5）

### 重现步骤（可重复执行）

```bash
cd /home/hk/my-zig/polyrole-cs

# 1. 构建全部产物（含 chat-loadtest）
zig build

# 2. 启动服务器（另开一个终端；看到监听日志即就绪）
zig-out/bin/chat-server 7788

# 3. 运行压测：10000 连接，心跳维持 6 秒
zig-out/bin/chat-loadtest 10000 127.0.0.1 7788 6
```

期望输出：

```
info: 压测：10000 连接 → 127.0.0.1:7788，心跳维持 6s
info: 完成：成功 10000，失败 0，总耗时 ~12000ms
```

压测结束后服务器 Room 应恢复为空（所有成员随 `/quit` 优雅移除，
client_id 槽位进空闲列表可复用）；可再跑 `chat-loadtest 1000 ...` 验证可重复。

### 广播压测（可选，msgs > 0，建议 `--silent-join`）

`chat-loadtest <N> ... <msgs> [batch_size]`——客户端 0 连发 msgs 条消息，
所有 N 个客户端统计收到的 chat 消息（board 模式含发送者回显，期望 N*msgs）：

```bash
# 服务器：静默注册（大群/压测关加入通知，避免注册风暴刷板）
zig-out/bin/chat-server 7788 --silent-join

# 10000 连接 + 10 条广播（共享板 O(1) 广播，全量送达）
zig-out/bin/chat-loadtest 10000 127.0.0.1 7788 15 10
```

实测（本机 20 核 / 31GB）：见 §5 广播压测表（10000 人 10 万条 100% 送达）。

**旧架构边界（已由 SharedBoard 解决）**：共享板改造前，"逐连接 inbox + trySend"
架构下同时注册 N 人产生 O(N²) 加入通知风暴，单连接 inbox 满 → 慢消费者
fail-fast 断开（1000 人同时注册仅 ~10% 存活）。改造后广播 = O(1) append +
游标拉取，无断开；`--silent-join` 压测 10000 人广播 100% 送达（见 §3.5）。

### 参数与缩放

| 参数 | 位置 | 说明 |
|---|---|---|
| 连接数 | 第 1 个 | 建议按 100 → 1000 → 5000 → 10000 递增观察 |
| host / port | 第 2、3 个 | 默认 `127.0.0.1:7788` |
| duration_s | 第 4 个 | 心跳维持秒数（6~10）；到期压测客户端投递 `/quit` 优雅退出 |
| msgs | 第 5 个 | 广播测试：客户端 0 连发的消息数（0 = 不广播） |
| batch_size | 第 6 个 | 分批注册的批大小（< 连接数时分批；默认不分批） |

### 注意事项

- **内存**：10000 连接约需 3GB 服务器端 + 客户端约 7.7GB（默认 256KB 栈），
  建议 16GB+ 机器；若内存不足可先跑 5000（约 5.5GB 总量）
- **推送必须被消费**：loadtest 内置 `drainPush` 消费收件箱；若自行改脚本不消费，
  注册风暴的加入通知会背压导致慢消费者被服务器断开（曾 100 连接 75% 失败）
- **端口**：确保 7788 未被占用；服务器日志出现 `聊天室服务器监听` 再跑压测
- **失败排查**：`失败 > 0` 时查看服务器端是否有 `readFrame` 错误日志（当前无日志，
  可临时在 `src/family_mux_channel.zig` readerLoop 加打印）

## 6. 结论

10000 同时在线的核心障碍依次是：**成员表容量（P0）> 栈内存（P1）> 心跳流量与延迟耦合（P1）> 广播 O(N)（P2）**。P0+P1 已全部实现并经本机压测验证（**10000/10000 连接 12s 全成功、零失败**）：动态成员表 + 8B Welcome + `/who` 按需列表 + 1s 心跳（消息延迟上界 100ms）+ 64KB 栈 + ops/inbox 扩容。P2 的并行分片与慢速降级留作流量增长后的扩展点。
