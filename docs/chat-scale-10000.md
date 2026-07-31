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

### 3.5 广播：保持串行，分片并行作扩展点（P2，默认不改）

**现状**：Room fiber 单线程遍历全部成员 trySend（默认单 executor 下无法并行）。

**推演**：
- **默认保持串行**：10000 人 1-2ms/广播，百毫秒目标内余量充足；全房间 100 msg/s 时 Room fiber 仅忙 10-20%
- **扩展点 A（并行广播）**：开启多 executor（`.executors = .auto`）+ 成员按 ID 分 N 片，每片一个广播 fiber 并行 trySend——把 O(N) 延迟除以核数。前提：成员表分片后一致性处理（register/remove 归属确定的分片），复杂度显著上升
- **扩展点 B（慢消费者降级）**：inbox 满时不再立即断开，改为"标记慢速 + 降低该连接广播优先级/丢系统通知"，网络抖动者免于被踢——需在 Room 增加慢速状态机
- **帧合并**：Mux Writer 已按协议合并多消息成帧，广播突发时自动批量化，无需改动

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

- 零失败；注册风暴（10000 并发注册）+ 1s 心跳维持 + 优雅退出
- 压测中发现的边界：loadtest 客户端若不消费推送收件箱，注册风暴的
  加入通知会塞满 inbox（容量 8）→ 背压 → 服务器按慢消费者断开（曾 100 连接
  75% 失败）；消费后 10000/10000 通过

## 6. 结论

10000 同时在线的核心障碍依次是：**成员表容量（P0）> 栈内存（P1）> 心跳流量与延迟耦合（P1）> 广播 O(N)（P2）**。P0+P1 已全部实现并经本机压测验证（**10000/10000 连接 12s 全成功、零失败**）：动态成员表 + 8B Welcome + `/who` 按需列表 + 1s 心跳（消息延迟上界 100ms）+ 64KB 栈 + ops/inbox 扩容。P2 的并行分片与慢速降级留作流量增长后的扩展点。
