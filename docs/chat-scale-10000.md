# chat demo 10000 并发在线架构推演

> 本文档基于 `examples/chat/` 现状做**纯架构推演**，不修改任何代码。
> 目标：支撑 **10000 同时在线**（内存/连接管理优先，广播延迟允许百毫秒级）。
> 选型：心跳放宽、成员列表按需获取。

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

### 3.1 成员表：定长数组 → 动态结构（P0，硬限制）

**现状**：`slots: [64]Slot` + `allocSlot()` 线性扫描；client_id = 槽位索引。

**推演**：换成动态哈希表 / 动态数组（容量随连接数增长，10000+ 无上限）。
- client_id 从"槽位索引"改为**单调递增 ID + 哈希表查成员**（`HashMap(u32, Slot)`），或保留紧凑数组但 `std.ArrayList` 动态扩容
- `Room.count` 改为 `usize`（当前 u8 上限 255）
- 广播遍历从"定长数组 for"改为"哈希表 value 迭代"——O(N) 不变，但无容量上限
- **权衡**：哈希表 vs 紧凑数组——广播是热点（O(N) 遍历），紧凑数组缓存友好；推荐"动态扩容数组 + 空闲槽位链表复用"（兼顾缓存与扩容），哈希表作为备选。

### 3.2 Welcome 协议：全量成员表 → 按需获取（P0，wire 格式变化）

**现状**：`WelcomePayload { client_id, member_count, members: [64][32]u8 }` 全量下发。

**推演**：
- Welcome 只带 `client_id + member_count`（≈ 8B），**去掉定长成员数组**
- 成员列表改为**按需**：
  - Ctrl 协议新增 `Who` 变体（客户端 `/who` → 服务器回成员列表），或
  - 客户端用**增量维护**：已有的 Join/Leave 系统通知 + 注册时收到的初始列表
- 客户端 `ClientContext.members` 从定长快照改为动态列表（ArrayList）
- **权衡**：`/who` 的成员列表响应是 O(N) 大帧（10000 × 32B = 320KB）——只对单个请求方发送，可接受；增量维护省流量但客户端逻辑稍复杂。

### 3.3 心跳：放宽间隔 + select 唤醒解耦（P1，协议语义不变）

**现状**：`HEARTBEAT_INTERVAL_MS = 100`；`Send.process` 每轮 `tryReceive 输入队列 → 空则 sleep(100ms) → 发 heartbeat`。**输入检查周期 = 心跳周期**，放宽心跳会让消息延迟上界同步变大。

**推演**：
- 间隔 100ms → **1~2s**；服务器 Ctrl recv 超时 2s → **10~20s**（TCP 断开时 recv 立即报错，超时只兜底"活着但静默"的异常，无额外副作用）
- **关键：用 `zio.select` 解耦消息延迟与心跳间隔**——`Send.process` 改为同时等"输入队列"与"心跳定时器"：
  ```
  select(.{
      .input  = &input.asyncReceive(),   // 用户消息/quit 到达 → 立即发送
      .timer  = &sleep.asyncWait(),      // 间隔到期 → 发 heartbeat
  })
  ```
  消息延迟保持 **<1ms**（到达即发），心跳频率降到 **1/间隔**（1~2s 一次）。
- 流量：10000 连接 × 0.5~1 msg/s × 2（poll+ack）= **1~2 万帧/s**（降 10~20 倍）
- **权衡**：掉线通知延迟 2s → 10~20s（选型接受）；`Send.process` 引入 select 后状态机逻辑稍复杂（仍是 `!@This()`，协议形态不变）。

### 3.4 栈内存：配置栈池（P1，零代码结构变化）

**现状**：`zio.Runtime.init(allocator, .{})` 使用默认栈池（256KB committed）。

**推演**：`Runtime.init(allocator, .{ .stack_pool = .{ .committed_size = 64 * 1024, ... } })`——256KB → 64KB。
- chat 协议 fiber 栈深度浅（锁步状态机、无深递归），64KB 余量充足
- 40000 fiber × 64KB ≈ **2.5GB**（默认 10GB 的 1/4）
- **为什么不少砍 fiber**：4 fiber = 双协议（Ctrl/Push 必须并发，`symmetric_run` 一次驱动一个状态机）+ Mux 固有 Reader/Writer（单 Reader 保帧序、单 Writer 保帧原子性）。合并 Push 进监督 fiber 需放弃 `symmetric_run` 的编译期状态机校验，不推演为选项。
- **每连接缓冲**：StreamChannel 4KB×2 + Mux wb 4KB + slots（ctrl 1×4KB + push 16×512B） + inbox 16×310B ≈ **25-30KB** → 10000 连接 ≈ 250-300MB（次要项，可维持现状）

### 3.5 广播：保持串行，分片并行作扩展点（P2，默认不改）

**现状**：Room fiber 单线程遍历全部成员 trySend（默认单 executor 下无法并行）。

**推演**：
- **默认保持串行**：10000 人 1-2ms/广播，百毫秒目标内余量充足；全房间 100 msg/s 时 Room fiber 仅忙 10-20%
- **扩展点 A（并行广播）**：开启多 executor（`.executors = .auto`）+ 成员按 ID 分 N 片，每片一个广播 fiber 并行 trySend——把 O(N) 延迟除以核数。前提：成员表分片后一致性处理（register/remove 归属确定的分片），复杂度显著上升
- **扩展点 B（慢消费者降级）**：inbox 满时不再立即断开，改为"标记慢速 + 降低该连接广播优先级/丢系统通知"，网络抖动者免于被踢——需在 Room 增加慢速状态机
- **帧合并**：Mux Writer 已按协议合并多消息成帧，广播突发时自动批量化，无需改动

### 3.6 容量参数（P1，常量调整）

| 参数 | 现状 | 推演 |
|---|---|---|
| Room ops 队列 | 64 | 按连接数比例（如 `max(1024, N/4)`）或动态扩容 |
| 每连接 inbox | 16 | 按广播频率与心跳间隔配置（如 64~256），防慢消费者误杀 |
| client_id / count | u8/u32 | 全部 usize 化 |

## 4. 内存总账（推演后）

| 项目 | 估算 |
|---|---|
| fiber 栈（40000 × 64KB 配置） | ≈ 2.5GB |
| 连接缓冲（10000 × ~28KB） | ≈ 0.3GB |
| Room 成员表（10000 条目） | ≈ 1-5MB |
| Mux 帧缓冲 / ops 队列 | < 10MB |
| **合计** | **≈ 2.8-3.0GB**（16-32GB 服务器可支撑 10000 同时在线） |

## 5. 验证推演（本机压测思路）

- **连接数压测**：本机起 1 服务器 + 脚本并发建立 10000 条 TCP 连接并注册（用真实 chat 协议），监控：内存峰值（/proc/self/status VmRSS）、注册吞吐（每秒完成注册数）、失败率（RoomFull）
- **心跳流量**：`tcpdump`/`ss -s` 统计 10000 连接空闲期帧率（应与 1-2s 间隔匹配）
- **广播延迟**：客户端 A 发消息，采样 10000 客户端收到的时间分布（P50/P99）
- **僵尸清理**：kill 一批客户端 TCP，验证 10-20s 内服务器清理 + 广播离开
- 压测脚本为独立工具（tools/），不修改 chat 协议代码

## 6. 结论

10000 同时在线的核心障碍依次是：**成员表容量（P0）> 栈内存（P1）> 心跳流量与延迟耦合（P1）> 广播 O(N)（P2）**。P0+P1 完成后内存约 3GB、帧率约 2 万/s、消息延迟 <1ms、广播延迟 1-2ms/次，均满足"10000 在线 + 百毫秒广播"目标；P2 的并行分片与慢速降级留作流量增长后的扩展点。
