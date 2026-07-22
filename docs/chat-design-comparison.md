# 聊天室设计对比：标准方案 vs polyrole-cs

| 维度 | 标准方案（文本协议 + 线程 + channel） | polyrole-cs（状态机 + Mux + SharedBoard） |
|------|--------------------------------------|------------------------------------------|
| | | |
| **并发模型** | 每连接两个线程：读线程（解析协议+回复OK）和写线程（阻塞在 send_chan 上推送）。两层竞争：读线程和写线程争同一个 TCP socket 的写锁；N 个连接的读线程争 MessageBoard 的写锁 | 每连接三个 fiber：Init、Chat、Push。三个 fiber 各自阻塞在 Mux 的不同子通道上，零交叉竞争。SharedBoard 的写锁仅在 Chat.preprocess 内持有，读端无锁 |
| | | |
| **协议表达** | 文本协议 `JOIN alice\n` `SEND hello\n` `PUSH alice hello\n`，解析器手写状态机（隐式） | 每个状态是 `union(enum)`，转移路径是 `Data(payload, NextState)`。编译期验证所有路径连通性、角色一致性、context 类型匹配 |
| | | |
| **发送 OK 和推送 PUSH 的关系** | 读线程回复 OK 时拿 `write_mu` 写 socket，写线程推送 PUSH 时也拿同一把锁——两个 agent 争一个信道 | Chat 的 Ack（OK）走 Mux channel-1，Push 的 items 走 Mux channel-2。Mux 在帧级串行化，协议层零竞争 |
| | | |
| **消息分发** | `for conn in connections { conn.send_chan.send(msg) }` — O(N) 遍历。send_chan 是有限容量的 mpsc buffer，满了就阻塞读线程 | Chat 写 SharedBoard（O(1)），Push 各自快照 board[cursor..committed]，无锁读。分发是拉取模型，零遍历 |
| | | |
| **背压处理** | send_chan 满 → 要么阻塞读线程（连锁反应），要么丢消息（静默） | CHUNK_SIZE 限制单帧大小，Mux 公平调度。写不阻塞读 |
| | | |
| **队头阻塞** | 如果一次 PUSH 10000 条消息（历史同步），写线程独占 socket，OK 回复被卡在锁后面 | Mux 帧级交替，Push 的 Chunk 和 Chat 的 Ack 交错发送。CHUNK_SIZE 保证单帧 ≤ 8 条消息 |
| | | |
| **Socket 写竞争** | 存在——读线程和写线程都可能写 socket | 不存在。所有发送走 Mux → writer fiber，天然串行化 |
| | | |
| **内存管理** | recv buffer → 解析 → 分配字符串 → 放入 HashMap / Vec → 手动 free。谁分配谁释放需要文档约定 | 每个状态明确：dupe 发生在 preprocess 内，defer free 在 test 的 cleanup 中。但资源所有权仍是手动管理，框架未抽象 |
| | | |
| **错误处理** | 解析错误 → 断开连接。send_chan 满 → 阻塞或丢消息。HashMap OOM → crash | 协议层：OOM → `catch return`（静默丢消息，已知问题）。连接断开 → `error.ChannelClosed` → 优雅退出 |
| | | |
| **测试** | Mock socket / channel，注入字节流，验证输出。端到端测试需要真实 TCP 或 async runtime | `symmetric_run` 直接在内存中驱动状态机。测试不需要真实 TCP，也不需要 `testing.io`。但 Mux 测试仍需 TCP |
| | | |
| **类型安全** | 无——消息格式是 `"PUSH alice hello\n"`，拼写错误运行时才发现 | 编译期：状态转移类型错误 → 编译失败。context 类型不匹配 → 编译失败。`reachableStates` DFS 验证全图 |
| | | |
| **新增协议** | 修改解析器 + 新增处理分支。同一连接上多个对话流（Chat + Push）需要额外的 channel_id 字段，手写 demux 逻辑 | 新协议 = 新文件 + 新 SubChannel。Mux 自动分派。框架验证上下文一致性和角色关系 |
| | | |
| **设计文档和代码的关系** | 设计文档描述协议格式和线程模型。代码实现可能偏离文档——没有编译期约束强制一致性 | 协议的类型定义就是设计文档。`Data(payload, NextState)` 同时是设计表达和运行时代码。不可偏离 |
| | | |
| **学习曲线** | 低——线程、锁、channel 是通用概念。代码冗长但直觉 | 高——必须理解 `Data`、`ProtocolInfo`、`Runner`、`symmetric_run`、Mux、state map 的编译期推导。但一旦理解，协议代码极其紧凑 |
| | | |
| **运行时开销** | 系统线程切换（24 线程 for 3 连接的 chat 室），syscall 密集 | zio fiber（用户态），Mux 使用原子操作的内存 ring buffer。send/recv 路径零 syscall（由独立 writer/reader fiber 批量处理） |
| | | |

## 关键分歧点：shared state 的并发模型

```
标准方案：                    polyrole-cs：

读线程1 ─┐                     Chat fiber1 ─→ board.append ─┐
读线程2 ─┼→ write_mu ─→ board   Chat fiber2 ─→ board.append ─┼→ mu ─→ board
读线程3 ─┘                     Chat fiber3 ─→ board.append ─┘

写线程1 ←─ send_chan1          Push fiber1 ←─ committed.load
写线程2 ←─ send_chan2          Push fiber2 ←─ committed.load
写线程3 ←─ send_chan3          Push fiber3 ←─ committed.load
    ↑                               ↑
遍历推送 ← 每收到一条消息          各自拉取 ← 定期自检
```

标准方案的推送是**中心化广播**——on_message 遍历所有连接推。polyrole-cs 的推送是**去中心化拉取**——每个 Push fiber 自己看 board 有没有新数据。

## 关键分歧点：并发对话的隔离

```
标准方案：同一个 TCP socket 上，回复 OK 和推送 PUSH 是两个线程竞争一把写锁。

polyrole-cs：同一个 TCP socket 上，Chat Ack 走 channel-1，Push items 走 channel-2。
Mux 在帧级串行化——两个 agent 不需要知道对方存在。
```

这不是"Mux 解决了锁竞争"——是框架先要求你把 Chat 和 Push 定义为两个独立状态机，然后 Mux 是实现两个状态机共享一根线的机制。设计先行，机制随后。

## 结论

标准方案更短平快——一个下午能写出来跑通。polyrole-cs 方案前期投入高——必须理解框架、定义状态图、通过编译期验证。但生产环境中，标准方案的"隐含假设"（线程安全、背压、队头阻塞、内存管理）会在压力下逐一暴露，修复成本非线性增长。polyrole-cs 的约束让你一开始就必须面对这些问题，代价是前期设计时间，收益是运行时零意外。
