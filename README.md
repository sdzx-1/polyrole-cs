# polyrole-cs

一个 Zig 编译期状态机协议框架，专用于 **Client-Server** 通信模型。

协议定义为 tagged union 的状态图，框架在编译期完成可达性分析、上下文类型校验和状态 ID 生成，
运行时零开销 dispatch。提供内存模拟和网络通道两种执行模式。

```zig
const polyrole = @import("polyrole_cs");
```

## 项目结构

```
polyrole-cs/
├── src/                          # 核心库
│   ├── root.zig                  # 模块入口，导出所有公共组件
│   ├── runner.zig                # 状态机驱动（simulate / symmetric_run）+ Mux 多路复用传输层
│   ├── channel.zig               # 通道抽象（StreamChannel / InMemoryChannel / TlsChannel）
│   ├── codec.zig                 # 二进制编解码
│   ├── Graph.zig                 # DOT 格式状态图生成
│   ├── test/                     # 全部测试（zig build test 编译此目录）
│   │   ├── test.zig              # 测试根
│   │   ├── codec_test.zig
│   │   ├── channel_test.zig
│   │   ├── runner_test.zig
│   │   ├── tls_test.zig
│   │   └── net_monitor_test.zig
│   └── protocol/                 # 协议实现
│       ├── tls.zig               # 简易加密握手协议（模块入口）
│       ├── tls/                  # 内部实现
│       │   ├── root.zig
│       │   ├── context.zig       # 握手上下文 + 密钥协商
│       │   ├── README.md
│       │   └── design.md
│       ├── net_monitor.zig       # 网络延迟探测协议（模块入口）
│       └── net_monitor/          # 内部实现
│           ├── root.zig
│           ├── context.zig
│           ├── design.md
│           └── design_cn.md
├── desigen.md                    # Mux 设计笔记
├── build.zig                     # 构建配置
├── build.zig.zon                 # 依赖声明
└── README.md
```

## 核心概念

### 状态（State）

每个状态是一个 tagged union，通过 `info` 声明所有权和协议元信息：

```zig
pub const ClientHello = union(enum) {
    to_server: polyrole.Data(ClientHelloPayload, ServerHello),

    pub const info: MyProtocol = .{ .agent = .client, .name = "ClientHello" };

    pub fn process(ctx: *ClientCtx) !@This() { ... }
    pub fn preprocess(ctx: *ServerCtx, result: @This()) !void { ... }
};
```

- **`info`** — 声明 agent（`.client` / `.server`）和协议名。
- **`process(ctx)`** — agent 端执行，产生转移数据。
- **`preprocess(ctx, result)`** — 对端接收数据，验证并更新上下文。

### 转移（Data）

每个 union variant 携带一个 `Data(Payload, NextState)`，声明数据载荷和下一个状态：

```zig
to_server: polyrole.Data(ClientHelloPayload, ServerHello),
//                                  ^载荷              ^下一状态
```

### 协议信息（ProtocolInfo）

声明协议名和双方上下文类型，编译期校验跨状态上下文一致性：

```zig
const MyProtocol = polyrole.ProtocolInfo("my_proto", ClientCtx, ServerCtx);
```

### 退出

所有协议的最终状态是 `polyrole.Exit`——框架内置的唯一终止状态。

---

## 状态机示例

一个简单的 Client-Server 计数器协议：

```zig
const std = @import("std");
const polyrole = @import("polyrole_cs");

const Info = polyrole.ProtocolInfo("counter", i32, i32);

const Counter = struct {
    pub const B = union(enum) {
        to_a: polyrole.Data(void, A),
        done: polyrole.Data(void, polyrole.Exit),
        pub const info: Info = .{ .agent = .server, .name = "B" };
        pub fn process(ctx: *i32) @This() {
            if (ctx.* >= 10) return .done;
            ctx.* += 1;
            return .to_a;
        }
    };

    pub const A = union(enum) {
        add: polyrole.Data(void, B),
        pub const info: Info = .{ .agent = .client, .name = "A" };
        pub fn process(ctx: *i32) @This() { _ = ctx; return .add; }
    };
};

const A = Counter.A;
```

---

## 执行模式

### simulate — 单线程内存模拟

双端在同一线程交替执行，零序列化开销，适合单元测试：

```zig
const R = polyrole.runner.Runner(A);
var client: i32 = 0;
var server: i32 = 0;
try R.simulate(&client, &server, A);
// server == 10
```

### symmetric_run — 通过 Channel 网络执行

单端驱动，另一端通过网络收发：

```zig
// Server 端
var ch: StreamChannel = undefined;
try ch.init(allocator, stream, 256, 256, 256);
defer ch.deinit(allocator);
try R.symmetric_run(.server, &server_ctx, &ch, A, null);

// Client 端（并发任务中）
try R.symmetric_run(.client, &client_ctx, &ch, A, null);
```

### Mux — 多路复用传输层（核心特性）

**Mux 是 polyrole-cs 的核心亮点**：多个协议共享一条底层连接，每个协议跑独立的
状态机——独立缓冲、独立超时、互不阻塞，而传输成本与单协议几乎相同。

它解决的是真实系统的典型问题：一个连接往往要承载多种语义（如控制面 + 推送面、
信令 + 数据），逐一独占连接浪费资源，手工拼帧又容易出错。Mux 把"多路复用"
变成框架能力，与 `simulate`（单机模拟）、`symmetric_run`（单协议端到端）并列，
构成第三种执行形态：

```
simulate        单机内存模拟（零序列化，测逻辑）
symmetric_run   单协议端到端（一条连接一个协议）
Mux             多协议端到端（一条连接 N 个协议，可选加密）
```

**机制**：每个协议一个 `SubChannel`（独立 `send_buff`/`recv_buff`，
大小 = `max_massage_size + 3`）。发送许可机制保证同一时刻每协议至多一条消息在途；
`writer_loop` 把一轮聚合的帧拷贝进连续缓冲，一次 `writeAll` 写出（聚合写）；
`reader_loop` 读入整批后按帧头切分分发到各协议。

**wire format（批记录）**：`[total_len u32 BE][帧...]`，每帧 `[protocol_id u8][payload_len u16 BE][payload]`。

**用法**——`Protocol` 声明协议族，comptime 决定是否加密：

```zig
const Mux = polyrole.runner.Mux;

const protocols = [_]polyrole.runner.Protocol{
    .{ .enter = Ctrl.A, .runner = R_ctrl, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
    .{ .enter = Push.A, .runner = R_push, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
};
```

**明文模式**（`encrypt = false`）——批明文直接写底层流，`keys` 传 `null`：

```zig
const TmpMux = Mux(&protocols, false);
var mux: TmpMux = undefined;
try mux.init(gpa, &sc, null);
try mux.run(.client, ctxs);   // ctxs 是各协议 context 的元组
```

**加密模式**（`encrypt = true`）——整批明文作为一条 AEAD 记录加密，密钥来自
TLS 握手（或任何带外协商）：

```zig
const TmpMux = Mux(&protocols, true);
var mux: TmpMux = undefined;
try mux.init(gpa, &sc, .{ .write_key = wk, .read_key = rk });
try mux.run(.server, ctxs);
```

加密模式下每条批记录 `[total_len u32][帧...]` 整体被 AEAD 认证；记录长度已升级为
u32，批明文可超过 64 KiB。密钥轮换同样生效——KeyUpdate 记录在 Mux 层被透明吸收。

`SubChannel` 接口与 `StreamChannel` 完全一致（`send`/`recv`），协议代码零改动；
`SubChannel.recv` 超时由 `Protocol.recv_timeout_ms` 声明，行为与 `symmetric_run` 相同。

---

## 模块

| 模块 | 路径 | 说明 |
|------|------|------|
| `runner` | `src/runner.zig` | 状态机驱动：`simulate()`、`symmetric_run()`；多路复用传输层 `Mux()` |
| `channel` | `src/channel.zig` | 通道抽象：`StreamChannel`（明文 TCP）、`InMemoryChannel`（进程内管道）、`TlsChannel`（AEAD 加密 + 密钥轮换） |
| `codec` | `src/codec.zig` | 二进制编解码：状态 ID + tag + payload |
| `Graph` | `src/Graph.zig` | DOT 格式状态图生成 |
| `tls` | `src/protocol/tls/` | 简易加密握手协议（示例） |
| `net_monitor` | `src/protocol/net_monitor/` | 网络延迟探测协议（示例） |

### Channel

**StreamChannel** — 明文 TCP 通道：

```zig
var ch: StreamChannel = undefined;
try ch.init(allocator, stream, read_buf_size, write_buf_size, max_slice_len);
defer ch.deinit(allocator);
```

**InMemoryChannel** — 进程内全双工管道（两个配对的 `HalfChannel` 交叉引用），
不经过网络 I/O，每个方向至多一条消息在途。

**TlsChannel** — AEAD 加密通道，在 StreamChannel 之上使用 NaCl SecretBox：

```zig
var tc: TlsChannel = undefined;
try tc.init(allocator, &sc, write_key, read_key, 512);
defer tc.deinit(allocator);
```

每条记录 wire format：`nonce(24) || tag(16) || ct_len(4 BE) || ciphertext`，
nonce = `counter(8 BE) || 0(15) || type(1)`——type 字节被 AEAD 认证，记录类型
（数据 / KeyUpdate）不可篡改。

**密钥轮换**（TLS 1.3 KeyUpdate 同构，`setRotationConfig` 可配置）：
- 发送侧在 `sealAndSend` 入口懒检查触发条件——写记录数阈值（默认 2^28，AEAD 数学安全硬性）
  或时间间隔（默认 10 分钟，空闲连接不产生空轮换包）；
- 触发时用当前密钥发一条 KeyUpdate 记录（明文 = 新 epoch），然后本地 HKDF 单向派生
  下一代写密钥并把写计数器归零（nonce 不重用）；
- 接收侧按序读到 KeyUpdate 后派生对应读密钥并归零读计数器，记录被**透明吸收**，
  上层（Mux / symmetric_run / 协议状态机）完全无感知。

**Mux** — 多路复用传输层（`src/runner.zig`），是框架的核心特性：
多个协议共享一条底层连接，可选批记录加密。用法与机制详见上文「执行模式 → Mux」。

### Codec

二进制序列化格式：

```
state_id(4 BE) || tag(1) || payload
```

支持类型：`void`、`bool`、整数、`[]const u8`、定长字节数组、struct。

### Graph

生成 DOT 格式状态图：

```zig
var graph = try polyrole.Graph.initWithFsm(allocator, P.A);
defer graph.deinit();
try graph.generateDot(.{}, &writer.interface);
```

用 Graphviz 渲染：`dot -Tpng graph.dot -o graph.png`

## 文档

| 文档 | 说明 |
|------|------|
| `README.md` | 本文件：框架概览与使用指南 |
| `desigen.md` | Mux 传输层的设计笔记（wb/rb 缓冲模型、frame 聚合约束） |
| `src/protocol/tls/README.md` | 简易 TLS 握手协议设计（中文） |
| `src/protocol/tls/design.md` | 简易 TLS 握手协议设计（英文） |
| `src/protocol/net_monitor/design.md` / `design_cn.md` | 网络延迟探测协议设计 |

### 测试

全部测试位于 `src/test/`（`zig build test` 编译运行）：

```bash
zig build test
```

| 文件 | 覆盖 |
|------|------|
| `codec_test.zig` | 编解码畸形输入（非法布尔、越界 tag、超长切片） |
| `channel_test.zig` | AEAD 错误路径（重放/篡改/长度）、密钥轮换、内存通道全双工 |
| `runner_test.zig` | simulate / symmetric_run / 超时 / TLS 加密通道 / Mux 明文+加密 |
| `tls_test.zig` | 握手协议（签名/MAC/临时公钥篡改、重放、会话隔离、跨会话 ClientFinished 重放、MAC 域分离） |
| `net_monitor_test.zig` | 网络延迟探测协议模拟与对称运行 |

---

## 错误处理

状态函数可以返回 error union。Runner 在编译期检测返回类型：
- 返回 `@This()` → 普通调用，继续协议。
- 返回 `!@This()` → `try` 调用，错误直接传播给调用方，协议立即终止。

```zig
pub fn process(ctx: *Ctx) !@This() {
    const key = loadKey() catch return error.KeyNotFound;
    // ...
}
```

---

## 简易加密握手协议（示例，单向认证）

项目包含一个自定的简易加密握手实现，展示了 polyrole-cs 的完整用法。

**前提**：Client 预先知道（带外/信任锚/证书固定）Server 的 Ed25519 公钥；Server **无需知道** Client 的身份——单向认证（HTTPS 模型），Server 可同时服务任意数量的客户端，客户端零密钥管理。

**握手流程**：三条消息完成密钥协商和 Server 认证，不包含数据阶段。

- `ClientHello → ServerHello → ClientFinished → Exit`
- X25519 临时密钥协商（ephemeral-ephemeral，提供前向安全）
- Server 的 Ed25519 身份签名 + 双方 HMAC（ClientFinished 为会话持有证明，不携带 Client 身份）
- Transcript 链式 SHA256 哈希防篡改（Server 签名覆盖双方临时公钥，MITM 无法替换）
- HKDF-SHA256 派生三把独立密钥

**安全边界**（HTTPS 模型，与标准 TLS 单向认证一致）：

| 提供 | 不提供 |
|------|--------|
| Server 身份认证（防 MITM 冒充 Server） | Client 身份认证（Client 匿名） |
| 机密性 / 完整性 / 防重放 / PFS | 防"陌生 Client 自报身份"——需要身份认证时在应用层实现（登录凭据/白名单注册） |

握手后 `write_key` / `read_key` 即为派生的对称密钥，可直接用于 `TlsChannel` 加密通信：

```zig
const tls = polyrole.tls;

// server_pk 为 Client 预置的 Server 公钥
// server_kp 为 Server 持有的身份密钥对（Client 公钥不再需要）
var client_ctx = tls.ClientContext.init(server_pk);
var server_ctx = tls.ServerContext.init(server_kp);

const R = polyrole.runner.Runner(tls.ClientHello);
try R.simulate(&client_ctx, &server_ctx, tls.ClientHello);

// client_ctx.write_key / read_key 即为派生的对称密钥
```

网络部署时用 `symmetric_run` 跑在 `StreamChannel` 上（或 Mux 加密模式），
握手后创建的 `TlsChannel` 支持密钥轮换（见下文）。

详见 `src/protocol/tls/README.md`。

TlsChannel 的密钥轮换（KeyUpdate）机制见上文「密钥轮换」小节。

---

## 安装

在项目根目录执行：
```shell
zig fetch --save git+https://github.com/sdzx-1/polyrole-cs.git
```

**build.zig:**

```zig
const polyrole_cs = b.dependency("polyrole_cs", .{});
exe.root_module.addImport("polyrole_cs", polyrole_cs.module("polyrole_cs"));
```

## 运行测试

```bash
zig build test
```

## 许可

MIT
