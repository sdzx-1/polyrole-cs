# polyrole-cs

一个 Zig 编译期状态机协议框架，专用于 **Client-Server** 通信模型。

协议定义为 tagged union 的状态图，框架在编译期完成可达性分析、上下文类型校验和状态 ID 生成，
运行时零开销 dispatch。从单机模拟到加密多路复用网络传输，同一份协议代码通吃。

```zig
const polyrole = @import("polyrole_cs");
```

## 为什么用它

手写 Client-Server 协议时，每个消息都要处理四件重复的事：**收发 dispatch、序列化、
状态推进、传输层**。它们纠缠在一起，改协议时四处都要动。

polyrole-cs 把协议**声明**与**执行**分离：

- 协议 = 一个 tagged union 状态图（纯声明，无收发代码）
- 执行 = 框架的三种模式（模拟 / 网络 / 多路复用），协议代码零改动
- 状态流转、上下文类型、消息合法性在**编译期**校验，运行时零开销

结果：一个协议定义可以在单机测试、端到端网络、多协议共享连接三种场景下原样运行。

## 快速开始

一个 Client-Server 计数器协议——完整可运行代码见 `src/test/quickstart_test.zig`：

```zig
const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");

// 协议信息：协议名 + 双方上下文类型（编译期校验一致性）
const Info = polyrole.ProtocolInfo("counter", i32, i32);

const Counter = struct {
    // B：服务端状态——收到 add 就 +1，到 10 结束
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

    // A：客户端状态——发起 add
    pub const A = union(enum) {
        add: polyrole.Data(void, B),
        pub const info: Info = .{ .agent = .client, .name = "A" };
        pub fn process(ctx: *i32) @This() { _ = ctx; return .add; }
    };
};
```

**模式 1：simulate**——单线程内存模拟，零序列化，测逻辑：

```zig
const R = polyrole.runner.Runner(Counter.A);
var client: i32 = 0;
var server: i32 = 0;
try R.simulate(&client, &server, Counter.A);
// server == 10
```

**模式 2：symmetric_run**——两端通过网络/通道执行，协议代码零改动：

```zig
// Server 端
try R.symmetric_run(.server, &server_ctx, &ch_s, Counter.A, null);
// Client 端（并发任务中）
try R.symmetric_run(.client, &client_ctx, &ch_c, Counter.A, null);
```

## 核心概念

### 状态（State）

每个状态是一个 tagged union，`info` 声明 agent（`.client` / `.server`）和协议名；
`process(ctx)` 在 agent 端执行并产生转移，`preprocess(ctx, result)` 在对端接收、验证并更新上下文：

> 注：状态函数可以执行任意逻辑（含阻塞式 IO，如数据库、文件、密码学计算、sleep），
> 框架只负责收发与序列化——状态函数不直接触碰通道。

```zig
pub const ClientHello = union(enum) {
    to_server: polyrole.Data(ClientHelloPayload, ServerHello),
    pub const info: MyProtocol = .{ .agent = .client, .name = "ClientHello" };
    pub fn process(ctx: *ClientCtx) !@This() { ... }
    pub fn preprocess(ctx: *ServerCtx, result: @This()) !void { ... }
};
```

### 转移（Data）

每个 variant 携带 `Data(Payload, NextState)`——数据载荷和下一状态：

```zig
to_server: polyrole.Data(ClientHelloPayload, ServerHello),
//                                  ^载荷              ^下一状态
```

### 退出（Exit）

所有协议的最终状态是 `polyrole.Exit`——框架内置的唯一终止状态。

## 执行模式

| 模式 | 用途 | 形态 |
|------|------|------|
| `simulate` | 单机逻辑测试 | 双端同线程交替执行，零序列化 |
| `symmetric_run` | 单协议端到端 | 一条连接一个协议，支持超时 |
| `Mux` | 多协议端到端 | 一条连接 N 个协议，可选批记录加密 |

### Mux — 多路复用传输层（核心特性）

多个协议共享一条底层连接，每个协议跑独立状态机——独立缓冲、独立超时，
同步握手实现每协议背压（消费方控制生产方，内存有界），
传输成本与单协议几乎相同。解决"一个连接承载多种语义"（控制面 + 推送面、信令 + 数据）
的真实问题，把多路复用变成框架能力。

**机制**：每个协议一个 `SubChannel`（独立 `send_buff`/`recv_buff`）。发送许可机制保证
同一时刻每协议至多一条消息在途；`writer_loop` 把一轮聚合的帧拷贝进连续缓冲一次写出；
`reader_loop` 读入整批后按帧头切分分发。

**wire format（批记录）**：`[total_len u32 BE][帧...]`，每帧 `[protocol_id u8][payload_len u16 BE][payload]`。

```zig
const Mux = polyrole.runner.Mux;

// 声明协议族（每个协议一个 Protocol 条目）
const protocols = [_]polyrole.runner.Protocol{
    .{ .enter = Ctrl.A, .runner = R_ctrl, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
    .{ .enter = Push.A, .runner = R_push, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
};

// 明文模式：keys 传 null
const TmpMux = Mux(&protocols, false);
var mux: TmpMux = undefined;
try mux.init(gpa, &sc, null);
try mux.run(.client, ctxs);   // ctxs 是各协议 context 的元组

// 加密模式：整批明文作为一条 AEAD 记录加密，密钥来自 TLS 握手（或带外协商）
const TmpMux = Mux(&protocols, true);
try mux.init(gpa, &sc, .{ .write_key = wk, .read_key = rk });
try mux.run(.server, ctxs);
```

加密模式细节：批记录整体 AEAD 认证，记录长度 u32（批明文可超 64 KiB）；
密钥轮换（KeyUpdate）在 Mux 层透明吸收，上层无感知。
`SubChannel` 接口与 `StreamChannel` 完全一致，协议代码零改动。

## 模块

| 模块 | 路径 | 说明 |
|------|------|------|
| `runner` | `src/runner.zig` | 状态机驱动：`simulate()`、`symmetric_run()`；多路复用传输层 `Mux()` |
| `channel` | `src/channel.zig` | 通道抽象：`StreamChannel`（明文 TCP）、`InMemoryChannel`（进程内管道）、`TlsChannel`（AEAD 加密 + 密钥轮换） |
| `codec` | `src/codec.zig` | 二进制编解码：状态 ID + tag + payload |
| `Graph` | `src/Graph.zig` | DOT 格式状态图生成 |
| `tls` | `src/protocol/tls/` | 简易加密握手协议（单向认证） |

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
try tc.init(allocator, &sc, write_key, read_key, 512);  // 512 = 单条记录缓冲上限
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

## 示例协议

### 简易加密握手协议（单向认证）

展示了框架在安全协议上的完整用法：三条消息完成密钥协商和 **Server 认证**
（HTTPS 模型——Client 预置 Server 公钥，Server 无需知道 Client 身份，可服务任意客户端）：

- `ClientHello → ServerHello → ClientFinished → Exit`
- X25519 临时密钥协商（PFS）+ Server Ed25519 身份签名 + 双方 HMAC
- Server 签名覆盖双方临时公钥，MITM 无法替换（防冒充 Server）
- ClientFinished 为会话持有证明，不携带 Client 身份

```zig
const tls = polyrole.tls;

// server_pk 为 Client 预置的 Server 公钥；server_kp 为 Server 的身份密钥对
var client_ctx = tls.ClientContext.init(server_pk);
var server_ctx = tls.ServerContext.init(server_kp);

const R = polyrole.runner.Runner(tls.ClientHello);
try R.simulate(&client_ctx, &server_ctx, tls.ClientHello);

// client_ctx.write_key / read_key 即为派生的对称密钥
```

网络部署：`symmetric_run` 跑在 `StreamChannel` 上（或 Mux 加密模式），握手后创建的
`TlsChannel` 支持密钥轮换。

**生产部署必须为握手设置超时**——`symmetric_run` 的 `recv_timeout_ms` 参数
（如 10 秒）。否则恶意/慢速对端可以在握手阶段挂起连接不发送消息，
server 将永久阻塞（握手 DoS）：

```zig
// 握手阶段：10s 超时（超时以 error.ReadFailed / error.Canceled 中止）
try R.symmetric_run(.server, &server_ctx, &ch, tls.ClientHello, 10_000);
// 数据阶段：TlsChannel 业务消息可另行配置超时
```

**安全边界**：

| 提供 | 不提供 |
|------|--------|
| Server 身份认证（防 MITM 冒充 Server） | Client 身份认证（Client 匿名） |
| 机密性 / 完整性 / 防重放 / PFS | 防"陌生 Client 自报身份"——需应用层实现（登录凭据/白名单注册） |

**生产使用注意点**：

- **信任锚分发**：安全性的根基是 Client 预置的 Server 公钥是真的——用带外安全渠道
  分发或证书固定，否则一切验证都建立在假锚上
- **单一密码套件**：无协商机制，固定 X25519 + Ed25519 + XSalsa20-Poly1305 +
  HKDF-SHA256；若需算法演进，需版本化协议
- **自定记录格式**：`TlsChannel` 的记录格式是自定的（nonce || tag || ct_len || ct），
  不是标准 TLS wire format——互操作对象只能是本库

## 测试

全部测试位于 `src/test/`（`zig build test` 编译运行）：

| 文件 | 覆盖 |
|------|------|
| `quickstart_test.zig` | 快速开始示例（simulate + InMemoryChannel 对称运行） |
| `codec_test.zig` | 编解码畸形输入（非法布尔、越界 tag、超长切片） |
| `channel_test.zig` | AEAD 错误路径（重放/篡改/长度）、密钥轮换、内存通道全双工 |
| `runner_test.zig` | simulate / symmetric_run / 超时 / TLS 加密通道 / Mux 明文+加密 |
| `tls_test.zig` | 握手协议（签名/MAC/临时公钥篡改、重放、会话隔离、跨会话 ClientFinished 重放、MAC 域分离） |

## 文档

| 文档 | 说明 |
|------|------|
| `README.md` | 本文件：框架概览与使用指南 |
| `README_EN.md` | 英文版框架概览与使用指南 |
| `src/protocol/tls/README.md` | 简易 TLS 握手协议设计（中文） |
| `src/protocol/tls/design.md` | 简易 TLS 握手协议设计（英文） |

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
