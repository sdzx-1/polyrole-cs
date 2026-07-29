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
│   ├── runner.zig                # 状态机驱动（simulate / symmetric_run）
│   ├── channel.zig               # 通道抽象（StreamChannel）
│   ├── family_mux_channel.zig    # 协议族传输层（MultiplexChannel）
│   ├── family_test.zig           # Mux 测试
│   ├── codec.zig                 # 二进制编解码
│   ├── Graph.zig                 # DOT 格式状态图生成
│   └── protocol/                 # 协议实现
│       ├── tls.zig               # 简易加密握手协议（模块入口）
│       ├── tls/                  # 内部实现
│       │   ├── root.zig
│       │   ├── context.zig       # 握手上下文 + 密钥协商
│       │   ├── test.zig
│       │   ├── README.md
│       │   └── design.md
│       ├── net_monitor.zig       # 网络延迟探测协议（模块入口）
│       └── net_monitor/          # 内部实现
│           ├── root.zig
│           ├── context.zig
│           ├── test.zig
│           ├── design.md
│           └── design_cn.md
├── tools/
│   └── graph.zig                 # 状态图生成工具（zig build graph）
├── docs/
│   ├── family.md                 # 协议族设计说明
│   └── graphs/                   # 生成的状态图（gitignored）
├── examples/                     # 示例（当前为空）
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
try ch.init(allocator, stream, 256, 256);
defer ch.deinit(allocator);
try R.symmetric_run(.server, &server_ctx, &ch, A, null);

// Client 端（并发任务中）
try R.symmetric_run(.client, &client_ctx, &ch, A, null);
```

---

## 模块

| 模块 | 路径 | 说明 |
|------|------|------|
| `runner` | `src/runner.zig` | 状态机驱动：`simulate()`、`symmetric_run()` |
| `channel` | `src/channel.zig` | 通道抽象：`StreamChannel`（明文 TCP）、`TlsChannel`（AEAD 加密） |
| `family_mux_channel` | `src/family_mux_channel.zig` | 协议族传输层：`MultiplexChannel(N)` 多协议共享 TCP |
| `codec` | `src/codec.zig` | 二进制编解码：状态 ID + tag + payload |
| `Graph` | `src/Graph.zig` | DOT 格式状态图生成 |
| `tls` | `src/protocol/tls/` | 简化 TLS 1.3 握手协议（示例） |

### Channel

**StreamChannel** — 明文 TCP 通道：

```zig
var ch: StreamChannel = undefined;
try ch.init(allocator, stream, read_buf_size, write_buf_size);
defer ch.deinit(allocator);
```

**TlsChannel** — AEAD 加密通道，在 StreamChannel 之上使用 NaCl SecretBox：

```zig
var tc: TlsChannel = undefined;
try tc.init(allocator, &sc, write_key, read_key, 512);
defer tc.deinit(allocator);
```

每条消息 wire format：`nonce(24) || tag(16) || ct_len(2 BE) || ciphertext`，
nonce 内嵌单调 u64 计数器，提供防重放和防乱序保护。

**MultiplexChannel** — 协议族传输层，多协议共享一条 TCP 连接：

```zig
pub fn MultiplexChannel(comptime protocol_count: u8, comptime max_message_size: usize, comptime channel_capacity: u8) type
```

**架构**：独立 Reader Fiber + 有界 MVar 队列：

```
  Reader Fiber [R]
  TCP → [protocol_id][payload_len][payload]
  → 解析 protocol_id
  → rb.send()
       ↓
  SubChannel[0].rb
  SubChannel[1].rb
  SubChannel[...].rb
```

SubChannel 接口与 `StreamChannel` 完全一致（`send`/`recv`），`symmetric_run` 零改动。

**wire format**：`[protocol_id: u8][payload_len: u16 BE][payload]`

**SubChannel.recv 超时**：`symmetric_run(..., 100)` → AutoCancel 100ms → `error.Canceled`。

详见 `docs/family.md`。

### Codec

二进制序列化格式：

```
state_id(4 BE) || tag(1) || payload
```

支持类型：`void`、`bool`、整数、`[]const u8`、定长字节数组、struct。

### Graph

生成 DOT 格式状态图：

```zig
var graph = try root.Graph.initWithFsm(allocator, P.A);
defer graph.deinit();

const graph_file = try std.Io.Dir.cwd().createFile(io, "graph.dot", .{});
var graph_file_writer = graph_file.writer(io, &.{});
try graph.generateDot(null, &graph_file_writer.interface);
```

可用 Graphviz 渲染：`dot -Tpng graph.dot -o graph.png`

## 文档

项目文档位于 `docs/` 目录：

| 文档 | 说明 |
|------|------|
| `docs/family.md` | 协议族（Protocol Family）设计——多协议共享 TCP 连接的架构说明 |

### 状态图

用 Graph 模块和 Graphviz 生成 DOT 格式状态图：

```bash
zig build graph
```

生成 `docs/graphs/*.dot` 和 `docs/graphs/*.png`，覆盖 `tls` 和 `net_monitor` 协议。

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

## 简易加密握手协议（示例）

项目包含一个自定的简易加密握手实现，展示了 polyrole-cs 的完整用法。

**前提**：Client 和 Server 已通过带外方式互知对方的 Ed25519 公钥——无需证书交换或 PKI。双方各自持有自己的身份密钥对，并信任对方的公钥。

**握手流程**：三条消息完成密钥协商和双向认证，不包含数据阶段。

- `ClientHello → ServerHello → ClientFinished → Exit`
- X25519 临时密钥协商（ephemeral-ephemeral，提供前向安全）
- Ed25519 身份签名 + HMAC 双向认证
- Transcript 链式 SHA256 哈希防篡改
- HKDF-SHA256 派生三把独立密钥

握手后 `write_key` / `read_key` 即为派生的对称密钥，可直接用于 `TlsChannel` 加密通信：

```zig
const tls = polyrole.tls;

// client_kp / server_pk 为 Client 持有的密钥对和 Server 公钥
// server_kp / client_pk 为 Server 持有的密钥对和 Client 公钥
var client_ctx = tls.ClientContext.init(client_kp, server_pk);
var server_ctx = tls.ServerContext.init(server_kp, client_pk);

const R = polyrole.runner.Runner(tls.ClientHello);
try R.simulate(&client_ctx, &server_ctx, tls.ClientHello);

// client_ctx.write_key / read_key 即为派生的对称密钥
```

详见 `src/protocol/tls/README.md`。

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
