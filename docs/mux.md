# Mux — 多路复用传输层说明

> polyrole-cs 的核心特性:一条连接承载 N 个协议,各自独立状态机、独立缓冲、独立超时,同步握手提供每协议背压(消费方控制生产方,内存有界)。

## 为什么需要 Mux

"一个连接承载多种语义"是真实需求:控制面 + 推送面、信令 + 数据、登录 + 业务。朴素做法是串行复用(握手 → 业务),或每条语义一条连接(连接数爆炸)。

Mux 把多路复用变成框架能力:多个协议共享一条底层连接,每个协议跑独立状态机,传输成本与单协议几乎相同。

## 核心概念

### 协议族声明(`Protocol`)

```zig
pub const Protocol = struct {
    enter: type,              // 协议入口状态(如 Ctrl.A)
    runner: type,             // Runner(协议状态机)
    client_ct: type,          // 客户端上下文类型
    server_ct: type,          // 服务端上下文类型
    max_massage_size: usize,  // 单条消息上限(子通道缓冲)
    recv_timeout_ms: ?u64,    // 每协议独立接收超时
};
```

### SubChannel

每个协议一个 `SubChannel`(独立 `send_buff`/`recv_buff`),接口与 `StreamChannel` 完全一致——**协议代码零改动**即可跑在 Mux 上(同一份协议定义同时支持 simulate / symmetric_run / Mux)。

**背压语义(有意设计)**:分发是同步握手——`reader_loop` 将一帧交给某协议时,必须等该协议任务 `recv` 取走才继续。因此:
- 每个 `recv_buff` 容量恒为 1,内存有界:慢协议不会导致缓冲堆积
- 处理不过来的协议会**背压整条通道**(reader 停 → 对端 writer 被 TCP 限速),把"消费速度"如实传回源头
- 消息顺序天然保证,状态机无需处理并发/乱序
- 代价:单协议吞吐受限于任务处理速度(约 1 消息/处理周期)——适合控制面/信令类小消息协议;高吞吐大流量场景请评估是否适用

### wire format(批记录)

```
[total_len u32 BE][帧...]
每帧: [protocol_id u8][payload_len u16 BE][payload]
```

`writer_loop` 把一轮聚合的帧拷贝进连续缓冲一次写出;`reader_loop` 读入整批后按帧头切分分发。

### 加密模式(可选)

`encrypt = true` 时,整批明文作为一条 AEAD 记录加密(`TlsChannel`):
- 记录长度 u32(批明文可超 64 KiB)
- **密钥轮换(KeyUpdate)在 Mux 层透明吸收**,上层无感知
- 密钥来自 TLS 握手(`MuxKeys{write_key, read_key}`)或带外协商

## API(当前版本)

### 1. 类型实例化:角色编译期绑定

```zig
const TmpMuxClient = Mux(&.{ protocol1, protocol2 }, .client, false); // 明文
const TmpMuxServer = Mux(&.{ protocol1, protocol2 }, .server, false);

const TmpMuxClientEnc = Mux(&.{ protocol1, protocol2 }, .client, true); // 加密
```

- 角色(`.client`/`.server`)是**编译期参数**——两端是不同静态类型,上下文元组类型在编译期推导(`CreateContextTuple`)
- 传错角色的上下文,编译期即报错

### 2. 初始化:上下文在 init 绑定

```zig
var mux: TmpMuxClient = undefined;
try mux.init(gpa, ctxs, &sc, keys);
defer mux.deinit(gpa);
```

- `ctxs`:各协议上下文指针元组(`.{ &ctx1, &ctx2 }`),存入 `mux.ctxs`
- `sc`:底层 `StreamChannel`(明文模式)或 `TlsChannel` 握手后的通道(加密模式)
- `keys`:加密模式必传 `MuxKeys{write_key, read_key}`,明文模式传 `null`

### 3. 运行:group 归调用方

```zig
var group: zio.Group = .init;
try mux.run(&group);   // 每个协议 spawn 一个任务进 group
try group.wait();      // 调用方决定何时汇合
```

- `run` 只负责把各协议任务 spawn 进调用方的 `group`,不隐式 wait
- group 生命周期由调用方控制,可复用、可与其它任务汇合

### 4. 结果:error_channel 逐协议上报

```zig
try group.wait();
while (mux.error_channel.tryRecv()) |info| {
    switch (info.err) {
        null => log.info("protocol {d} finished ok", .{info.protocol_id}),
        else => |e| log.err("protocol {d} failed: {s}", .{ info.protocol_id, @errorName(e) }),
    }
}
```

- 每个协议**恰好一条消息**:成功 `{protocol_id, err=null}`,失败 `{protocol_id, err}`
- 容量 = 协议数,不会阻塞;**应在 `group.wait()` 后及时消费**

## 完整示例(明文,双协议)

```zig
const protocols = [_]polyrole.runner.Protocol{
    .{ .enter = Ctrl.A, .runner = R_ctrl, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
    .{ .enter = Push.A, .runner = R_push, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
};

const TmpMuxClient = Mux(&protocols, .client, false);
const TmpMuxServer = Mux(&protocols, .server, false);

// 服务端
var mux_s: TmpMuxServer = undefined;
try mux_s.init(allocator, .{ &server_ctx1, &server_ctx2 }, &sc, null);
defer mux_s.deinit(allocator);
var group_s: zio.Group = .init;
try mux_s.run(&group_s);
try group_s.wait();

// 客户端(并发任务中)
var mux_c: TmpMuxClient = undefined;
try mux_c.init(gpa, .{ &client_ctx1, &client_ctx2 }, &sc, null);
defer mux_c.deinit(gpa);
var group_c: zio.Group = .init;
try mux_c.run(&group_c);
try group_c.wait();
```

加密模式差别:实例化时 `encrypt=true`,`init` 传 `MuxKeys`;TLS 握手阶段用 `symmetric_run` 跑在 `StreamChannel` 上,握手后把 `TlsChannel` 密钥交给 Mux。

## 错误语义

| 场景 | 行为 |
|---|---|
| 某协议返回错误 | 其余协议继续;错误经 `error_channel` 上报,`group.wait()` 正常返回 |
| 某协议 panic | 进程级崩溃(zio 不捕获 panic) |
| error_channel 满 | panic(`mux: error_channel full`)——容量=协议数,每协议一条,正常不会发生 |
| 一端提前退出 | 另一端对应协议 recv 超时/EOF 失败,按各自 `recv_timeout_ms` 处理 |

## 限制与注意

- `max_massage_size` 决定子通道缓冲,同时是单条消息上限;总发送缓冲 = Σ(max_massage_size + 3) + 4
- 明文模式 `recv_buf` 由 Mux 分配;加密模式批记录由 TlsChannel 处理
- 每个协议至多一条消息在途(发送许可),高吞吐协议可适当调大消息粒度
- KeyUpdate 透明吸收,无需在协议层处理

## 测试

`src/test/runner_test.zig`:`mux test`(明文)、`mux test encrypted`(加密)——含双向多协议、TLS 握手后加密 Mux、错误路径。
