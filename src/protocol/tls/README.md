# Simple TLS 协议详解

## 1. 概述

Simple TLS 是一个基于 polyrole-cs 状态机框架实现的轻量级握手协议，设计上参考了 TLS 1.3 的三消息握手模式。协议的目标是：在 Client 和 Server 之间建立一个加密会话，派生对称密钥，供后续的 `TlsChannel` 加密通信使用。

**核心特点：**

- **三消息握手**：`ClientHello → ServerHello → ClientFinished → Exit`，无数据阶段。
- **前向安全**：通过 ephemeral-ephemeral X25519 ECDH 实现，会话密钥不可回溯。
- **双向身份认证**：Ed25519 签名证明身份，HMAC 证明正确的 `shared_secret` 派生。
- **Transcript 链式哈希**：每一步都包含前一步的哈希，防止截断、重排和中间人插入。

---

## 2. 前置条件

双方通过 **带外方式** 已知对方的 Ed25519 公钥（等同于 TLS 中预共享的信任锚）：

| 角色   | 持有                           |
|--------|--------------------------------|
| Client | `client_keypair` + `server_public_key` |
| Server | `server_keypair` + `client_public_key` |

不需要证书交换或 PKI。

---

## 3. 状态机总览

```
ClientHello ──(client)──▶ ServerHello ──(server)──▶ ClientFinished ──(client)──▶ Exit
```

每个状态的 `info.agent` 字段标明了该状态由谁执行：
- `.client`：Client 执行 `process()` 产生转移数据，Server 执行 `preprocess()` 接收处理。
- `.server`：相反。

框架的 `Runner.symmetric_run()` 根据 `info.agent` 自动判断当前角色是该 `send` 还是 `recv`。

---

## 4. 密码学原语

| 原语 | 用途 |
|------|------|
| **X25519** (`crypto.dh.X25519`) | 临时密钥协商，产生 `shared_secret` |
| **Ed25519** (`crypto.sign.Ed25519`) | 身份签名，证明持有私钥 |
| **HMAC-SHA256** | Finished 消息认证 |
| **SHA256** | Transcript 链式哈希 |
| **HKDF-SHA256** | 从 `shared_secret` 派生独立密钥 |
| **CSPRNG** (`Io.randomSecure`) | 生成随机 nonce 和临时密钥 |

---

## 5. 握手三步详解

### 5.1 第一步 — ClientHello（Client → Server）

**执行者**：Client（`process`）

**Payload**：`{ nonce: [24]u8, ephemeral_pk: [32]u8 }`

Client 在 `process()` 中执行：

```zig
// 1. 生成随机 nonce
const nonce = try randomBytes(ctx.io, 24);

// 2. 生成临时 X25519 密钥对
const kp = generateX25519Keypair(ctx.io);

// 3. 保存到 Context（后续步骤需要）
ctx.own_nonce = nonce;
ctx.ephemeral_sk = kp.secret_key;
ctx.own_ephemeral_pk = kp.public_key;

// 4. 发送给 Server
return .{ .to_server = .{ .data = .{
    .nonce = nonce,
    .ephemeral_pk = kp.public_key,
}}};
```

Server 在 `preprocess()` 中接收并保存 Client 的 nonce 和临时公钥：

```zig
ctx.peer_nonce = d.data.nonce;
ctx.peer_ephemeral_pk = d.data.ephemeral_pk;
```

**数据流向**：

```
Client                                     Server
  │                                          │
  │  nonce_c, ephemeral_pk_c                 │
  ├─────────────────────────────────────────▶│
  │                                          │ 保存 peer_nonce, peer_ephemeral_pk
```

---

### 5.2 第二步 — ServerHello（Server → Client）

Server 完成 ECDH 计算、签名和 MAC，是最关键的一步。

**执行者**：Server（`process`）

**Payload**：`{ nonce: [24]u8, ephemeral_pk: [32]u8, signature: [64]u8, mac: [32]u8 }`

#### process（Server 端）

```zig
// 1. 生成 Server 的随机 nonce 和临时密钥
const nonce = try randomBytes(ctx.io, 24);
const kp = generateX25519Keypair(ctx.io);

// 2. ECDH：计算共享密钥
const shared_secret = crypto.dh.X25519.scalarmult(
    kp.secret_key,              // server ephemeral sk
    ctx.peer_ephemeral_pk,      // client ephemeral pk (第1步收到的)
) catch return error.DhFailed;

// 3. HKDF 派生 handshake_key
const keys = types.deriveKeys(shared_secret);
ctx.handshake_key = keys.handshake_key;

// 4. Transcript 链：t1 → t2 → mac
const t1 = sha256(client_nonce ++ client_epk ++ server_nonce ++ server_epk);
const signature = try sign(server_id_keypair, t1);
const t2 = sha256(t1 ++ signature);
const mac = hmacSha256(handshake_key, "server_fin", t2);
```

**Transcript 构造**：

```
t1 = SHA256(cn || epk_c || sn || epk_s)
     └── 绑定双方 nonce 和临时公钥，建立会话唯一性

sig_s = Ed25519.Sign(server_id_sk, t1)
     └── 证明 Server 持有 server_id_sk（身份认证）

t2 = SHA256(t1 || sig_s)
     └── 将签名纳入 transcript，后续 hash 无法伪造

mac_s = HMAC-SHA256(handshake_key, "server_fin" || t2)
     └── 证明 Server 计算出了正确的 shared_secret
     └── 使用独立标签 "server_fin" 防止域混淆
```

#### preprocess（Client 端）

Client 收到 ServerHello 后，独立执行相同的计算并验证：

```zig
// 1. ECDH（使用自己的临时私钥 + Server 的临时公钥）
const shared_secret = crypto.dh.X25519.scalarmult(
    ctx.ephemeral_sk,             // client ephemeral sk（第1步保存的）
    payload.ephemeral_pk,         // server ephemeral pk（刚收到的）
) catch return error.DhFailed;

// 2. 派生 handshake_key
const keys = types.deriveKeys(shared_secret);
ctx.handshake_key = keys.handshake_key;

// 3. 重建 t1，验证签名
const t1 = sha256(own_nonce ++ own_epk ++ server_nonce ++ server_epk);
try verifySignature(signature, t1, server_id_pk);
//   ↑ 失败 → error.SignatureInvalid

// 4. 重建 t2，验证 MAC
const t2 = sha256(t1 ++ signature);
try verifyHmac(handshake_key, "server_fin", t2, mac);
//   ↑ 失败 → error.HmacInvalid（handshake_key 不一致或 transcript 被篡改）
```

Client 将 Server 的关键数据保存到 Context，供第三步使用：

```zig
ctx.peer_nonce = payload.nonce;
ctx.peer_ephemeral_pk = payload.ephemeral_pk;
ctx.peer_signature = payload.signature;
ctx.peer_mac = payload.mac;
```

**数据流向**：

```
Client                                     Server
  │                                          │
  │  nonce_s, epk_s, sig_s, mac_s            │
  │◀─────────────────────────────────────────┤
  │                                          │
  │ 1. ECDH → shared_secret                  │
  │ 2. HKDF → handshake_key                  │
  │ 3. 重建 t1, 验证 sig_s                    │
  │ 4. 重建 t2, 验证 mac_s                    │
```

**安全分析**：

- `sig_s` 验证失败 → Server 不是声称的身份（或 transcript 被篡改）
- `mac_s` 验证失败 → 双方 `shared_secret` 不一致（可能中间人替换了临时公钥）

---

### 5.3 第三步 — ClientFinished（Client → Server）

Client 完成签名、MAC 和最终密钥派生，握手结束。

**执行者**：Client（`process`）

**Payload**：`{ signature: [64]u8, mac: [32]u8 }`

**唯一转移**：`close: Data(ClientFinishedPayload, Exit)` — 握手后直接退出，无数据状态。

#### process（Client 端）

```zig
// 1. 派生应用密钥（handshake_key 已在 ServerHello.preprocess 中派生）
const keys = types.deriveKeys(shared_secret);

// 2. 重建 transcript 链
const t1 = sha256(own_nonce ++ own_epk ++ peer_nonce ++ peer_epk);
const t2 = sha256(t1 ++ peer_signature);    // peer_signature 来自 ServerHello
const t3 = sha256(t2 ++ peer_mac);          // peer_mac 来自 ServerHello

// 3. 签名 t3
const signature = try sign(client_id_keypair, t3);

// 4. t4 → MAC
const t4 = sha256(t3 ++ signature);
const mac = hmacSha256(handshake_key, "client_fin", t4);

// 5. 保存应用密钥，供 TlsChannel 使用
ctx.write_key = keys.client_write_key;
ctx.read_key = keys.server_write_key;

return .{ .close = .{ .data = .{ .signature = signature, .mac = mac } } };
```

**Transcript 构造**：

```
t3 = SHA256(t2 || mac_s)
     └── 绑定 Server 的 MAC，证明 Server 的 Finished 消息是真实的

sig_c = Ed25519.Sign(client_id_sk, t3)
     └── 证明 Client 持有 client_id_sk（身份认证）

t4 = SHA256(t3 || sig_c)

mac_c = HMAC-SHA256(handshake_key, "client_fin" || t4)
     └── 证明 Client 计算出了正确的 shared_secret
     └── 标签 "client_fin" 与 server_fin 不同，防止域混淆
```

#### preprocess（Server 端）

Server 验证 Client 的签名和 MAC，然后派生应用密钥：

```zig
// 1. 重建 transcript 链
const t1 = sha256(peer_nonce ++ peer_epk ++ own_nonce ++ own_epk);
const t2 = sha256(t1 ++ own_signature);     // ServerHello 中保存的 own_signature
const t3 = sha256(t2 ++ own_mac);           // ServerHello 中保存的 own_mac

// 2. 验证 Client 签名
try verifySignature(signature, t3, client_id_pk);

// 3. t4 → 验证 MAC
const t4 = sha256(t3 ++ signature);
try verifyHmac(handshake_key, "client_fin", t4, mac);

// 4. 派生应用密钥
const keys = types.deriveKeys(shared_secret);
ctx.read_key = keys.client_write_key;
ctx.write_key = keys.server_write_key;
```

**数据流向**：

```
Client                                     Server
  │                                          │
  │  sig_c, mac_c                            │
  ├─────────────────────────────────────────▶│
  │                                          │
  │                                          │ 1. 重建 t1,t2,t3
  return .close → Exit                       │ 2. 验证 sig_c
                                             │ 3. 重建 t4, 验证 mac_c
                                             │ 4. 派生 read/write key
```

---

## 6. 密钥派生 (HKDF-SHA256)

使用 RFC 5869 两阶段模式，从 `shared_secret` 派生三把独立密钥：

```
Phase 1 — Extract:
    salt = [0x00; 32]          (零填充，因为 shared_secret 已是高质量随机数)
    prk  = HMAC-SHA256(salt, shared_secret)

Phase 2 — Expand:
    handshake_key   = HMAC-SHA256(prk, "hs"  || 0x01)
    client_write_key = HMAC-SHA256(prk, "c2s" || 0x01)
    server_write_key = HMAC-SHA256(prk, "s2c" || 0x01)
```

**代码实现**（`context.zig`）：

```zig
pub fn deriveKeys(shared_secret: [32]u8) struct {
    handshake_key: [32]u8,
    client_write_key: [32]u8,
    server_write_key: [32]u8,
} {
    const salt = [_]u8{0} ** 32;
    const prk = hkdf_extract(salt, shared_secret);
    return .{
        .handshake_key = hkdf_expand(prk, "hs"),
        .client_write_key = hkdf_expand(prk, "c2s"),
        .server_write_key = hkdf_expand(prk, "s2c"),
    };
}
```

**方向映射**：

| 角色   | `write_key`        | `read_key`          |
|--------|--------------------|---------------------|
| Client | `client_write_key` | `server_write_key`  |
| Server | `server_write_key` | `client_write_key`  |

每把密钥有独立的 `info` 标签字符串，确保即使 `prk` 相同，不同用途的密钥在密码学上也是独立的。

---

## 7. Transcript 链的完整性

Transcript 链式哈希是整个握手的安全核心。每一步的哈希输入都包含上一步的输出，形成一个不可分割的因果链：

```
t1 = SHA256( cn || epk_c || sn || epk_s )
       │
       ▼
sig_s = Sign(server_sk, t1)
       │
       ▼
t2 = SHA256( t1 || sig_s )
       │
       ▼
mac_s = HMAC(hk, "server_fin" || t2)
       │
       ▼
t3 = SHA256( t2 || mac_s )
       │
       ▼
sig_c = Sign(client_sk, t3)
       │
       ▼
t4 = SHA256( t3 || sig_c )
       │
       ▼
mac_c = HMAC(hk, "client_fin" || t4)
```

**每一步的安全含义**：

| 步骤 | 绑定了什么 | 防止什么 |
|------|-----------|---------|
| t1 | 双方 nonce + 临时公钥 | 跨会话重放（nonce 唯一） |
| sig_s | t1 + server_id | Server 身份冒充 |
| t2 | t1 + sig_s | 篡改 Server 签名 |
| mac_s | t2 + shared_secret | 双方 shared_secret 不一致 |
| t3 | t2 + mac_s | 跳过 Server MAC 验证 |
| sig_c | t3 + client_id | Client 身份冒充 |
| t4 | t3 + sig_c | 篡改 Client 签名 |
| mac_c | t4 + shared_secret | 双方 shared_secret 不一致（二次确认） |

任何一方如果 `shared_secret` 计算错误，或者 transcript 被篡改，对应的 MAC 验证会立即失败。

---

## 8. 安全属性

| 属性 | 实现机制 |
|------|---------|
| **Client 身份认证** | Ed25519 签名 `sig_c` 绑定 `t3`（包含 `mac_s`），间接证明 Client 也计算出了正确的 `shared_secret` |
| **Server 身份认证** | Ed25519 签名 `sig_s` 绑定 `t1`（包含双方 nonce），证明 Server 控制 `server_id_sk` |
| **前向安全 (PFS)** | `shared_secret = X25519(esk_c, esk_s)`。临时密钥在握手后不可恢复（内存由调用方管理） |
| **密钥机密性** | `shared_secret` 从未在网络上传输，由各自独立计算 |
| **防重放** | 每次会话生成新的 CSPRNG nonce，杜绝跨会话重放 |
| **Transcript 一致性** | 链式 SHA256 防止截断、重排和消息插入 |
| **密钥独立性** | HKDF 使用独立 info 标签，handshake_key 泄漏不影响数据加密密钥 |
| **常数时间比较** | HMAC 验证使用 `crypto.timing_safe.eql`，抵抗时序侧信道 |

**不提供的属性：**

| 属性 | 说明 |
|------|------|
| **后妥协安全 (PCS)** | 身份密钥泄露后，攻击者可冒充该身份发起未来会话（但不解密历史会话） |
| **证书/信任链** | 双方公钥通过带外方式预共享，无 PKI |

---

## 9. 握手后：TlsChannel 加密通信

握手完成后，`ClientContext` 和 `ServerContext` 中的 `write_key` / `read_key` 即为派生的对称密钥。调用方创建 `TlsChannel` 并使用这些密钥进行加密通信：

```zig
// 握手结束后：
// ctx.write_key = [32]u8 (发送方向密钥)
// ctx.read_key  = [32]u8 (接收方向密钥)

var tc: TlsChannel = undefined;
try tc.init(io, allocator, stream, ctx.write_key, ctx.read_key, 512);
defer tc.deinit(allocator);

// 通过 symmetric_run 驱动后续协议
try R.symmetric_run(.client, &app_ctx, &tc, AppProtocol.Start);
```

`TlsChannel` 的详细设计见 `src/channel.zig` 文档注释。核心机制：
- 每条消息用 NaCl SecretBox AEAD 加密（XSalsa20-Poly1305）
- 24 字节 nonce 嵌入单调递增 u64 计数器，防止重放和乱序
- 消息长度嵌入 AEAD 载荷内部认证，wire 上的 `ct_len` 仅作帧分隔符

---

## 10. 错误处理

协议定义了四种错误类型：

| 错误 | 触发条件 | 含义 |
|------|---------|------|
| `EntropyUnavailable` | `randomBytes()` 失败 | 系统熵不足，无法继续 |
| `DhFailed` | `X25519.scalarmult()` 失败 | 对端临时公钥无效 |
| `SignatureInvalid` | `Ed25519.verify()` 失败 | 身份认证失败——对端不持有声称的私钥 |
| `HmacInvalid` | HMAC 不匹配 | `shared_secret` 不一致或 transcript 被篡改 |

这些错误在 `process` / `preprocess` 中通过 `!void` 或 `!@This()` 返回。polyrole-cs Runner 在编译期检测返回类型，如果是 error union 则自动 `try`，错误会直接传播到调用方，**协议不会继续执行**——这是一种安全的快速失败策略（fail-fast）。

---

## 11. 代码组织

```
src/protocol/tls/
├── root.zig         — 状态机定义 (ClientHello, ServerHello, ClientFinished)
├── context.zig      — 共享类型 (ClientContext, ServerContext, deriveKeys)
├── test.zig         — 测试 (simulate 和 symmetric_run 测试)
├── design.md        — 设计文档（英文）
└── README.md        — 本文档
```

状态机定义、类型和测试完全分离，`root.zig` 纯描述协议逻辑，`context.zig` 纯数据结构和密码学工具函数。
