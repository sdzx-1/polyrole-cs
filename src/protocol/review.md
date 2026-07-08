# Simple TLS 设计审查

## 🔴 正确性问题

### 1. ServerFinished 的 transcript_hash 与 ClientFinished 相同

文档中两者都使用同一个 `transcript_hash = SHA256(ClientHello || ServerHello)`。

TLS 1.3 中，ServerFinished 的 transcript 包含了 ClientFinished：

```
ClientFinished transcript = Hash(ClientHello...ServerHello)
ServerFinished transcript = Hash(ClientHello...ServerHello...ClientFinished)
```

**为什么必须不同？** 如果 ServerFinished 不绑定 ClientFinished，攻击者可以截获 ClientFinished 后直接重放一个旧的 ServerFinished（来自同一 transcript_hash 的另一个会话）。ServerFinished 包含 ClientFinished 的 MAC 能防止 Finished 消息被跨会话拼接。

**修复：**
- `ClientFinished.mac` = `HMAC(handshake_key, "client_fin" ++ SHA256(client_nonce || server_nonce))`
- `ServerFinished.mac` = `HMAC(handshake_key, "server_fin" ++ SHA256(client_nonce || server_nonce || client_finished_mac))`

Server 在 `ServerFinished.preprocess` 中验证 ClientFinished 后，将其 `mac` 存入 context，用于计算自己的 transcript。

---

### 2. transcript 序列化方式不明确

`SHA256(ClientHello || ServerHello)` 没有定义如何把两条消息转换为字节。

由于 polyrole 框架中 `process()` 返回 variant（包含 tag），序列化发生在 channel 层（`codec.encode`），协议状态本身拿不到 wire format 字节。

**推荐方案：**

直接 hash `client_nonce ++ server_nonce`。理由：
- 双方在 ServerHello 阶段后都持有这两个值
- `handshake_key` 是从 `session_key` 派生的，而 `session_key` 是从 `encrypted_key` 解密的——任何对 `encrypted_key` 的篡改都会改变 `session_key`，进而改变 `handshake_key`，导致 HMAC 验证失败
- 因此 transcript_hash 只需要绑定 nonce 就足够，不需要重复绑定 key material

---

## 🟡 设计问题

### 3. 数据阶段自环需要应用层配合

`ClientData → ClientData` 自环意味着 Client 可以连续发送多条消息。但 `symmetric_run()` 是一个内部循环，无法在两次 `process()` 之间让应用层注入新的明文。

**推荐：** 初始实现使用严格交替模式：

```zig
// ClientData
send: Data(Ciphertext, ServerData),  // 发送一条后转到对方
close: Data(void, Exit),

// ServerData  
send: Data(Ciphertext, ClientData),
close: Data(void, Exit),
```

每一方发送一条消息后必须等待对方响应（或关闭）。应用层在调用 Runner 前将待发送明文写入 `ctx.send_buffer`。

---

### 4. 认证失败的处理方式

`crypto_box.open` 和 HMAC 验证可能失败，但当前框架的 `preprocess` 签名是 `fn(ctx, result) void`，无法传播错误。

作为示例协议，可以接受 `@panic("authentication failed")`。生产环境需要更优雅的处理，但那是框架层的问题（见 transport-error-design）。

状态机层面：认证失败的路径在状态图中不可见，这是有意的——协议只描述正确路径。

---

## 🟢 确认无问题的设计

| 项目 | 结论 |
|---|---|
| **crypto_box payload 大小** | `encrypted_nonce`: 24 + 16 = 40 ✓ / `encrypted_key`: 32 + 16 = 48 ✓ |
| **box_nonce 不重复使用** | 每步独立生成随机 24 字节，碰撞概率可忽略 |
| **server_nonce 明文传输** | 非秘密值，完整性由 HMAC 保证，安全 |
| **HKDF-Expand 公式** | `HMAC-SHA256(prk, info ‖ 0x01)[0..32]` 是正确的单块展开 |
| **HKDF 衍生层级** | 两级（handshake / app → c2s,s2c）略多但无问题，符合密钥隔离原则 |
| **密钥用途隔离** | `handshake_key` ≠ `write_key` ≠ `read_key`，防止跨用途密钥复用 |
| **XChaCha20 nonce** | 每条消息独立随机 24 字节，192-bit 空间，安全 |
| **防重放** | 双方 nonce 随机 + HMAC 绑定，重放消息 MAC 验证必然失败 |
| **无前向安全性** | 明确的设计取舍，文档已标注 |

---

## 修正汇总

| 项目 | 原方案 | 修正后 |
|---|---|---|
| ClientFinished transcript | `SHA256(ClientHello ‖ ServerHello)` | `SHA256(client_nonce ‖ server_nonce)` |
| ServerFinished transcript | 同上 | `SHA256(client_nonce ‖ server_nonce ‖ client_finished_mac)` |
| 数据阶段自环 | `ClientData → ClientData` | 严格交替：`ClientData → ServerData → ClientData → ...` |
| 错误处理 | "abort" | `@panic`（示例协议，可接受） |
