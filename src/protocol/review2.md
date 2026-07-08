# Simple TLS 二次审计报告

## 🔴 握手会失败的正确性 bug

### Bug 1：`transcript_hash` 编码歧义 — Client/Server 计算出的 hash 不同

文档定义 `transcript_hash = SHA256(own_nonce || peer_nonce)`。

代入具体值：

| 角色 | `own_nonce` | `peer_nonce` | 实际计算 |
|------|-------------|-------------|----------|
| Client | `client_nonce` | `server_nonce` | `SHA256(client_nonce \|\| server_nonce)` |
| Server | `server_nonce` | `client_nonce` | `SHA256(server_nonce \|\| client_nonce)` |

拼接顺序不同 → **hash 不同** → HMAC 永远对不上 → 握手 100% 失败。

**修复方法：** 使用显式命名，保证双方用同一个拼接顺序：

```
transcript_hash = SHA256(client_nonce ++ server_nonce)
```

双方都持有 `client_nonce` 和 `server_nonce`，按固定顺序拼即可。

---

### Bug 2：`extended_transcript_hash` 同样的问题

`extended_transcript_hash = SHA256(peer_nonce || own_nonce || client_finished_mac)` 有相同的顺序歧义。

**修复：**

```
extended_transcript_hash = SHA256(client_nonce ++ server_nonce ++ client_finished_mac)
```

---

### Bug 3：Client 端 `client_finished_mac` 未存储

`ServerFinished.preprocess` (client) 需要用到"自己的 `client_finished_mac`"：

> preprocess (client): Compute extended_transcript_hash from own_nonce (= client_nonce), peer_nonce (= server_nonce), and its own client_finished_mac.

但 `ClientFinished.process` 中从未将 `mac` 存入 context：

> process (client):
> 1. Compute transcript_hash
> 2. Compute mac
> 3. Derive application keys
> 4. Return

**修复：** 在 `ClientFinished.process` 中增加一步：

> 3. Store `mac` as `client_finished_mac` in context.

---

## 🟡 次要

### XChaCha20-Poly1305 API 签名与 std lib 不一致

文档伪代码：

```
xchacha20_poly1305.encrypt(ciphertext, tag, plaintext, null, nonce, write_key)  // 6 参数
```

`std.crypto.aead.xchacha20_poly1305.XChaCha20Poly1305.encrypt` 实际是 5 参数组合形式（`tag || ciphertext`）。如果用分离的 tag 需要 `encryptDetached`。这是实现细节，编码时确认 API 即可。

---

## 修正汇总

| 位置 | 问题 | 修正 |
|------|------|------|
| ClientFinished: `transcript_hash` | `SHA256(own \|\| peer)` 两边不同 | `SHA256(client_nonce ++ server_nonce)` |
| ServerFinished: `extended_transcript_hash` | 同上 | `SHA256(client_nonce ++ server_nonce ++ client_finished_mac)` |
| ClientFinished.process | 未保存 `mac` 到 context | 增加 `ctx.client_finished_mac = mac` |
