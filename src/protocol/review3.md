# FS 设计审计

## 🔴 缺失存储

### ServerHello.process 未存储 `own_mac`

ServerContext 有 `own_mac: [32]u8` 字段，注释说"computed in ServerHello, used in ClientFinished transcript"。但 ServerHello.process 的步骤列表中未写入存储 `own_mac` 的操作。

ClientFinished.preprocess (server) 需要 `own_mac` 计算 `transcript_3`。不存储则 server 在验证时拿不到自己的 MAC。

**修复：** ServerHello.process step 8 增加 `own_mac`。

---

## 🟡 不足

### 双方都需要自己的 ephemeral_pk 来重算 transcript，但未存储

`ClientFinished` 中双方需要计算 `transcript_1 = SHA256(client_nonce ++ epk_c ++ server_nonce ++ epk_s)`。当前 Context 只有 `ephemeral_sk` 和 `peer_ephemeral_pk`，缺自己的公钥。

两方可从 `ephemeral_sk` 通过 `X25519.publicKey(esk)` 重算——只是多一次 EC 标量乘法。影响：性能（约 200k cycles，对示例可忽略），不影响正确性。

**选择：** 接受重算，或给两个 Context 都加上 `own_ephemeral_pk: [32]u8`。建议加字段（显式 > 隐式，32 字节可忽略）。

---

## ✅ 已验证通过

| 检查项 | 结果 |
|--------|------|
| `transcript_1` 双方一致（固定顺序 cn ++ epk_c ++ sn ++ epk_s） | ✓ |
| `t2`–`t4` 逐级链式绑定 | ✓ |
| 两个 HMAC（server_fin / client_fin）label 不同 | ✓ |
| 两个 Ed25519 签名密钥不同（server_id_sk ≠ client_id_sk），transcript 不同（t1 ≠ t3） | ✓ |
| HKDF info 字符串互不相同（"hs" / "c2s" / "s2c"） | ✓ |
| shared_secret 从未在网络上明文传输 | ✓ |
| 身份密钥泄露不解密历史会话（前向安全） | ✓ |
| ClientHello 未认证 → MitM 替换内容导致签名验证失败 → 仅 DoS | ✓ |
| Context 所有字段在首次使用前已设置 | ✓ (own_mac 除外) |
| Ed25519 签名 64 字节 | ✓ |
| `enter_data` / `close` 两变体共享同一 Payload | ✓ |
