# FS 设计最终审计

## 结论：✅ 设计完整正确，无可发现缺陷

---

## 全面验证清单

### 1. 转录链一致性

| 哈希 | Server 计算 | Client 计算 | 一致？ |
|------|-------------|-------------|:---:|
| `t1 = SHA256(cn++epk_c++sn++epk_s)` | `peer_nonce++peer_epk++own_nonce++own_epk` | `own_nonce++own_epk++peer_nonce++peer_epk` | ✅ |
| `t2 = SHA256(t1++sig_s)` | t1 + 刚算出的 sig_s | t1 + payload 中的 sig_s | ✅ |
| `t3 = SHA256(t2++mac_s)` | t2 + ctx.own_mac | t2 + ctx.peer_mac | ✅ |
| `t4 = SHA256(t3++sig_c)` | t3 + payload 中的 sig_c | t3 + 刚算出的 sig_c | ✅ |

### 2. Context 字段生命周期

| 字段 | ClientContext | ServerContext |
|------|:--:|:--:|
| `id_secret` | 初始 → ClientFinished 用 | 初始 → ServerHello 用 |
| `peer_id_public` | 初始 → ServerHello.preprocess | 初始 → ClientFinished.preprocess |
| `ephemeral_sk` | ClientHello → ServerHello.preprocess (DH) | ServerHello → (DH 后丢弃) |
| `own_ephemeral_pk` | ClientHello → 各处 transcript | ServerHello → 各处 transcript |
| `peer_ephemeral_pk` | ServerHello.preprocess → transcript | ClientHello.preprocess → DH+transcript |
| `own_nonce` | ClientHello → transcript | ServerHello → transcript |
| `peer_nonce` | ServerHello.preprocess → transcript | ClientHello.preprocess → transcript |
| `peer_signature` | ServerHello.preprocess → verify+t2/t3/t4 | — |
| `peer_mac` | ServerHello.preprocess → verify+t3 | — |
| `own_mac` | — | ServerHello → ClientFinished.preprocess (t3) |
| `shared_secret` | ServerHello.preprocess → HKDF | ServerHello → HKDF |
| `handshake_key` | ServerHello.preprocess → verify+sign | ServerHello → mac+verify |
| `write_key` | ClientFinished → ClientData | ClientFinished.preprocess → ServerData |
| `read_key` | ClientFinished → ServerData.preprocess | ClientFinished.preprocess → ClientData.preprocess |

全部字段 **先设后用**，无遗漏无未定义。✅

### 3. 签名绑定

| 签名 | 覆盖内容 | 语义 |
|------|---------|------|
| `sig_s = Sign(server_id_sk, t1)` | cn, epk_c, sn, epk_s | Server 确认收到这些 DH 参数 |
| `sig_c = Sign(client_id_sk, t3)` | t2(=t1++sig_s), mac_s | Client 确认收到并验证了 server 签名和 MAC |

链式绑定：任意消息被篡改 → 后续哈希变 → 签名/MAC 验证失败。✅

### 4. 攻击模型

| 攻击 | 结果 |
|------|------|
| 替换 ClientHello 中的 epk_c | server 签名 t1(含篡改值) ≠ client 的 t1 → 验证失败 → DoS |
| 替换 ClientFinished 签名 | Ed25519 验证失败 → DoS |
| 重放整个会话 | 新 nonce → t1 不同 → 签名不匹配 |
| 身份密钥泄露（历史会话） | esk_c/esk_s 已销毁 → 无法恢复 shared_secret → 安全 |

### 5. 密钥衍生

```
prk = HMAC-SHA256(zeros, shared_secret)
handshake_key   = HMAC-SHA256(prk, "hs" ‖ 0x01)    (label 2 bytes)
client_write_key = HMAC-SHA256(prk, "c2s" ‖ 0x01)  (label 3 bytes)
server_write_key = HMAC-SHA256(prk, "s2c" ‖ 0x01)  (label 3 bytes)
```

信息字符串互不相同 → 三个独立密钥。✅

### 6. polyrole-cs 兼容性

- 所有状态为 tagged union ✅
- 每个 variant 为 `Data(Data_, NextState)` ✅
- process/preprocess 签名正确 ✅
- 状态转换图有效（无可达循环，Exit 终端） ✅
- Payload 为 codec 支持的固定数组和 `[]u8` ✅

### 7. 尺寸验证

| 项 | 大小 | 正确？ |
|----|------|:---:|
| ClientHello payload | 56 bytes | ✅ |
| ServerHello payload | 152 bytes | ✅ |
| ClientFinished payload | 96 bytes | ✅ |
| X25519 密钥 | 32 bytes | ✅ |
| Ed25519 签名 | 64 bytes | ✅ |
| X25519 公钥 | 32 bytes | ✅ |
| Ed25519 公钥 | 32 bytes | ✅ |
| HMAC-SHA256 输出 | 32 bytes | ✅ |
| XChaCha20 nonce | 24 bytes | ✅ |
| Poly1305 tag | 16 bytes | ✅ |

---

## 无缺陷。设计冻结，可提交代码实现。
