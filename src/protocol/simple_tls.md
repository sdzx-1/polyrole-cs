# Simple TLS Protocol Design (with Forward Secrecy)

## Overview

A simplified TLS 1.3-style protocol built on polyrole-cs state machine
framework. No certificate exchange — both parties already know each other's
public key out-of-band.

Forward secrecy is provided via ephemeral-ephemeral X25519 ECDH, with
Ed25519 signatures for identity authentication.

## Prerequisites

- Client holds:
  - `client_id_sk` (Ed25519 secret key, 64 bytes)
  - `server_id_pk` (Ed25519 public key, 32 bytes)
- Server holds:
  - `server_id_sk` (Ed25519 secret key, 64 bytes)
  - `client_id_pk` (Ed25519 public key, 32 bytes)

## State Machine

```
ClientHello ─(c)──▶ ServerHello ─(s)──▶ ClientFinished ─(c)──▶ Exit
                                                                  │
                                              ┌───────────────────┘
                                              ▼
                                   ClientData ─(alt)──▶ ServerData ─(alt)──▶ ...
                                              │                     │
                                              └──▶ Exit             └──▶ Exit
```

Three handshake steps (down from four — Server and Client authenticate in
two messages total). Forward secrecy is achieved because ephemeral X25519
keys are destroyed after each session.

## State-by-State Detail

### Step 1 — ClientHello

| Field | Agent |
|-------|-------|
| `ClientHello` | client |

**Payload:**

| Name | Type | Description |
|------|------|-------------|
| `nonce` | `[24]u8` | Random client nonce |
| `ephemeral_pk` | `[32]u8` | Client's ephemeral X25519 public key |

**process (client):**
1. Generate random `client_nonce` (24 bytes).
2. Generate ephemeral X25519 keypair → `(esk_c, epk_c)`.
3. Store `client_nonce`, `esk_c`, `own_ephemeral_pk` (= `epk_c`) in context.
4. Return `.client_hello` variant.

**preprocess (server):**
1. Store `peer_nonce` (= `client_nonce`), `peer_ephemeral_pk` (= `epk_c`) in context.

---

### Step 2 — ServerHello

| Field | Agent |
|-------|-------|
| `ServerHello` | server |

**Payload:**

| Name | Type | Description |
|------|------|-------------|
| `nonce` | `[24]u8` | Random server nonce |
| `ephemeral_pk` | `[32]u8` | Server's ephemeral X25519 public key |
| `signature` | `[64]u8` | `Ed25519.sign(server_id_sk, transcript_1)` |
| `mac` | `[32]u8` | `HMAC-SHA256(handshake_key, "server_fin" \|\| transcript_2)` |

**Transcript chain:**

```
transcript_1 = SHA256(client_nonce ++ epk_c ++ server_nonce ++ epk_s)
transcript_2 = SHA256(transcript_1 ++ signature)
```

`transcript_1` binds both nonces and both ephemeral public keys — the entire
DH negotiation. No message before this point was authenticated, but any
tampering will cause the signature verification in the next step to fail.

`transcript_2` includes the signature. The server MAC covers
`transcript_2`, proving the server not only signed correctly but also
computed the correct `shared_secret` (from which `handshake_key` derives).

**process (server):**
1. Generate random `server_nonce` (24 bytes).
2. Generate ephemeral X25519 keypair → `(esk_s, epk_s)`.
3. Compute `shared_secret = X25519(esk_s, peer_ephemeral_pk)`.
4. Derive `handshake_key` from `shared_secret` (see Key Derivation).
5. Compute `transcript_1`, `transcript_2`.
6. Compute `signature = Ed25519.sign(server_id_sk, transcript_1)`.
7. Compute `mac = HMAC(handshake_key, "server_fin" ++ transcript_2)`.
8. Store `server_nonce`, `esk_s`, `own_ephemeral_pk` (= `epk_s`), `shared_secret`, `handshake_key`, `own_mac` (= `mac`) in context.
9. Return `.server_hello` variant.

**preprocess (client):**
1. Store `peer_nonce` (= `server_nonce`), `peer_ephemeral_pk` (= `epk_s`), `signature`, `mac` from payload.
2. Compute `shared_secret = X25519(esk_c, peer_ephemeral_pk)`.
3. Derive `handshake_key` from `shared_secret`.
4. Compute `transcript_1`, `transcript_2`.
5. Verify `Ed25519.verify(signature, transcript_1, server_id_pk)`. On failure → `@panic`.
6. Compute `expected_mac = HMAC(handshake_key, "server_fin" ++ transcript_2)`.
7. Verify `mac == expected_mac`. On failure → `@panic`.
8. Store `shared_secret`, `handshake_key` in context.

---

### Step 3 — ClientFinished

| Field | Agent |
|-------|-------|
| `ClientFinished` | client |

**Payload:**

| Name | Type | Description |
|------|------|-------------|
| `signature` | `[64]u8` | `Ed25519.sign(client_id_sk, transcript_3)` |
| `mac` | `[32]u8` | `HMAC-SHA256(handshake_key, "client_fin" \|\| transcript_4)` |

**Transcript chain (continued):**

```
transcript_3 = SHA256(transcript_2 ++ server_mac)
transcript_4 = SHA256(transcript_3 ++ signature)
```

`transcript_3` binds the server's MAC (which itself covers `transcript_2`).
The client signs this, proving possession of `client_id_sk` and confirming
receipt of the authentic server MAC.

`transcript_4` includes the client signature. The client MAC covers this,
proving the client also computed the correct `shared_secret`.

**process (client):**
1. Compute `transcript_3`, `transcript_4` (using stored server MAC).
2. Compute `client_signature = Ed25519.sign(client_id_sk, transcript_3)`.
3. Compute `client_mac = HMAC(handshake_key, "client_fin" ++ transcript_4)`.
4. Derive application keys (`write_key`, `read_key`) from `shared_secret` (see Key Derivation).
5. Return `.client_finished` variant (send or close).

**preprocess (server):**
1. Compute `transcript_3`, `transcript_4` (using stored server MAC).
2. Verify `Ed25519.verify(client_signature, transcript_3, client_id_pk)`. On failure → `@panic`.
3. Compute `expected_mac = HMAC(handshake_key, "client_fin" ++ transcript_4)`.
4. Verify `client_mac == expected_mac`. On failure → `@panic`.
5. Derive application keys from `shared_secret`.

**Variant design:**

`ClientFinished` has two transitions: enter the data phase or exit.
Both carry the same payload (signature + MAC) — the variant tag determines
the next state.

```zig
pub const ClientFinished = union(enum) {
    enter_data: Data(ClientFinishedPayload, ClientData),
    close: Data(ClientFinishedPayload, Exit),
};
```

---

### Data Phase — ClientData / ServerData

Identical to the non-FS design. XChaCha20-Poly1305 with strict alternation.

**Payload:**

| Name | Type | Description |
|------|------|-------------|
| `nonce` | `[24]u8` | Random nonce for XChaCha20-Poly1305 |
| `tag` | `[16]u8` | Poly1305 authentication tag |
| `ciphertext` | `[]u8` | Encrypted plaintext |

```zig
pub const ClientData = union(enum) {
    send: Data(Ciphertext, ServerData),
    close: Data(void, Exit),
};

pub const ServerData = union(enum) {
    send: Data(Ciphertext, ClientData),
    close: Data(void, Exit),
};
```

## Key Derivation

Using HKDF-SHA256 (RFC 5869) in two-phase mode:

```
Phase 1 — Extract (salt = zero-filled 32 bytes):
    prk = HKDF-Extract(salt, shared_secret)
        = HMAC-SHA256(zeroes, shared_secret)

Phase 2 — Expand (each key gets its own info label):
    handshake_key   = HMAC-SHA256(prk, "hs"  || 0x01)
    client_write_key = HMAC-SHA256(prk, "c2s" || 0x01)
    server_write_key = HMAC-SHA256(prk, "s2c" || 0x01)
```

**Direction mapping:**

| Role | `write_key` | `read_key` |
|------|-------------|------------|
| Client | `client_write_key` | `server_write_key` |
| Server | `server_write_key` | `client_write_key` |

## Context Definitions

```zig
pub const ClientContext = struct {
    /// Own Ed25519 identity secret key
    id_secret: [64]u8,
    /// Server's Ed25519 identity public key
    peer_id_public: [32]u8,
    /// Own ephemeral X25519 secret key (generated in ClientHello)
    ephemeral_sk: [32]u8,
    /// Own ephemeral X25519 public key (generated in ClientHello)
    own_ephemeral_pk: [32]u8,
    /// Generated in ClientHello
    own_nonce: [24]u8,
    /// Received in ServerHello
    peer_nonce: [24]u8,
    /// Peer's ephemeral X25519 public key (received in ServerHello)
    peer_ephemeral_pk: [32]u8,
    /// Peer's signature (received in ServerHello)
    peer_signature: [64]u8,
    /// Peer's Finished MAC (received in ServerHello)
    peer_mac: [32]u8,
    /// X25519 shared secret = derived session key
    shared_secret: [32]u8,
    /// Derived from shared_secret
    handshake_key: [32]u8,
    /// Derived: encrypt ClientData
    write_key: [32]u8,
    /// Derived: decrypt ServerData
    read_key: [32]u8,
    /// Plaintext to send in ClientData.process (set by application)
    send_buffer: []const u8,
    /// Plaintext received in ServerData.preprocess
    recv_buffer: []u8,
};

pub const ServerContext = struct {
    /// Own Ed25519 identity secret key
    id_secret: [64]u8,
    /// Client's Ed25519 identity public key
    peer_id_public: [32]u8,
    /// Own ephemeral X25519 secret key (generated in ServerHello)
    ephemeral_sk: [32]u8,
    /// Own ephemeral X25519 public key (generated in ServerHello)
    own_ephemeral_pk: [32]u8,
    /// Peer's ephemeral X25519 public key (received in ClientHello)
    peer_ephemeral_pk: [32]u8,
    /// Received in ClientHello
    peer_nonce: [24]u8,
    /// Generated in ServerHello
    own_nonce: [24]u8,
    /// X25519 shared secret = derived session key
    shared_secret: [32]u8,
    /// Derived from shared_secret
    handshake_key: [32]u8,
    /// Own Finished MAC (computed in ServerHello, used in ClientFinished transcript)
    own_mac: [32]u8,
    /// Derived: decrypt ClientData
    read_key: [32]u8,
    /// Derived: encrypt ServerData
    write_key: [32]u8,
    /// Plaintext to send in ServerData.process (set by application)
    send_buffer: []const u8,
    /// Plaintext received in ClientData.preprocess
    recv_buffer: []u8,
};
```

## Security Properties

| Property | Mechanism |
|---|---|
| **Client authentication** | Ed25519 signature over `transcript_3` proves possession of `client_id_sk`. HMAC proves correct `shared_secret` derivation. |
| **Server authentication** | Ed25519 signature over `transcript_1` proves possession of `server_id_sk`. HMAC proves correct `shared_secret` derivation. |
| **Forward secrecy** | `shared_secret = X25519(esk_c, esk_s)` — ephemeral keys discarded after session. Compromise of identity keys reveals nothing about past sessions. |
| **Key confidentiality** | `shared_secret` never transmitted — both sides compute it independently from ephemeral public keys. |
| **Replay protection** | Fresh random nonces per session. HMAC binds the full transcript. |
| **Transcript consistency** | Chained SHA256 hashes prevent truncation, reordering, or message insertion. Each signature and MAC covers all preceding data. |
| **Key separation** | HKDF derives independent keys for handshake auth and each data direction. |
| **Post-compromise security** | Not provided. New sessions use fresh ephemeral keys, but if an attacker actively steals an identity key they can impersonate that party in new sessions. |

## Transcript Chain Summary

```
Step 1:  ClientHello sent     → { cn, epk_c }
Step 2:  ServerHello sent     → { sn, epk_s, sig_s, mac_s }

         t1 = SHA256(cn ++ epk_c ++ sn ++ epk_s)
         sig_s = Sign(server_id_sk, t1)
         t2 = SHA256(t1 ++ sig_s)
         mac_s = HMAC(hk, "server_fin" ++ t2)

Step 3:  ClientFinished sent  → { sig_c, mac_c }

         t3 = SHA256(t2 ++ mac_s)
         sig_c = Sign(client_id_sk, t3)
         t4 = SHA256(t3 ++ sig_c)
         mac_c = HMAC(hk, "client_fin" ++ t4)
```

cn = client_nonce, epk_c = client ephemeral pk,
sn = server_nonce, epk_s = server ephemeral pk,
hk = handshake_key

## Comparison: Without vs. With Forward Secrecy

| | Without FS (static box) | With FS (ephemeral DH + sig) |
|---|---|---|
| Handshake steps | 4 | 3 |
| Identity key type | X25519 (DH + auth) | Ed25519 (sig only) |
| State transitions per message | 1 payload | ServerHello: 4 fields (nonce + pk + sig + mac) |
| `session_key` origin | server generates randomly | `X25519(esk_c, esk_s)` — joint derivation |
| Security if identity key leaks | All past sessions decrypted | Past sessions safe; future sessions impersonatable |

## Error Handling

Same as before: cryptographic failures → `@panic`. This is an example protocol.

## Dependencies

- `std.crypto.dh.X25519` — ephemeral-ephemeral Diffie-Hellman
- `std.crypto.sign.Ed25519` — identity signatures
- `std.crypto.aead.xchacha20_poly1305` — symmetric AEAD for data phase
- `std.crypto.auth.hmac.sha2.HmacSha256` — HMAC for Finished messages
- `std.crypto.hash.sha2.Sha256` — transcript hashing and HKDF
- `polyrole_cs` — state machine framework
