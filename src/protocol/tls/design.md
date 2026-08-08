# Simple TLS Protocol Design (with Forward Secrecy)

## Overview

A simplified TLS 1.3-style handshake protocol built on polyrole-cs. The
protocol establishes an encrypted session in three messages — identity
authentication via Ed25519 signatures, key agreement via ephemeral-ephemeral
X25519 ECDH, and transcript chaining via SHA256 hashes.

After the handshake, `ClientContext.write_key` / `read_key` and
`ServerContext.write_key` / `read_key` contain the derived symmetric keys.
The caller then creates a `TlsChannel` using these keys to encrypt arbitrary
protocol traffic. The handshake protocol itself has no data phase — it exits
immediately after key derivation.

The client knows the server's Ed25519 public key out-of-band (trust anchor /
certificate pinning); the server authenticates itself but does not know the
client — server-authenticated (HTTPS-style) one-way authentication. No
certificate exchange.

## State Machine

```
ClientHello ─(c)──▶ ServerHello ─(s)──▶ ClientFinished ─(c)──▶ Exit
```

Three handshake steps. No data phase — `ClientFinished.close` transitions
directly to `Exit`. Use `TlsChannel` for encrypted data transport instead.

## Error Handling

Cryptographic failures return typed errors instead of panicking:

```zig
const TlsError = error{
    EntropyUnavailable,  // CSPRNG failed
    DhFailed,            // X25519 key agreement failed (invalid peer key)
    SignatureInvalid,    // Ed25519 signature verification failed
    HmacInvalid,         // HMAC verification failed (tampered transcript or wrong key)
};
```

The Runner propagates these through `simulate` (`!void`) and `symmetric_run`
(`!void`). Cooperative close goes through the `Exit` transition; security
violations abort the protocol immediately via error return.

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
1. Generate random `client_nonce` (24 bytes) via `zio.randomSecure`.
2. Generate ephemeral X25519 keypair via `generateX25519Keypair()`.
3. Store `own_nonce`, `ephemeral_sk`, `own_ephemeral_pk` in context.
4. Return `.to_server` variant.

**preprocess (server):**
1. Store `peer_nonce` (= client nonce), `peer_ephemeral_pk` (= client epk) in context.

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

`transcript_1` binds both nonces and both ephemeral public keys.
`transcript_2` includes the server's own signature, so the server MAC
proves it computed the correct `shared_secret`.

**process (server):**
1. Generate random `server_nonce`, ephemeral X25519 keypair.
2. `shared_secret = X25519(esk_s, peer_ephemeral_pk)`.
3. Derive `handshake_key` via HKDF from `shared_secret`.
4. Compute `transcript_1`, sign it with `server_id_sk`.
5. Compute `transcript_2`, MAC it with `handshake_key`.
6. Store context fields, return `.to_client` variant.

**preprocess (client):**
1. `shared_secret = X25519(esk_c, peer_ephemeral_pk)`.
2. Derive `handshake_key`.
3. Compute `transcript_1`, verify `signature` against `server_id_pk`.
4. Compute `transcript_2`, verify `mac` against computed HMAC.
5. Store `peer_nonce`, `peer_ephemeral_pk`, `peer_signature`, `peer_mac` for
   ClientFinished transcript.

---

### Step 3 — ClientFinished

| Field | Agent |
|-------|-------|
| `ClientFinished` | client |

**Payload:**

| Name | Type | Description |
|------|------|-------------|
| `mac` | `[32]u8` | `HMAC-SHA256(handshake_key, "client_fin" \|\| transcript_3)` |

**Transcript chain (continued):**

```
transcript_3 = SHA256(transcript_2 ++ server_mac)
```

`transcript_3` binds the server's MAC (proving the server's Finished message
was authentic). The client MAC over `transcript_3` proves the client computed
the correct `shared_secret` — a session-possession proof, not an identity
proof (server-only authentication, HTTPS model).

**Variant — single transition:**

```zig
pub const ClientFinished = union(enum) {
    close: Data(ClientFinishedPayload, Exit),
};
```

Only one variant: the handshake always exits after key derivation. Data
exchange happens through `TlsChannel`, not through protocol-internal data
states.

**process (client):**
1. Compute `transcript_3` (using stored `peer_signature`, `peer_mac`).
2. MAC `transcript_3` with `handshake_key`.
3. Derive `write_key`, `read_key` via HKDF from `shared_secret`.
4. Return `.close` variant — Runner transitions to `Exit`.

**preprocess (server):**
1. Compute `transcript_3` (using stored `own_signature`, `own_mac`).
2. Verify client MAC. Failure → `error.HmacInvalid`.
3. Derive `read_key`, `write_key` via HKDF from `shared_secret`.

## Key Derivation

Using HKDF-SHA256 (RFC 5869, two-phase mode):

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
    id_keypair: crypto.sign.Ed25519.KeyPair, // own identity
    peer_id_public: Ed25519.PublicKey,       // server identity
    ephemeral_sk: [32]u8,                    // own X25519 secret
    own_ephemeral_pk: [32]u8,                // own X25519 public
    own_nonce: [24]u8,                       // generated in ClientHello
    peer_nonce: [24]u8,                      // received in ServerHello
    peer_ephemeral_pk: [32]u8,               // received in ServerHello
    peer_signature: [64]u8,                  // received in ServerHello
    peer_mac: [32]u8,                        // received in ServerHello
    shared_secret: [32]u8,                   // X25519 output
    handshake_key: [32]u8,                   // HKDF-derived
    write_key: [32]u8,                       // for TlsChannel send
    read_key: [32]u8,                        // for TlsChannel recv
};

pub const ServerContext = struct {
    id_keypair: crypto.sign.Ed25519.KeyPair, // own identity
    peer_id_public: Ed25519.PublicKey,       // client identity
    ephemeral_sk: [32]u8,                    // own X25519 secret
    own_ephemeral_pk: [32]u8,                // own X25519 public
    peer_ephemeral_pk: [32]u8,               // received in ClientHello
    peer_nonce: [24]u8,                      // received in ClientHello
    own_nonce: [24]u8,                       // generated in ServerHello
    shared_secret: [32]u8,                   // X25519 output
    handshake_key: [32]u8,                   // HKDF-derived
    own_signature: [64]u8,                   // computed in ServerHello
    own_mac: [32]u8,                         // computed in ServerHello
    read_key: [32]u8,                        // for TlsChannel recv
    write_key: [32]u8,                       // for TlsChannel send
};
```

No `send_buffer`, `recv_buffer`, `encrypted_buf`, or counters — these belong
to `TlsChannel`, not the handshake protocol.

## Security Properties

| Property | Mechanism |
|---|---|
| **Server authentication** | Ed25519 signature over `transcript_1` proves possession of `server_id_sk`. HMAC proves correct `shared_secret`. |
| **Client session proof** | `ClientFinished` HMAC over the full transcript proves the client computed the correct `shared_secret` (no client identity — anonymous client, HTTPS model). |
| **Forward secrecy** | `shared_secret = X25519(esk_c, esk_s)` — ephemeral keys discarded after session. |
| **Key confidentiality** | `shared_secret` never transmitted — both sides compute it from ephemeral public keys. |
| **Replay protection** | Fresh CSPRNG nonces per session. HMAC binds the full transcript. |
| **Transcript consistency** | Chained SHA256 hashes prevent truncation, reordering, or insertion. |
| **Key separation** | HKDF derives independent keys for handshake auth and each data direction. |
| **Post-compromise security** | Not provided. Identity key compromise enables future impersonation (but not past decryption). |

## Transcript Chain Summary

```
Step 1:  ClientHello sent     → { cn, epk_c }
Step 2:  ServerHello sent     → { sn, epk_s, sig_s, mac_s }

         t1 = SHA256(cn ++ epk_c ++ sn ++ epk_s)
         sig_s = Sign(server_id_sk, t1)
         t2 = SHA256(t1 ++ sig_s)
         mac_s = HMAC(hk, "server_fin" ++ t2)

Step 3:  ClientFinished sent  → { mac_c }

         t3 = SHA256(t2 ++ mac_s)
         mac_c = HMAC(hk, "client_fin" ++ t3)
```

cn = client_nonce, epk_c = client ephemeral pk,
sn = server_nonce, epk_s = server ephemeral pk,
hk = handshake_key

## Architecture: TLS + TlsChannel

The TLS handshake produces symmetric keys. Data transport is delegated to
`TlsChannel` (see `src/channel.zig`):

```
TCP stream
  ├─ StreamChannel ── TLS handshake ── get keys
  ├─ StreamChannel.deinit (frees buffers, stream stays open)
  └─ TlsChannel.init(stream, write_key, read_key)
       └─ sends encrypted protocol messages
```

`TlsChannel` wraps a `StreamChannel` with NaCl SecretBox AEAD encryption.
Each record is framed as `nonce(24) || tag(16) || ct_len(4 BE) || ciphertext`.
The nonce embeds a monotonic u64 counter plus a record-type byte (data /
KeyUpdate), both AEAD-authenticated, for replay/reordering/tamper protection.

In-band key rotation (TLS 1.3 KeyUpdate style): the sender lazily triggers on
a record-count threshold (default 2^28) or time interval (default 10 min),
seals a KeyUpdate record (plaintext = new epoch) and derives the next key
via HKDF-SHA256; the receiver advances its read key in stream order and
absorbs the record transparently. See `src/channel.zig` (`RotationConfig`).

## Dependencies

- `std.crypto.dh.X25519` — ephemeral-ephemeral Diffie-Hellman
- `std.crypto.sign.Ed25519` — identity signatures
- `std.crypto.auth.hmac.sha2.HmacSha256` — HMAC for Finished messages
- `std.crypto.hash.sha2.Sha256` — transcript hashing and HKDF
- `std.crypto.timing_safe` — constant-time MAC comparison
- `zio.randomSecure` — CSPRNG via kernel entropy
- `polyrole_cs` — state machine framework
- `polyrole_cs.channel.TlsChannel` — encrypted data transport
