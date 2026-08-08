# polyrole-cs

A Zig compile-time state-machine protocol framework for the **Client-Server**
communication model.

Protocols are declared as tagged-union state graphs. The framework performs
reachability analysis, context-type validation, and state-ID generation at
compile time, with **zero runtime dispatch overhead**. The same protocol code
runs unchanged from in-process simulation to encrypted, multiplexed network
transport.

```zig
const polyrole = @import("polyrole_cs");
```

## Why

When hand-writing Client-Server protocols, every message drags in four
repetitive concerns: **send/receive dispatch, serialization, state
transition, and the transport layer**. They get tangled together; changing
the protocol means touching all four at once.

polyrole-cs separates protocol **declaration** from **execution**:

- A protocol is a tagged-union state graph (pure declaration — no send/receive or serialization code)
- Execution is provided by the framework's three modes (simulate / network / multiplex), with zero changes to the protocol code
- State transitions, context types, and message validity are all checked at **compile time**; runtime dispatch is free

Note: "no send/receive code" means the framework owns transport and
serialization. `process`/`preprocess` state functions are free to run
arbitrary logic, including blocking I/O (database access, files,
cryptographic work, sleeps) — they just never touch the channel directly.

The result: one protocol definition runs unchanged in unit tests, end-to-end
over the network, and across multiple protocols sharing a single connection.

## Quick start

A Client-Server counter protocol — the complete runnable code lives in
`src/test/quickstart_test.zig`:

```zig
const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");

// Protocol info: protocol name + both sides' context types (consistency
// is enforced at compile time)
const Info = polyrole.ProtocolInfo("counter", i32, i32);

const Counter = struct {
    // B: server state — +1 on each add, finish at 10
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

    // A: client state — issues add
    pub const A = union(enum) {
        add: polyrole.Data(void, B),
        pub const info: Info = .{ .agent = .client, .name = "A" };
        pub fn process(ctx: *i32) @This() { _ = ctx; return .add; }
    };
};
```

**Mode 1: simulate** — single-threaded in-memory simulation, zero
serialization, pure logic testing:

```zig
const R = polyrole.runner.Runner(Counter.A);
var client: i32 = 0;
var server: i32 = 0;
try R.simulate(&client, &server, Counter.A);
// server == 10
```

**Mode 2: symmetric_run** — both ends run over a network/channel, protocol
code unchanged:

```zig
// Server side
try R.symmetric_run(.server, &server_ctx, &ch_s, Counter.A, null);
// Client side (concurrent task)
try R.symmetric_run(.client, &client_ctx, &ch_c, Counter.A, null);
```

## Core concepts

### State

Each state is a tagged union. `info` declares the agent (`.client` /
`.server`) and the protocol name; `process(ctx)` runs on the agent side and
produces a transition, while `preprocess(ctx, result)` runs on the peer
side — validating, updating context, or aborting:

```zig
pub const ClientHello = union(enum) {
    to_server: polyrole.Data(ClientHelloPayload, ServerHello),
    pub const info: MyProtocol = .{ .agent = .client, .name = "ClientHello" };
    pub fn process(ctx: *ClientCtx) !@This() { ... }
    pub fn preprocess(ctx: *ServerCtx, result: @This()) !void { ... }
};
```

### Transitions (Data)

Every variant carries `Data(Payload, NextState)` — the payload and the next
state:

```zig
to_server: polyrole.Data(ClientHelloPayload, ServerHello),
//                                  ^payload            ^next state
```

### Exit

Every protocol's final state is `polyrole.Exit` — the framework's single
built-in terminal state.

## Execution modes

| Mode | Use case | Shape |
|------|----------|-------|
| `simulate` | Local logic tests | Both ends alternate on one thread, zero serialization |
| `symmetric_run` | Single protocol, end to end | One connection, one protocol, timeout support |
| `Mux` | Multiple protocols, end to end | One connection, N protocols, optional batch-record encryption |

### Mux — the multiplexed transport layer (core feature)

Multiple protocols share one underlying connection; each protocol runs its
own state machine — independent buffering, independent timeouts, no mutual
blocking — at nearly the same transport cost as a single protocol. This
solves the real problem of "one connection carrying multiple semantics"
(control plane + push plane, signaling + data) by making multiplexing a
framework capability.

**Mechanism**: each protocol gets a `SubChannel` (independent
`send_buff`/`recv_buff`). A send-credit mechanism guarantees at most one
in-flight message per protocol at any moment; a `writer_loop` copies an
aggregated round of frames into one contiguous buffer and writes it out in
one call; a `reader_loop` reads a whole batch and demultiplexes frames by
their headers.

**Wire format (batch records)**: `[total_len u32 BE][frames...]`, each
frame `[protocol_id u8][payload_len u16 BE][payload]`.

```zig
const Mux = polyrole.runner.Mux;

// Declare the protocol family (one Protocol entry per protocol)
const protocols = [_]polyrole.runner.Protocol{
    .{ .enter = Ctrl.A, .runner = R_ctrl, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
    .{ .enter = Push.A, .runner = R_push, .client_ct = i32, .server_ct = i32,
       .max_massage_size = 1024, .recv_timeout_ms = null },
};

// Plaintext mode: pass null keys
const TmpMux = Mux(&protocols, false);
var mux: TmpMux = undefined;
try mux.init(gpa, &sc, null);
try mux.run(.client, ctxs);   // ctxs: tuple of per-protocol contexts

// Encrypted mode: the whole batch is sealed as one AEAD record; keys come
// from the TLS handshake (or out-of-band negotiation)
const TmpMux = Mux(&protocols, true);
try mux.init(gpa, &sc, .{ .write_key = wk, .read_key = rk });
try mux.run(.server, ctxs);
```

Encryption details: the whole batch record is AEAD-authenticated, record
length is u32 (batch plaintext may exceed 64 KiB); key rotation
(KeyUpdate) is absorbed transparently at the Mux layer. The `SubChannel`
interface is identical to `StreamChannel`, so protocol code is untouched.

## Modules

| Module | Path | Description |
|--------|------|-------------|
| `runner` | `src/runner.zig` | State-machine driver: `simulate()`, `symmetric_run()`; multiplexed transport `Mux()` |
| `channel` | `src/channel.zig` | Channel abstraction: `StreamChannel` (plaintext TCP), `InMemoryChannel` (in-process pipe), `TlsChannel` (AEAD encryption + key rotation) |
| `codec` | `src/codec.zig` | Binary codec: state ID + tag + payload |
| `Graph` | `src/Graph.zig` | DOT-format state-graph generation |
| `tls` | `src/protocol/tls/` | Minimal authenticated handshake protocol (server-auth) |

### Channel

**StreamChannel** — plaintext TCP channel:

```zig
var ch: StreamChannel = undefined;
try ch.init(allocator, stream, read_buf_size, write_buf_size, max_slice_len);
defer ch.deinit(allocator);
```

**InMemoryChannel** — in-process full-duplex pipe (two paired
`HalfChannel`s referencing each other); no network I/O; at most one message
in flight per direction.

**TlsChannel** — AEAD-encrypted channel layered over a StreamChannel, using
NaCl SecretBox:

```zig
var tc: TlsChannel = undefined;
try tc.init(allocator, &sc, write_key, read_key, 512);  // 512 = per-record buffer cap
defer tc.deinit(allocator);
```

Each record's wire format: `nonce(24) || tag(16) || ct_len(4 BE) || ciphertext`,
nonce = `counter(8 BE) || 0(15) || type(1)` — the type byte is AEAD-authenticated,
so record types (data / KeyUpdate) cannot be tampered with.

**Key rotation** (isomorphic to TLS 1.3 KeyUpdate, tunable via
`setRotationConfig`):
- The sender lazily checks the trigger at the `sealAndSend` entry — a
  written-record threshold (default 2^28, the AEAD mathematical safety
  bound) or a time interval (default 10 minutes; idle connections never
  emit empty rotation packets);
- On trigger, it sends one KeyUpdate record with the current key (plaintext
  = new epoch), then derives the next-generation write key locally via
  one-way HKDF and resets the write counter (nonces never repeat);
- The receiver, on reading a KeyUpdate in order, derives the matching read
  key and resets its read counter; the record is **absorbed transparently**,
  invisible to the layers above (Mux / symmetric_run / protocol state
  machines).

### Codec

Binary serialization format:

```
state_id(4 BE) || tag(1) || payload
```

Supported types: `void`, `bool`, integers, `[]const u8`, fixed-length byte
arrays, structs.

### Graph

Generate DOT-format state graphs:

```zig
var graph = try polyrole.Graph.initWithFsm(allocator, P.A);
defer graph.deinit();
try graph.generateDot(.{}, &writer.interface);
```

Render with Graphviz: `dot -Tpng graph.dot -o graph.png`

## Example protocol

### Minimal authenticated handshake (server authentication)

Shows the framework's complete use on a security protocol: three messages
negotiate keys and authenticate the **Server** (the HTTPS model — the
Client holds the Server's public key out-of-band; the Server need not know
the Client's identity and can serve arbitrary clients):

- `ClientHello → ServerHello → ClientFinished → Exit`
- X25519 ephemeral key agreement (PFS) + Server Ed25519 identity signature + mutual HMAC
- The Server signature covers both ephemeral public keys, so a MITM cannot substitute them (anti-spoofing)
- ClientFinished proves session possession and carries no client identity

```zig
const tls = polyrole.tls;

// server_pk: Client's out-of-band Server public key; server_kp: Server identity keypair
var client_ctx = tls.ClientContext.init(server_pk);
var server_ctx = tls.ServerContext.init(server_kp);

const R = polyrole.runner.Runner(tls.ClientHello);
try R.simulate(&client_ctx, &server_ctx, tls.ClientHello);

// client_ctx.write_key / read_key are the derived symmetric keys
```

Network deployment: `symmetric_run` runs over a `StreamChannel` (or Mux in
encrypted mode); the `TlsChannel` created after the handshake supports key
rotation.

**Production deployments must set a handshake timeout** — the
`recv_timeout_ms` argument of `symmetric_run` (e.g. 10 seconds). Otherwise
a malicious or slow peer can hold a connection open without sending
anything and block the server forever (handshake DoS):

```zig
// Handshake phase: 10s timeout (aborts with error.ReadFailed / error.Canceled)
try R.symmetric_run(.server, &server_ctx, &ch, tls.ClientHello, 10_000);
// Data phase: business messages over TlsChannel can use their own timeout
```

**Security boundary**:

| Provides | Does not provide |
|----------|------------------|
| Server identity authentication (MITM cannot impersonate the Server) | Client identity authentication (Clients are anonymous) |
| Confidentiality / integrity / replay protection / PFS | Protection against "any stranger claiming an identity" — that needs an application layer (login credentials / allowlist registration) |

**Production notes**:

- **Trust anchor distribution**: security rests on the Client's out-of-band
  Server public key being genuine — distribute it over a secure channel or
  pin it; otherwise every check builds on a false anchor
- **Single cipher suite**: no negotiation; fixed X25519 + Ed25519 +
  XSalsa20-Poly1305 + HKDF-SHA256; algorithm evolution requires versioning
  the protocol
- **Custom record format**: `TlsChannel`'s record format
  (nonce || tag || ct_len || ct) is custom, not the standard TLS wire
  format — it interoperates only with this library

## Testing

All tests live in `src/test/` (run with `zig build test`):

| File | Coverage |
|------|----------|
| `quickstart_test.zig` | Quick-start example (simulate + InMemoryChannel symmetric run) |
| `codec_test.zig` | Codec malformed input (invalid booleans, out-of-range tags, oversized slices) |
| `channel_test.zig` | AEAD error paths (replay/tamper/length), key rotation, in-memory full-duplex channel |
| `runner_test.zig` | simulate / symmetric_run / timeout / TLS encrypted channel / Mux plaintext + encrypted |
| `tls_test.zig` | Handshake protocol (signature/MAC/ephemeral-key tampering, replay, session isolation, cross-session ClientFinished replay, MAC domain separation) |

## Documentation

| Document | Description |
|----------|-------------|
| `README.md` | Framework overview and usage guide (Chinese) |
| `README_EN.md` | This file: English overview and usage guide |
| `desigen.md` | Design notes for the Mux transport (wb/rb buffer model, frame aggregation constraints) |
| `src/protocol/tls/README.md` | Minimal TLS handshake protocol design (Chinese) |
| `src/protocol/tls/design.md` | Minimal TLS handshake protocol design (English) |

## Error handling

State functions may return error unions. The Runner detects the return
type at compile time:
- Returning `@This()` → ordinary call; the protocol continues.
- Returning `!@This()` → `try`-called; the error propagates to the caller
  and the protocol terminates immediately.

```zig
pub fn process(ctx: *Ctx) !@This() {
    const key = loadKey() catch return error.KeyNotFound;
    // ...
}
```

## Installation

From your project root:

```shell
zig fetch --save git+https://github.com/sdzx-1/polyrole-cs.git
```

**build.zig:**

```zig
const polyrole_cs = b.dependency("polyrole_cs", .{});
exe.root_module.addImport("polyrole_cs", polyrole_cs.module("polyrole_cs"));
```

## Running the tests

```bash
zig build test
```

## License

MIT
