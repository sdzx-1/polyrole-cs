# Network Monitor Protocol Design

## Overview

A session-based network connectivity and latency monitoring protocol built
on polyrole-cs. The client sends periodic pings; the server echoes them back
verbatim. Each response is recorded as a per-ping `PingResult` in an
`ArrayList` on the client.

All timing uses milliseconds.

**Transport-agnostic.** Works over StreamChannel, TlsChannel, or any
transport implementing send/recv.

## State Machine

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingQuery (cycle)
  │                                    │
  └───────── close → Exit ◀────────────┘
```

Two states. `PingQuery` handles both the send and the continue/close
decision. `PingResponse` is a pure echo.

## Payload

```zig
pub const PingPayload = struct { seq_num: u64, };
pub const PongPayload = struct { seq_num: u64, };
```

## Context

```zig
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
    timestamp: zio.Timestamp,
};

pub const ClientContext = struct {
    allocator: std.mem.Allocator,
    seq_num: u64 = 0,
    last_send_ms: u64 = 0,           // local timestamp, used for RTT
    remaining: u32 = 0,              // total pings (> 0)
    interval_ms: u64 = 0,            // sleep between pings
    file: ?zio.File = null,          // optional CSV output
    results: std.ArrayList(PingResult),
};

pub const ServerContext = struct {
    last_seq_num: u64 = 0,
};
```

## States

### PingQuery (client)

`process()`: if `remaining == 0`, returns `.close → Exit`. Otherwise
sleeps `interval_ms` (skip on first entry when `seq_num == 0`),
decrements `remaining`, records `last_send_ms`, increments `seq_num`,
and sends `.to_server → PingResponse`. Returns `!@This()`.

`preprocess()`: server stores `seq_num` for echo-back. On the `.close`
variant, the server simply passes through to Exit.

### PingResponse (server → client)

Pure echo — `process()` returns the `seq_num` stored by PingQuery's
preprocess.

`preprocess()` computes `rtt_ms = now - last_send_ms` and appends a
`PingResult` to `results`. Returns `!void` — allocation failure.

## Caller Example

```zig
var client = ClientContext{
    .allocator = allocator,
    .remaining = 60,
    .interval_ms = 1000,
    .results = std.ArrayList(PingResult).empty,
};
defer client.deinit();
var server = ServerContext{};

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery, null);

for (client.results.items) |r| {
    std.debug.print("[{d}] rtt={d}ms\n", .{ r.seq_num, r.rtt_ms });
}
```

## Error Propagation

`PingQuery.process` and `PingResponse.preprocess` return errors.
Runner detects at compile time and uses `try`.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Stateless server | Scales to arbitrary concurrent clients |
| Millisecond units | Network RTT is ms-scale; ns adds noise |
| Per-ping records | Simpler than windowed aggregation — caller groups as needed |
| zio native | Portable clock and sleep via `zio.Timestamp` / `zio.sleep` |
| No panics | Errors propagate through Runner |
| 2-state machine | Merged PingDecision into PingQuery — sleep before send avoids Nagle |
