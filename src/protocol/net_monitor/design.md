# Network Monitor Protocol Design

## Overview

A session-based network connectivity and latency monitoring protocol built
on polyrole-cs. The client sends periodic pings; the server echoes them back
verbatim. RTT is computed entirely on the client side using a single clock
— no cross-machine time synchronization required.

All timing uses **milliseconds** throughout. Network RTT is inherently a
millisecond-scale measurement — nanosecond precision adds no value.

**Transport-agnostic.** Works over StreamChannel, TlsChannel, or any
transport implementing send/recv.

## State Machine

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (cycle)
                                                          │
                                                          └── close → Exit
```

## Clock-Independent RTT

| Field | Type | Filled by | Purpose |
|-------|------|-----------|---------|
| `seq_num` | `u64` | client, echoed by server | Ordering, diagnostic tracing |
| `client_send_time` | `u64` | client, echoed by server | RTT = now - client_send_time (both on client clock) |
| `server_dwell_ms` | `u64` | server | Server processing dwell in milliseconds |

## Payload

```zig
pub const PingPayload = struct { seq_num: u64, client_send_time: u64, };
pub const PongPayload = struct { seq_num: u64, client_send_time: u64, server_dwell_ms: u64, };
```

## Context

```zig
pub const WindowMetrics = struct {
    start_ms: u64 = 0,
    rtt_sum_ms: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ms: u64 = std.math.maxInt(u64),
    rtt_max_ms: u64 = 0,
};

pub const ClientContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    seq_num: u64 = 0,
    remaining: u32 = 0,              // total pings (> 0)
    interval_ms: u64 = 0,            // sleep between pings
    window_duration_ms: u64 = 0,     // per-window width (> 0, e.g. 60000 = 1 min)
    session_start_ms: u64 = 0,       // 0 = not started
    windows: std.ArrayList(WindowMetrics),
};

pub const ServerContext = struct {
    io: std.Io,
    last_seq_num: u64 = 0,
    last_client_send_time: u64 = 0,
};
```

## States

### PingQuery (client → server)

`process()` records `session_start_ms` (once), increments `seq_num`,
and sends the ping with a monotonic millisecond timestamp.

`preprocess()` stores the received fields in `ServerContext` for echo-back.

### PingResponse (server → client)

`process()` samples the monotonic clock before and after, computing
`server_dwell_ms` as the difference.

`preprocess()` computes `rtt_net`, determines the window index by
response arrival time, grows the window list, and accumulates metrics.
Returns `!void` — allocation failure.

### PingDecision (client)

`process()` decrements `remaining`. If 0, returns `.close`. Otherwise
sleeps `interval_ms` and returns `.ping_again`. Returns `!@This()` —
sleep cancellation.

`remaining` counts **total** pings including the first. `remaining = 5`
produces exactly 5 pings. Must be > 0.

## Caller Example

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ms = 1000,
    .window_duration_ms = 60000,
    .windows = std.ArrayList(WindowMetrics).empty,
};
defer client.windows.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue;
    std.debug.print("window {d}: avg={d} ms\n", .{ i, w.rtt_sum_ms / w.rtt_count });
}
```

## Error Propagation

`PingDecision.process` and `PingResponse.preprocess` return errors.
The Runner detects these at compile time and uses `try`.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Stateless server | Scales to arbitrary concurrent clients |
| Clock-independent timing | RTT uses client clock; dwell uses server-local relative time |
| Millisecond units everywhere | Network RTT is ms-scale; ns adds noise, not value |
| Io interface | `Io.Timestamp.now(io, .awake)` / `Io.sleep` — portable |
| Error propagation | No panics — failures return through Runner |
| Gap-filling window append | `while items.len <= index` fills gaps from slow responses |
