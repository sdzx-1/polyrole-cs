# Network Monitor Protocol Design

## Overview

A session-based network connectivity and latency monitoring protocol built
on polyrole-cs. The client sends periodic pings; the server echoes them back
verbatim. Each response is recorded as a per-ping `PingResult` in an
`ArrayList` on the client, with an optional `max_results` cap.

All timing uses milliseconds.

**Transport-agnostic.** Works over StreamChannel, TlsChannel, or any
transport implementing send/recv.

## State Machine

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (cycle)
                                                          │
                                                          └── close → Exit
```

## Payload

```zig
pub const PingPayload = struct { seq_num: u64, client_send_time: u64, };
pub const PongPayload = struct { seq_num: u64, client_send_time: u64, server_dwell_ms: u64, };
```

## Context

```zig
pub const PingResult = struct {
    seq_num: u64,
    rtt_ms: u64,
    server_dwell_ms: u64,
};

pub const ClientContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    seq_num: u64 = 0,
    remaining: u32 = 0,              // total pings (> 0)
    interval_ms: u64 = 0,            // sleep between pings
    max_results: u32 = 0,            // 0 = unlimited, else cap
    results: std.ArrayList(PingResult),
};

pub const ServerContext = struct {
    io: std.Io,
    last_seq_num: u64 = 0,
    last_client_send_time: u64 = 0,
};
```

## States

### PingQuery (client → server)

`process()` increments `seq_num` and sends the ping with a monotonic
millisecond timestamp.

`preprocess()` stores the received fields in `ServerContext` for echo-back.

### PingResponse (server → client)

`process()` samples the clock before and after, computing
`server_dwell_ms` as the difference.

`preprocess()` computes `rtt_ms = now - client_send_time - server_dwell_ms`,
then appends a `PingResult` to `results` (subject to `max_results` cap).
Returns `!void` — allocation failure.

### PingDecision (client)

`process()` decrements `remaining`. If 0, returns `.close`. Otherwise
sleeps `interval_ms` and returns `.ping_again`. Returns `!@This()`.

`remaining` counts **total** pings including the first. Must be > 0.

## Caller Example

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ms = 1000,
    .max_results = 1000,
    .results = std.ArrayList(PingResult).empty,
};
defer client.results.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.results.items) |r| {
    std.debug.print("[{d}] rtt={d}ms dwell={d}ms\n",
        .{ r.seq_num, r.rtt_ms, r.server_dwell_ms });
}
```

## Error Propagation

`PingDecision.process` and `PingResponse.preprocess` return errors.
Runner detects at compile time and uses `try`.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Stateless server | Scales to arbitrary concurrent clients |
| Millisecond units | Network RTT is ms-scale; ns adds noise |
| Per-ping records | Simpler than windowed aggregation — caller groups as needed |
| max_results cap | Prevents unbounded memory growth in long sessions |
| Io interface | Portable clock and sleep via `Io.Timestamp` / `Io.sleep` |
| No panics | Errors propagate through Runner |
