# Network Monitor Protocol Design

## Overview

A session-based network connectivity and latency monitoring protocol built
on polyrole-cs. The client sends periodic pings; the server echoes them back
verbatim. RTT is computed entirely on the client side using a single clock
— no cross-machine time synchronization required.

**Transport-agnostic.** The protocol needs only a channel that implements
`send(state_id, State, result)` and `recv(state_id, State) -> result`.
Works over `StreamChannel` (raw TCP), `TlsChannel` (encrypted), or any
custom transport implementing the same interface.

## State Machine

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (cycle)
                                                          │
                                                          └── close → Exit
```

## Clock-Independent RTT

Three fields per round-trip, none requiring cross-machine clock sync:

| Field | Type | Filled by | Purpose |
|-------|------|-----------|---------|
| `seq_num` | `u64` | client, echoed by server | Ordering, diagnostic tracing |
| `client_send_time` | `u64` | client, echoed by server | RTT = now - client_send_time (both on client clock) |
| `server_dwell_ns` | `u64` | server | Relative duration server spent processing (own clock, short interval) |

## Payload

```zig
pub const PingPayload = struct { seq_num: u64, client_send_time: u64, };
pub const PongPayload = struct { seq_num: u64, client_send_time: u64, server_dwell_ns: u64, };
```

## Context

```zig
pub const WindowMetrics = struct {
    start_ns: u64 = 0,
    rtt_sum_ns: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ns: u64 = std.math.maxInt(u64),
    rtt_max_ns: u64 = 0,
};

pub const ClientContext = struct {
    io: std.Io,                          // for clock and sleep
    allocator: std.mem.Allocator,        // for windows list growth
    seq_num: u64 = 0,
    remaining: u32 = 0,                  // total pings (> 0)
    interval_ns: u64 = 0,                // sleep between pings
    window_duration_ns: u64 = 0,         // per-window width
    session_start_ns: u64 = 0,           // 0 = not started
    windows: std.ArrayList(WindowMetrics),
};

pub const ServerContext = struct {
    io: std.Io,                          // for clock (dwell measurement)
    last_seq_num: u64 = 0,              // echoed from PingQuery
    last_client_send_time: u64 = 0,     // echoed from PingQuery
};
```

## States

### PingQuery (client → server)

```zig
pub const PingQuery = union(enum) {
    to_server: Data(PingPayload, PingResponse),
    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingQuery" };

    pub fn process(ctx: *ClientContext) @This() { ... }
    pub fn preprocess(ctx: *ServerContext, result: @This()) void { ... }
};
```

`process()` records `session_start_ns` (once), increments `seq_num`,
and sends the ping with a monotonic timestamp.

`preprocess()` stores the received `seq_num` and `client_send_time`
in `ServerContext` so `PingResponse` can echo them back.

### PingResponse (server → client)

```zig
pub const PingResponse = union(enum) {
    to_client: Data(PongPayload, PingDecision),
    pub const info: NetMonitorInfo = .{ .agent = .server, .name = "PingResponse" };

    pub fn process(ctx: *ServerContext) @This() { ... }
    pub fn preprocess(ctx: *ClientContext, result: @This()) !void { ... }
};
```

`process()` samples the monotonic clock before and after constructing
the response, computing `server_dwell_ns` as the difference.

`preprocess()` computes `rtt_net`, determines the window index by
response arrival time, grows the window list if needed, and
accumulates RTT metrics. Returns `!void` — window list growth can
fail on allocation error.

### PingDecision (client)

```zig
pub const PingDecision = union(enum) {
    ping_again: Data(void, PingQuery),
    close: Data(void, Exit),
    pub const info: NetMonitorInfo = .{ .agent = .client, .name = "PingDecision" };

    pub fn process(ctx: *ClientContext) !@This() { ... }
};
```

`process()` decrements `remaining`; if 0, returns `.close`.
Otherwise sleeps `interval_ns` and returns `.ping_again`.
Returns `!@This()` — sleep can fail on cancellation.

`remaining` counts **total** pings including the first one. Setting
`remaining = 5` produces exactly 5 pings. Must be > 0.

## Channel

```
Runner(net_monitor).symmetric_run(role, ctx, channel, PingQuery)
```

Works over `StreamChannel` (raw TCP) or `TlsChannel` (encrypted).
The protocol makes no assumption about the underlying transport.

## Caller Example

```zig
var client = ClientContext{
    .io = io,
    .allocator = allocator,
    .remaining = 60,
    .interval_ns = 1_000_000_000,
    .window_duration_ns = 60_000_000_000,
    .windows = std.ArrayList(WindowMetrics).empty,
};
defer client.windows.deinit(client.allocator);
var server = ServerContext{ .io = io };

try Runner(PingQuery).symmetric_run(.client, &client, &channel, PingQuery);

for (client.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue;
    std.debug.print("window {d}: avg={d} ns\n", .{ i, w.rtt_sum_ns / w.rtt_count });
}
```

## Error Propagation

`PingDecision.process` and `PingResponse.preprocess` return errors
(sleep cancellation, allocation failure). The Runner detects these
at compile time and uses `try`, propagating errors to the caller.

The protocol has no custom error set — it relies on the standard
library error types (`Allocator.Error`, `Cancelable!void`).

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Stateless server | Scales to arbitrary concurrent clients |
| Clock-independent timing | RTT uses client clock; dwell uses server-local relative time |
| Transport-agnostic | Only needs send/recv — StreamChannel, TlsChannel, etc. |
| Io interface | Uses `Io.Timestamp.now(io, .awake)` and `Io.sleep` — portable, same pattern as simple_tls |
| Error propagation | No panics — allocation and sleep failures return errors through Runner |
| Windowed aggregation | Per-window `WindowMetrics` via `ArrayList`, caller-managed lifetime |
| Gap-filling window append | `while items.len <= index` appends — fills gaps from slow responses spanning window boundaries |
