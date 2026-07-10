# Network Monitor Protocol Design

## Overview

A session-based network connectivity and latency monitoring protocol built
on polyrole-cs. The client sends periodic pings; the server echoes them back
verbatim. RTT is computed entirely on the client side using a single clock
— no cross-machine time synchronization required.

Runs over an encrypted `TlsChannel` after a `simple_tls` handshake.

## State Machine

```
PingQuery ─(c)──▶ PingResponse ─(s)──▶ PingDecision ─(c)──┬── ping_again → PingQuery (cycle)
                                                          │
                                                          └── close → Exit
```

Three states, two roles:

| State | Owner | Role |
|-------|-------|------|
| `PingQuery` | client | Timestamps and sends a ping |
| `PingResponse` | server | Echoes all fields + appends dwell time |
| `PingDecision` | client | Computes RTT, decides continue or exit |

## Clock-Independent RTT

Three fields per round-trip, none requiring cross-machine clock sync:

| Field | Type | Filled by | Purpose |
|-------|------|-----------|---------|
| `seq_num` | `u32` | client, echoed by server | Ordering, loss detection |
| `client_send_time` | `i64` | client, echoed by server | RTT = now - client_send_time (both on client clock) |
| `server_dwell_ns` | `u64` | server | Relative duration server spent processing (own clock, short interval) |

```
RTT_raw   = client_now - client_send_time
RTT_net   = RTT_raw - server_dwell_ns   // strips server-side delay
```

`server_dwell_ns` is a relative measurement — `response_time - arrival_time`
on the server's own monotonic clock. Over sub-millisecond intervals,
clock drift is negligible. No absolute timestamps cross machine boundaries.

## Payload Design

```zig
const PingPayload = struct {
    seq_num: u32,
    client_send_time: i64,
};

const PongPayload = struct {
    seq_num: u32,
    client_send_time: i64,
    server_dwell_ns: u64,
};
```

`PingDecision` carries `void` — the decision variants (`ping_again` / `close`)
don't need payload data. Metrics are accumulated in `ClientContext` during
`preprocess()`.

## Context Design

### ClientContext

```zig
pub const WindowMetrics = struct {
    start_ns: i64,         // first ping in this window (monotonic)
    rtt_sum_ns: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ns: u64 = math.maxInt(u64),
    rtt_max_ns: u64 = 0,
    lost_count: u32 = 0,
};

pub const ClientContext = struct {
    /// Allocator for dynamic window list (set by caller before symmetric_run())
    allocator: std.mem.Allocator,

    /// Monotonic ping sequence number
    seq_num: u32,

    /// Remaining cycles before close (set by caller)
    remaining: u32,

    /// Interval between pings (nanoseconds), set by caller before symmetric_run()
    interval_ns: u64,

    /// Window duration (e.g. 60_000_000_000 = 1 minute)
    window_duration_ns: u64,

    /// Session start time (recorded on first PingQuery)
    session_start_ns: i64,

    /// Dynamic list of per-window metrics, append-only
    windows: std.ArrayList(WindowMetrics),
};
```

### ServerContext

```zig
pub const ServerContext = struct {
    /// Monotonic clock for dwell measurement
    clock: std.time.Instant,
};
```

The server is **completely stateless** across pings — no session table,
no counter, no history. Each `PingResponse.process()` is a pure function:
read ping, compute dwell, echo back. This means an arbitrary number of
clients can share a single server without session management.

## Per-State Details

### PingQuery (client)

`process()`:
1. If `session_start_ns == 0`: record current monotonic time as `session_start_ns`.
2. Record current monotonic time as `client_send_time`.
3. Increment `seq_num`.
4. Return `.to_server` with `PingPayload`.

No `preprocess()` — server state, nothing for client to receive here.

### PingResponse (server)

`process()`:
1. Read arrival time `t_arrival` from `clock`.
2. Construct response (echo `seq_num`, `client_send_time`).
3. Record `t_departure`, compute `server_dwell_ns = t_departure - t_arrival`.
4. Return `.to_client` with `PongPayload`.

`preprocess()` (client):
1. Compute `rtt_net = now() - client_send_time - server_dwell_ns`.
2. Determine window index: `(now - session_start_ns) / window_duration_ns`.
3. If the window doesn't exist yet in `windows`, append a new `WindowMetrics`
   with `start_ns = session_start_ns + index * window_duration_ns`.
4. Accumulate `rtt_net` into that window's `rtt_sum_ns`, `rtt_count`,
   `rtt_min_ns`, `rtt_max_ns`.

### PingDecision (client)

```zig
pub const PingDecision = union(enum) {
    ping_again: Data(void, PingQuery),
    close: Data(void, Exit),
};
```

`process()`:
1. If `remaining == 0`: return `.close`.
2. Sleep for `interval_ns` (blocking — Runner is synchronous).
3. Decrement `remaining`.
4. Return `.ping_again`.

No `preprocess()` — server doesn't participate in this state.

## Composition with simple_tls

```
TCP stream
  ├─ StreamChannel ─── TLS handshake (Runner(simple_tls).symmetric_run)
  │   ClientHello → ServerHello → ClientFinished → Exit
  ├─ StreamChannel.deinit()
  └─ TlsChannel(write_key, read_key)
       └─ Runner(net_monitor).symmetric_run
            PingQuery ⇄ PingResponse ⇄ PingDecision → Exit
```

Same pattern as the existing TLS + app-protocol test in `runner.zig`.
TlsChannel is passed as the `channel` parameter — contexts are pure state.

## Protocol Parameters (caller-controlled)

Before entering `Runner(net_monitor).symmetric_run()`, the caller sets:

| Parameter | Field | Meaning |
|-----------|-------|---------|
| Ping count | `ClientContext.remaining` | How many ping cycles to run |
| Interval | `ClientContext.interval_ns` | Nanoseconds between pings (e.g. `1_000_000_000` = 1s) |
| Window duration | `ClientContext.window_duration_ns` | Per-window width (e.g. `60_000_000_000` = 1 min) |
| Windows list | `ClientContext.windows` | Dynamic `ArrayList(WindowMetrics)`, caller inits before `run()`, reads after |
| Allocator | `ClientContext.allocator` | For `windows` list growth |
| Clock source | `ServerContext.clock` | Which monotonic clock to use |

After the Runner exits, the caller reads `ClientContext.windows.items`
for per-window aggregated results and deinits the ArrayList.

**Caller setup example:**

```zig
var ctx: ClientContext = .{
    .allocator = allocator,
    .remaining = 60,               // 60 pings
    .interval_ns = 1_000_000_000,  // 1 second apart
    .window_duration_ns = 60_000_000_000, // 1 minute windows
    .windows = std.ArrayList(WindowMetrics).init(allocator),
    .session_start_ns = 0,
    .seq_num = 0,
};
defer ctx.windows.deinit();
try Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &tc, net_monitor.PingQuery);

// ctx.windows.items now contains per-minute stats
for (ctx.windows.items, 0..) |w, i| {
    std.debug.print("window {d}: avg={d}ns min={d} max={d} count={d} lost={d}\n", .{
        i, w.rtt_sum_ns / w.rtt_count, w.rtt_min_ns, w.rtt_max_ns, w.rtt_count, w.lost_count,
    });
}
```

### Saving windows to file every N minutes

The protocol itself has no concept of persistence. Periodic saving is
achieved by splitting a long session into consecutive short runs over the
same `TlsChannel` — no re-handshake needed:

```zig
// One chunk: run monitor for N minutes, save windows, repeat
while (total_remaining > 0) {
    const chunk = @min(total_remaining, pings_per_10min);
    ctx.remaining = chunk;
    ctx.windows.clearRetainingCapacity();
    ctx.session_start_ns = 0;
    ctx.seq_num = 0;

    try Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &tc, net_monitor.PingQuery);

    // Save this chunk to file
    try saveWindowFile("monitor_10min.json", ctx.windows.items);

    total_remaining -= chunk;
}
```

Key invariants:
- `TlsChannel` and TCP stream stay open across Runner calls — re-entry is
  cheap, no TLS handshake overhead.
- Each chunk resets `session_start_ns` and `windows`, so file content
  covers exactly one time slice.
- Caller controls the file naming and format; protocol layer stays focused
  on measurement.

## Error Handling

No protocol-level errors expected in normal operation. Network failures
(connection reset, TLS decrypt failure) propagate as errors from
`TlsChannel.send()` / `TlsChannel.recv()`, which the Runner propagates
to its caller via `try`.

The server never rejects a ping — no authentication/authorization at
this layer. If access control is needed, it's handled by the TLS
handshake (Ed25519 identity verification) before the monitor protocol
starts.

## Design Decisions Summary

| Decision | Rationale |
|----------|-----------|
| Stateless server | Scales to arbitrary concurrent clients, no session GC |
| Clock-independent timing | RTT uses client clock only; dwell uses server-local relative time |
| Cycle via explicit variants | Framework constraint — each possible next state is a separate union variant |
| No server-side metrics | Server doesn't care about RTT; let caller aggregate client-side |
| `remaining` counter on client | Simple, explicit termination condition; server is passive |
| Void payload on Decision | Decision carries no data — the transition itself is the signal |
| Protocol-managed interval | Ping frequency controlled by `interval_ns` in context; callers set one field, Runner handles timing |
| Synchronous sleep in process() | Runner is single-threaded — blocking `std.time.sleep` during interval is expected, not wasteful |
| Windowed aggregation | Per-minute `WindowMetrics` via `ArrayList` — caller inits, protocol appends, caller reads. No length limit |
| ArrayList over pre-allocated slice | Dynamic growth avoids forcing the caller to guess how many windows they'll need upfront |
