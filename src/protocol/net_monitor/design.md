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
| `seq_num` | `u64` | client, echoed by server | Ordering, diagnostic tracing |
| `client_send_time` | `u64` | client, echoed by server | RTT = now - client_send_time (both on client clock) |
| `server_dwell_ns` | `u64` | server | Relative duration server spent processing (own clock, short interval) |

```
RTT_raw   = client_now - client_send_time
RTT_net   = RTT_raw - server_dwell_ns   // strips server-side delay
```

`server_dwell_ns` is a relative measurement — `response_time - arrival_time`
on the server's own monotonic clock. Over sub-millisecond intervals,
clock drift is negligible. No absolute timestamps cross machine boundaries.

`rtt_net` subtracts a server-clock duration from a client-clock duration.
This is technically imprecise (clocks drift at different rates), but the
error is bounded by `drift_ratio × dwell`. For typical dwell under 1ms
and clock drift under 100ppm, the error is under 100ns — well below
network jitter. The subtraction is therefore a useful approximation for
stripping server-side processing delay from RTT measurements.

## Payload Design

```zig
const PingPayload = struct {
    seq_num: u64,
    client_send_time: u64,
};

const PongPayload = struct {
    seq_num: u64,
    client_send_time: u64,
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
    start_ns: u64,         // first ping in this window (monotonic)
    rtt_sum_ns: u64 = 0,
    rtt_count: u32 = 0,
    rtt_min_ns: u64 = math.maxInt(u64),
    rtt_max_ns: u64 = 0,
};

pub const ClientContext = struct {
    /// Allocator for dynamic window list (set by caller before symmetric_run())
    allocator: std.mem.Allocator,

    /// Monotonic ping sequence number
    seq_num: u64,

    /// Remaining cycles before close (set by caller)
    remaining: u32,

    /// Interval between pings (nanoseconds), set by caller before symmetric_run()
    interval_ns: u64,

    /// Window duration (e.g. 60_000_000_000 = 1 minute)
    window_duration_ns: u64,

    /// Session start time (recorded on first PingQuery).
    /// 0 means not started. Monotonic clocks start near 0 at boot but
    /// establishing a connection takes long enough that this sentinel
    /// is safe in practice.
    session_start_ns: u64,

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
1. If `session_start_ns == 0`: record current monotonic `u64` time as `session_start_ns`.
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
   Window assignment is by **response arrival time** — a ping sent near a
   window boundary whose reply arrives in the next window is counted in
   the later window.
3. While `windows.items.len <= index`, append a new `WindowMetrics`
   with `start_ns = session_start_ns + windows.items.len * window_duration_ns`.
   This fills any gaps caused by index jumps (e.g. a very slow response
   spanning several window boundaries).
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
1. Decrement `remaining`.
2. If `remaining == 0`: return `.close`.
3. Sleep for `interval_ns` (blocking — Runner is synchronous).
4. Return `.ping_again`.

`remaining` counts **total** pings including the first one. The initial
PingQuery fires unconditionally (no check), then each PingDecision
decrements once. Setting `remaining = 5` produces exactly 5 pings.
Caller must set `remaining > 0`.

No `preprocess()` — server doesn't participate in this state.

## Channel

The protocol runs over any channel implementing the polyrole-cs send/recv
interface:

```
Runner(net_monitor).symmetric_run(role, ctx, channel, PingQuery)
```

Minimal setup with `StreamChannel` (raw TCP):

```
TCP stream
  └─ StreamChannel ─── Runner(net_monitor).symmetric_run
       PingQuery ⇄ PingResponse ⇄ PingDecision → Exit
```

Optionally layered over TLS:

```
TCP stream
  ├─ StreamChannel ─── Runner(simple_tls).symmetric_run (handshake)
  ├─ StreamChannel.deinit()
  └─ TlsChannel ─── Runner(net_monitor).symmetric_run
       PingQuery ⇄ PingResponse ⇄ PingDecision → Exit
```

The protocol makes no assumption about the underlying transport — encryption
is a deployment choice, not a protocol concern.

## Protocol Parameters (caller-controlled)

Before entering `Runner(net_monitor).symmetric_run()`, the caller sets:

| Parameter | Field | Meaning |
|-----------|-------|---------|
| Ping count | `ClientContext.remaining` | Total ping cycles (> 0) |
| Interval | `ClientContext.interval_ns` | Nanoseconds between pings (e.g. `1_000_000_000` = 1s) |
| Window duration | `ClientContext.window_duration_ns` | Per-window width (> 0, e.g. `60_000_000_000` = 1 min) |
| Windows list | `ClientContext.windows` | Dynamic `ArrayList(WindowMetrics)`, caller inits before `symmetric_run()`, reads after |
| Allocator | `ClientContext.allocator` | For `windows` list growth |
| Clock source | `ServerContext.clock` | Which monotonic clock to use |

After the Runner exits, the caller reads `ClientContext.windows.items`
for per-window aggregated results and deinits the ArrayList.

**Caller setup example:**

```zig
var ctx: ClientContext = .{
    .allocator = allocator,
    .remaining = 60,               // 60 total pings
    .interval_ns = 1_000_000_000,  // 1 second apart
    .window_duration_ns = 60_000_000_000, // 1 minute windows
    .windows = std.ArrayList(WindowMetrics).init(allocator),
    .session_start_ns = 0,
    .seq_num = 0,
};
defer ctx.windows.deinit();
try Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &channel, net_monitor.PingQuery);

// ctx.windows.items now contains per-minute stats
for (ctx.windows.items, 0..) |w, i| {
    if (w.rtt_count == 0) continue; // empty window (no responses in this slice)
    std.debug.print("window {d}: avg={d}ns min={d} max={d} count={d}\n", .{
        i, w.rtt_sum_ns / w.rtt_count, w.rtt_min_ns, w.rtt_max_ns, w.rtt_count,
    });
}
```

### Saving windows to file every N minutes

The protocol itself has no concept of persistence. Periodic saving is
achieved by splitting a long session into consecutive short runs over the
same channel — no reconnection needed as long as the connection stays
healthy:

```zig
while (total_remaining > 0) {
    const chunk = @min(total_remaining, pings_per_10min);
    ctx.remaining = chunk;
    ctx.windows.clearRetainingCapacity();
    ctx.session_start_ns = 0;
    ctx.seq_num = 0;

    Runner(net_monitor.PingQuery).symmetric_run(.client, &ctx, &channel, net_monitor.PingQuery) catch |err| {
        // connection broken — reconnect and retry, or propagate to caller
        return err;
    };

    try saveWindowFile("monitor_10min.json", ctx.windows.items);
    total_remaining -= chunk;
}
```

Key invariants:
- Channel stays open across Runner calls — re-entry is cheap, as long as
  the connection is alive.
- Each chunk resets `session_start_ns` and `windows`, so file content
  covers exactly one time slice.
- Caller controls the file naming and format; protocol layer stays focused
  on measurement.

## Error Handling

The protocol defines no error states. Network or channel failures propagate
naturally through the Runner's `try` mechanism — the caller catches them
and decides whether to reconnect, retry, or abort. The protocol itself
only models the correct-flow state machine.

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
| Transport-agnostic | Protocol only requires send/recv interface — works over StreamChannel, TlsChannel, or any custom transport. Encryption is a deployment choice |
| `remaining > 0` enforced at call site | Protocol assumes at least one ping; zero-ping sessions are meaningless for a monitor |
| Error handling is a caller concern | Protocol models only the correct-flow state machine. Faults propagate through Runner — the caller decides retry/reconnect/abort |
| Gap-filling window append | `while windows.items.len <= index`而非单次 `append` — 大 RTT 可能一次跳跃多个窗口，必须填充中间的所有空位 |
