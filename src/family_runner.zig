const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const MultiplexChannel = @import("family_mux_channel.zig").MultiplexChannel;

/// Drives multiple protocols concurrently over a single MultiplexChannel.
///
/// `states` is a tuple of protocol root states, e.g. `.{ ProtocolA.State, ProtocolB.State }`.
pub fn FamilyRunner(comptime states: anytype) type {
    return struct {
        const N = states.len;

        pub fn run(
            comptime role: polyrole.Role,
            contexts: anytype,
            mux: *MultiplexChannel(N),
            recv_timeout_ms: ?u64,
        ) !void {
            var handles: [N]zio.JoinHandle(anyerror!void) = undefined;

            inline for (states, 0..) |State, i| {
                const ch = mux.subChannel(@intCast(i));
                const Wrapper = struct {
                    fn run(
                        ctx_: @TypeOf(contexts[i]),
                        ch_: *MultiplexChannel(N).SubChannel,
                        timeout_: ?u64,
                    ) anyerror!void {
                        const R = polyrole.runner.Runner(State);
                        R.symmetric_run(role, ctx_, ch_, State, timeout_) catch |e| return e;
                    }
                };
                handles[i] = try zio.spawn(Wrapper.run, .{ contexts[i], ch, recv_timeout_ms });
            }

            for (&handles) |*h| {
                h.join() catch |err| {
                    for (0..N) |j| mux.subChannel(@intCast(j)).recv_ch.close(.immediate);
                    return err;
                };
            }
        }
    };
}

// ── tests ─────────────────────────────────────────────────────────────────

const TestProtocol = struct {
    fn make(comptime name: []const u8, comptime Next: type) type {
        return struct {
            const Info = polyrole.ProtocolInfo(name, i32, i32);

            pub const A = union(enum) {
                to_b: polyrole.Data(void, B),

                pub const info: Info = .{ .agent = .client, .name = name ++ ".A" };

                pub fn process(ctx: *i32) @This() {
                    _ = ctx;
                    return .to_b;
                }
            };

            pub const B = union(enum) {
                to_a: polyrole.Data(void, A),
                done: polyrole.Data(void, Next),

                pub const info: Info = .{ .agent = .server, .name = name ++ ".B" };

                pub fn process(ctx: *i32) @This() {
                    ctx.* += 1;
                    if (ctx.* >= 3) return .done;
                    return .to_a;
                }
            };
        };
    }
};

test "family: manual send/recv over SubChannel" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const Mux = MultiplexChannel(1);
    const R = polyrole.runner.Runner(P1.A);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    // Spawn server in a fiber
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            const stream = try addr.connect(.{});
            var mux: Mux = undefined;
            try mux.init(allocator, stream, 256, 256);
            defer mux.deinit();
            var ctx: i32 = 0;
            try R.symmetric_run(.client, &ctx, mux.subChannel(0), P1.A, null);
            try testing.expectEqual(@as(i32, 0), ctx);
        }
    }.run, .{listener.socket.address});

    const stream = try listener.accept(.{});
    var mux: Mux = undefined;
    try mux.init(allocator, stream, 256, 256);
    defer mux.deinit();
    var ctx: i32 = 0;
    try R.symmetric_run(.server, &ctx, mux.subChannel(0), P1.A, null);
    try testing.expectEqual(@as(i32, 3), ctx);
}
