const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const Mux = @import("family_mux_channel.zig").MultiplexChannel;

pub fn FamilyRunner(comptime states: anytype) type {
    return struct {
        const N = states.len;
        const MuxType = Mux(N);

        pub fn initServer(
            mux: *MuxType,
            contexts: anytype,
            recv_timeout_ms: ?u64,
        ) !void {
            inline for (states, 0..) |State, i| {
                const Wrapper = struct {
                    fn run(
                        ctx_: @TypeOf(contexts[i]),
                        ch_: *MuxType.SubChannel,
                        timeout_: ?u64,
                    ) anyerror!void {
                        const R = polyrole.runner.Runner(State);
                        R.symmetric_run(.server, ctx_, ch_, State, timeout_) catch |e| return e;
                    }
                };
                _ = try zio.spawn(Wrapper.run, .{ contexts[i], mux.subChannel(@intCast(i)), recv_timeout_ms });
            }
        }

        pub fn start(
            mux: *MuxType,
            comptime id: u8,
            ctx: anytype,
            recv_timeout_ms: ?u64,
        ) !void {
            const State = states[id];
            const R = polyrole.runner.Runner(State);
            try R.symmetric_run(.client, ctx, mux.subChannel(id), State, recv_timeout_ms);
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
                pub fn process(ctx: *i32) @This() { _ = ctx; return .to_b; }
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

test "family: full handshake with reader fiber" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const Fr = FamilyRunner(.{P1.A});
    const M = Mux(1);
    const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var l = try lh.listen(.{});
    defer l.close();

    var srv_ctx: i32 = 0;
    var cli_ctx: i32 = 0;

    var g: zio.Group = .init;
    defer g.cancel();
    try g.spawn(struct {
        fn run(a: zio.net.Address, ctx: *i32) !void {
            const s = try a.connect(.{});
            var m: M = undefined;
            try m.init(allocator, s, 256, 256);
            defer m.deinit();
            try Fr.start(&m, 0, ctx, null);
        }
    }.run, .{l.socket.address, &cli_ctx});

    const s = try l.accept(.{});
    var m: M = undefined;
    try m.init(allocator, s, 256, 256);
    defer m.deinit();
    try Fr.initServer(&m, .{&srv_ctx}, null);

    try zio.sleep(zio.Duration.fromMilliseconds(500));
    try std.testing.expectEqual(@as(i32, 3), srv_ctx);
}
