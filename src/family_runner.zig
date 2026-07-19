const std = @import("std");
const zio = @import("zio");
const polyrole = @import("root.zig");
const Mux = @import("family_mux_channel.zig").MultiplexChannel;

/// Manages concurrent protocols over a MultiplexChannel.
///
/// `states` is a tuple of protocol root states, e.g. `.{ProtoA.State, ProtoB.State}`.
pub fn FamilyRunner(comptime states: anytype) type {
    return struct {
        const N = states.len;
        const MuxType = Mux(N);

        /// Initialize the server side. Spawns a background reader fiber that
        /// automatically creates server sessions when the first frame for a
        /// protocol arrives.
        pub fn initServer(
            mux: *MuxType,
            contexts: anytype,
            recv_timeout_ms: ?u64,
        ) !void {
            const S = struct {
                fn dispatch(
                    mux_: *MuxType,
                    ctxs: @TypeOf(contexts),
                    timeout_: ?u64,
                ) anyerror!void {
                    while (true) {
                        const frame = mux_.readFrame() catch |err| {
                            for (0..N) |j| mux_.subChannel(@intCast(j)).closed = true;
                            return err;
                        };
                        if (frame.id >= N) {
                            mux_.allocator.free(frame.data);
                            continue;
                        }
                        const ch = mux_.subChannel(frame.id);

                        if (ch.buf.items.len > 0 or ch.closed) {
                            ch.push(frame.data) catch {
                                mux_.allocator.free(frame.data);
                            };
                        } else {
                            switch (frame.id) {
                                inline 0...N - 1 => |i| {
                                    spawnServer(i, mux_, ctxs, timeout_, frame.data) catch {
                                        mux_.allocator.free(frame.data);
                                    };
                                },
                                else => mux_.allocator.free(frame.data),
                            }
                        }
                    }
                }
            };
            _ = try zio.spawn(S.dispatch, .{ mux, contexts, recv_timeout_ms });
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

        fn spawnServer(
            comptime id: u8,
            mux: *MuxType,
            contexts: anytype,
            recv_timeout_ms: ?u64,
            first_data: []const u8,
        ) !void {
            const State = states[id];
            const Wrapper = struct {
                fn run(
                    ctx_: @TypeOf(contexts[id]),
                    ch_: *MuxType.SubChannel,
                    timeout_: ?u64,
                    first_: []const u8,
                ) anyerror!void {
                    defer ch_.mux.allocator.free(first_);
                    try ch_.push(first_);
                    const R = polyrole.runner.Runner(State);
                    R.symmetric_run(.server, ctx_, ch_, State, timeout_) catch |e| return e;
                }
            };
            _ = try zio.spawn(Wrapper.run, .{ contexts[id], mux.subChannel(id), recv_timeout_ms, first_data });
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

test "family: init + deinit" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const M = Mux(1);
    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    _ = try zio.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            const stream = try addr.connect(.{});
            var mux: M = undefined;
            try mux.init(allocator, stream, 256, 256);
            defer mux.deinit();
        }
    }.run, .{listener.socket.address});
    const stream = try listener.accept(.{});
    var mux: M = undefined;
    try mux.init(allocator, stream, 256, 256);
    defer mux.deinit();
}

test "family: send + readFrame" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const M = Mux(1);
    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            const stream = try addr.connect(.{});
            var mux: M = undefined;
            try mux.init(allocator, stream, 256, 256);
            defer mux.deinit();
            const sw = &mux.stream_writer.interface;
            try sw.writeByte(0);
            try sw.writeInt(u16, 1, .big);
            try sw.writeAll(&.{42});
            try sw.flush();
        }
    }.run, .{listener.socket.address});
    const stream = try listener.accept(.{});
    var mux: M = undefined;
    try mux.init(allocator, stream, 256, 256);
    defer mux.deinit();
    const frame = try mux.readFrame();
    defer mux.allocator.free(frame.data);
    try std.testing.expectEqual(@as(u8, 0), frame.id);
}

test "family: manual send via SubChannel" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const M = Mux(1);
    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            const stream = try addr.connect(.{});
            var mux: M = undefined;
            try mux.init(allocator, stream, 256, 256);
            defer mux.deinit();
            var ctx: i32 = 0;
            const state = P1.A.process(&ctx);
            const R = polyrole.runner.Runner(P1.A);
            try mux.subChannel(0).send(R.idFromState(P1.A), P1.A, state);
        }
    }.run, .{listener.socket.address});
    const stream = try listener.accept(.{});
    var mux: M = undefined;
    try mux.init(allocator, stream, 256, 256);
    defer mux.deinit();
    const frame = try mux.readFrame();
    defer mux.allocator.free(frame.data);
    try std.testing.expectEqual(@as(u8, 0), frame.id);
}

test "family: full handshake" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();
    const P1 = TestProtocol.make("p1", polyrole.Exit);
    const R = polyrole.runner.Runner(P1.A);
    const M = Mux(1);
    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            const stream = try addr.connect(.{});
            var mux: M = undefined;
            try mux.init(allocator, stream, 256, 256);
            defer mux.deinit();
            var ctx: i32 = 0;
            try R.symmetric_run(.client, &ctx, mux.subChannel(0), P1.A, null);
        }
    }.run, .{listener.socket.address});
    const stream = try listener.accept(.{});
    var mux: M = undefined;
    try mux.init(allocator, stream, 256, 256);
    defer mux.deinit();
    const ch = mux.subChannel(0);
    var server_ctx: i32 = 0;
    const frame = try mux.readFrame();
    try std.testing.expectEqual(@as(u8, 0), frame.id);
    try ch.push(frame.data);
    try R.symmetric_run(.server, &server_ctx, ch, P1.A, null);
    try std.testing.expectEqual(@as(i32, 3), server_ctx);
}
