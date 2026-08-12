const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");

const Info = polyrole.ProtocolInfo("counter", i32, i32);

const Counter = struct {
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

    pub const A = union(enum) {
        add: polyrole.Data(void, B),
        pub const info: Info = .{ .agent = .client, .name = "A" };
        pub fn process(ctx: *i32) @This() {
            _ = ctx;
            return .add;
        }
    };
};

test "simulate" {
    const R = polyrole.runner.Runner(Counter.A);
    var client: i32 = 0;
    var server: i32 = 0;
    try R.simulate(&client, &server, Counter.A);
    try std.testing.expectEqual(@as(i32, 10), server);
}

test "symmetric_run over InMemoryChannel" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const R = polyrole.runner.Runner(Counter.A);
    const InMemoryChannel = polyrole.channel.InMemoryChannel;
    const HalfChannel = polyrole.channel.HalfChannel;

    var h1: HalfChannel = undefined;
    var h2: HalfChannel = undefined;
    try h1.init(allocator, 64);
    try h2.init(allocator, 64);
    defer h1.deinit(allocator);
    defer h2.deinit(allocator);

    var ch_c: InMemoryChannel = .{ .half_self = &h1, .half_peer = &h2 };
    var ch_s: InMemoryChannel = .{ .half_self = &h2, .half_peer = &h1 };

    const ClientRunner = struct {
        fn run(ch: *InMemoryChannel, ctx: *i32) !void {
            try R.symmetric_run(.client, ctx, ch, Counter.A, null);
        }
    };

    var client: i32 = 0;
    var server: i32 = 0;

    var t = try zio.spawn(ClientRunner.run, .{ &ch_c, &client });
    try R.symmetric_run(.server, &server, &ch_s, Counter.A, null);
    try t.join();
    try std.testing.expectEqual(@as(i32, 10), server);
}
