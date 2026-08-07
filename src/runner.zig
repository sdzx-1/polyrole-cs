const std = @import("std");
const zio = @import("zio");
const root = @import("root.zig");
const StateMap = root.StateMap;
const Role = root.Role;
const Data = root.Data;
const ProtocolInfo = root.ProtocolInfo;
const Exit = root.Exit;
const Io = std.Io;
const codec = @import("codec.zig");
const builtin = @import("builtin");

fn returnsError(comptime fun: anytype) bool {
    const ret = @typeInfo(@TypeOf(fun)).@"fn".return_type.?;
    return @typeInfo(ret) == .error_union;
}

/// 状态机运行器：驱动协议在两端之间执行。
///
/// 错误处理设计：
/// ----------------------
/// 最初 `process` 和 `preprocess` 不应返回错误。理由是典型通信协议中，
/// 任一方都不应在未通知对端的情况下单方面中止——通过 Exit 转移优雅关闭
/// 才是预期路径。
///
/// 但某些场景需要单方面终止。例如，面对非法或恶意客户端的服务端，
/// 在检测到无效凭据、被篡改的消息或重放攻击时必须立即中止协议。
/// 此时尝试与对端协作（发送优雅关闭）不可取甚至不可能。
///
/// 为同时支持两种模式，Runner 在编译期检查每个状态的 `process` 和
/// `preprocess` 返回类型：
/// - 若返回类型是普通 union（如 `@This()`），直接调用，协议照常推进。
/// - 若返回类型是错误联合（如 `!@This()`），Runner 用 `try` 并把错误
///   传播给自己的调用方，立即终止协议。
///
/// 这样协议作者可以按状态选择是否启用"出错即中止"语义，
/// 而无需让所有状态都采用同一模型。
pub fn Runner(
    comptime State_: type,
) type {
    return struct {
        const Info = @TypeOf(State_.info);
        pub const Client = Info.client_context;
        pub const Server = Info.server_context;
        pub const state_map: StateMap = .init(State_);
        pub const StateId = state_map.StateId;

        pub fn idFromState(comptime State: type) StateId {
            return state_map.idFromState(State);
        }

        pub fn StateFromId(comptime state_id: StateId) type {
            return state_map.StateFromId(state_id);
        }

        /// 在内存中运行协议两端，不经过通道。
        ///
        /// 在单线程中驱动客户端与服务端状态，模拟完整协议执行。
        /// 每个状态先调用归属方的 `process`，再让另一端的 `preprocess`
        /// 接收转移——不涉及序列化或网络 I/O。
        ///
        /// 适用于在部署到真实通道之前测试协议逻辑，
        /// 或两端共享同一地址空间的场景。
        pub fn simulate(client: *Client, server: *Server, start: type) !void {
            const start_id = idFromState(start);
            @setEvalBranchQuota(10_000_000);
            sw: switch (start_id) {
                inline else => |state_id| {
                    const State = StateFromId(state_id);
                    if (comptime State == Exit) return;
                    const info = State.info;
                    const process_ctx = if (comptime info.agent == .client) client else server;
                    const preprocess_ctx = if (comptime info.agent == .client) server else client;
                    const process = State.process;
                    const result = if (comptime returnsError(process)) try process(process_ctx) else process(process_ctx);
                    if (@hasDecl(State, "preprocess")) {
                        const preprocess = State.preprocess;
                        if (comptime returnsError(preprocess)) try preprocess(preprocess_ctx, result) else preprocess(preprocess_ctx, result);
                    }

                    switch (result) {
                        inline else => |new_state_wit| {
                            const NewState = @TypeOf(new_state_wit).State;
                            continue :sw comptime idFromState(NewState);
                        },
                    }
                },
            }
        }

        /// 通过通道运行协议的一端（对称拓扑）。
        ///
        /// 单个函数同时处理客户端和服务端角色：通过 `role` 传入 `.client`
        /// 或 `.server`。当前角色拥有的状态在本地处理并通过通道发送；
        /// 另一端拥有的状态通过 `preprocess` 接收和处理。
        ///
        /// "对称"指 1:1 通信拓扑——单个客户端与单个服务端通信。
        /// N:1 或其他非对称拓扑需要单独的 runner。
        ///
        /// `channel` 必须实现 `send(state_id, State, result)`
        /// 和 `recv(state_id, State) -> result`。
        ///
        /// 若设置 `recv_timeout_ms`，每次 `channel.recv()` 调用都由一个
        /// 全新的 zio AutoCancel 定时器守护。若 recv 阻塞超过超时时间，
        /// fiber 被取消，`error.Canceled` 向上传播。每次 recv 后
        /// （无论成功或失败）定时器都会被清除，因此下一轮从干净状态开始。
        pub fn symmetric_run(
            comptime role: Role,
            ctx: if (role == .client) *Client else *Server,
            channel: anytype,
            start: type,
            recv_timeout_ms: ?u64,
        ) !void {
            const start_id = idFromState(start);
            @setEvalBranchQuota(10_000_000);
            sw: switch (start_id) {
                inline else => |state_id| {
                    const State = StateFromId(state_id);
                    if (comptime State == Exit) return;
                    const info = State.info;
                    const result = blk: {
                        if (comptime role == info.agent) {
                            const process = State.process;
                            const res = if (comptime returnsError(process)) try process(ctx) else process(ctx);
                            try channel.send(state_id, State, res);
                            break :blk res;
                        } else {
                            const res = blkres: {
                                if (recv_timeout_ms) |ms| {
                                    var timeout: zio.AutoCancel = .init;
                                    defer timeout.clear();
                                    timeout.set(zio.Timeout.fromMilliseconds(ms));
                                    break :blkres try channel.recv(state_id, State);
                                } else {
                                    break :blkres try channel.recv(state_id, State);
                                }
                            };

                            if (@hasDecl(State, "preprocess")) {
                                const preprocess = State.preprocess;
                                if (comptime returnsError(preprocess)) try preprocess(ctx, res) else preprocess(ctx, res);
                            }
                            break :blk res;
                        }
                    };
                    switch (result) {
                        inline else => |new_state_wit| {
                            const NewState = @TypeOf(new_state_wit).State;
                            continue :sw comptime idFromState(NewState);
                        },
                    }
                },
            }
        }
    };
}

pub const Protocol = struct {
    //enter state
    enter: type,
    //runner
    runner: type,
    //client context typpe
    client_ct: type,
    //server context type
    server_ct: type,
    //max massage size
    max_massage_size: usize,
    recv_timeout_ms: ?u64,
};

pub const SubChannel = struct {
    id: usize,
    len: usize,
    send_buff: []u8,
    recv_buff: []u8,

    //send
    send_start_buf: [1]void = undefined,
    send_start: zio.Channel(void),

    send_end: *zio.Channel(usize),

    //recv
    recv_start_buf: [1]void = undefined,
    recv_start: zio.Channel(void),

    recv_end_buf: [1]void = undefined,
    recv_end: zio.Channel(void),

    pub fn init(
        self: *@This(),
        gpa: std.mem.Allocator,
        id: usize,
        buff_size: usize,
        send_end: *zio.Channel(usize),
    ) !void {
        self.id = id;
        const send_buff = try gpa.alloc(u8, buff_size);
        const recv_buff = try gpa.alloc(u8, buff_size);
        self.send_buff = send_buff;
        self.recv_buff = recv_buff;
        self.send_start = .init(&self.send_start_buf);
        self.recv_start = .init(&self.recv_start_buf);
        self.recv_end = .init(&self.recv_end_buf);
        self.send_end = send_end;
        self.len = 0;
        try self.send_start.send({});
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.send_buff);
        gpa.free(self.recv_buff);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        _ = try self.send_start.receive();
        var writer = Io.Writer.fixed(self.send_buff);
        try writer.writeByte(@intCast(self.id)); //协议id
        try writer.writeInt(u16, 0, .big); //payload_len，先占位置，后面再修改
        try codec.encode(&writer, state_id, val);
        self.len = writer.buffered().len;
        const payload_len: u16 = @intCast(self.len - 3); //payload_len = 总长度 - id(1) - payload_len(2)
        const bytes: [2]u8 = @bitCast(payload_len);
        //payload_len 传输时使用大端序, 需要判断本机的端序
        switch (builtin.cpu.arch.endian()) {
            .big => {
                self.send_buff[1] = bytes[0];
                self.send_buff[2] = bytes[1];
            },
            .little => {
                self.send_buff[1] = bytes[1];
                self.send_buff[2] = bytes[0];
            },
        }

        try self.send_end.send(self.id);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        try self.recv_start.send({});
        try self.recv_end.receive();
        var reader = Io.Reader.fixed(self.recv_buff[0..self.len]);
        //TODO: fix 4096
        const res = try codec.decode(&reader, state_id, T, 4096);
        return res;
    }
};

pub fn Mux(comptime protocols: []const Protocol) type {
    const protocol_count = protocols.len;

    return struct {
        send_end_buf: [protocol_count]usize,
        send_end: zio.Channel(usize),

        sub_channels: [protocol_count]SubChannel,

        writer: *Io.Writer,
        reader: *Io.Reader,

        send_buff_collect_buff: [protocol_count][]const u8,
        send_buff_collect: std.ArrayList([]const u8),

        send_id_collect_buff: [protocol_count]usize,
        send_id_collect: std.ArrayList(usize),

        reader_handle: zio.JoinHandle(anyerror!void),
        writer_handle: zio.JoinHandle(anyerror!void),

        pub fn init(self: *@This(), gpa: std.mem.Allocator, writer: *Io.Writer, reader: *Io.Reader) !void {
            self.send_end = .init(self.send_end_buf[0..]);

            for (0..protocol_count) |id| {
                try self.sub_channels[id].init(gpa, id, protocols[id].max_massage_size, &self.send_end);
            }

            self.writer = writer;
            self.reader = reader;
            self.send_buff_collect = .initBuffer(self.send_buff_collect_buff[0..]);
            self.send_id_collect = .initBuffer(self.send_id_collect_buff[0..]);

            self.send_buff_collect.clearRetainingCapacity();
            self.send_id_collect.clearRetainingCapacity();

            self.reader_handle = try zio.spawn(reader_loop, .{self});
            self.writer_handle = try zio.spawn(writer_loop, .{self});
        }

        pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
            for (0..protocols.len) |id| {
                self.sub_channels[id].deinit(gpa);
            }

            self.reader_handle.cancel();
            self.writer_handle.cancel();
        }

        pub fn run(
            self: *@This(),
            comptime role: Role,
            ctxs: anytype,
        ) !void {
            var group: zio.Group = .init;
            inline for (0..protocol_count) |id| {
                const curr = protocols[id];
                const S = struct {
                    const Ctx = if (role == .client) curr.client_ct else curr.server_ct;
                    pub fn foo(ctx: *Ctx, channel: *SubChannel) !void {
                        try curr.runner.symmetric_run(role, ctx, channel, curr.enter, curr.recv_timeout_ms);
                    }
                };
                try group.spawn(S.foo, .{ ctxs[id], &self.sub_channels[id] });
            }
            try group.wait();
        }

        //[protocol_id u8][payload_len u16 BE][payload ...]
        pub fn reader_loop(self: *@This()) anyerror!void {
            while (true) {
                //TODO: add more check
                const protocol_id: usize = @intCast(try self.reader.takeByte());
                const payload_len: usize = @intCast(try self.reader.takeInt(u16, .big));
                const current = &self.sub_channels[protocol_id];
                try current.recv_start.receive();
                current.len = payload_len;
                const payload = try self.reader.take(payload_len);
                @memcpy(current.recv_buff[0..payload_len], payload);
                try current.recv_end.send({});
            }
        }

        pub fn writer_loop(self: *@This()) anyerror!void {
            while (true) {
                self.send_buff_collect.clearRetainingCapacity();
                self.send_id_collect.clearRetainingCapacity();
                {
                    const id = try self.send_end.receive();
                    const curr = self.sub_channels[id];
                    self.send_buff_collect.appendAssumeCapacity(curr.send_buff[0..curr.len]);
                    self.send_id_collect.appendAssumeCapacity(id);
                }

                while (self.send_end.tryReceive()) |id| {
                    const curr = self.sub_channels[id];
                    self.send_buff_collect.appendAssumeCapacity(curr.send_buff[0..curr.len]);
                    self.send_id_collect.appendAssumeCapacity(id);
                } else |_| {}

                for (self.send_id_collect.items) |id| {
                    try self.sub_channels[id].send_start.send({});
                }

                try self.writer.writeVecAll(self.send_buff_collect.items);
                try self.writer.flush();
            }
        }
    };
}

fn CreateTestProtocol(name: []const u8, Next: type) type {
    return struct {
        const TestInfo = ProtocolInfo(name, i32, i32);

        pub const A = union(enum) {
            add: Data(void, B),

            pub const info: TestInfo = .{ .agent = .client, .name = "A" };

            pub fn process(ctx: *i32) @This() {
                _ = ctx;
                return .add;
            }
        };

        pub const B = union(enum) {
            to_a: Data(void, A),
            next: Data(void, C),

            pub const info: TestInfo = .{ .agent = .server, .name = "B" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* >= 1000) return .next;
                ctx.* += 1;
                return .to_a;
            }
        };

        pub const C = union(enum) {
            client_add: Data(void, @This()),
            next: Data(void, Next),

            pub const info: TestInfo = .{ .agent = .server, .name = "C" };

            pub fn process(ctx: *i32) @This() {
                if (ctx.* == 0) return .next;
                ctx.* -= 1;
                return .client_add;
            }

            pub fn preprocess(ctx: *i32, msg: @This()) !void {
                switch (msg) {
                    .client_add => ctx.* += 1,
                    .next => {},
                }
            }
        };
    };
}

test "simulate" {
    const testing = std.testing;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client: i32 = 0;
    var server: i32 = 0;
    try R.simulate(&client, &server, P.A);
    try testing.expectEqual(client, 1000);
}
test "symmetric run" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;
    const P = CreateTestProtocol("p2", Exit);
    const R = Runner(P.A);
    var client_context: i32 = 0;
    var server_context: i32 = 0;

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try localhost.listen(.{});
    defer server.close();

    const StreamChannel = root.channel.StreamChannel;

    const S = struct {
        fn clientFn(server_address: zio.net.Address, ctx: *i32) !void {
            var stream = try server_address.connect(.{});
            defer stream.close();

            var stream_channel: StreamChannel = undefined;
            try stream_channel.init(allocator, stream, 100, 100, 4096);
            defer stream_channel.deinit(allocator);

            try R.symmetric_run(.client, ctx, &stream_channel, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ server.socket.address, &client_context });

    var stream = try server.accept(.{});
    defer stream.close();

    var stream_channel: StreamChannel = undefined;
    try stream_channel.init(allocator, stream, 100, 100, 4096);
    defer stream_channel.deinit(allocator);

    try R.symmetric_run(.server, &server_context, &stream_channel, P.A, null);

    // 服务端跑完时客户端可能还在收 C 阶段的剩余消息,必须等客户端完成再断言。
    try group.wait();
    try testing.expectEqual(client_context, 1000);
}

test "symmetric run over in-memory channel" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P = CreateTestProtocol("inmem", Exit);
    const R = Runner(P.A);
    const InMemoryChannel = root.channel.InMemoryChannel;
    const HalfChannel = root.channel.HalfChannel;

    var client_context: i32 = 0;
    var server_context: i32 = 0;

    // InMemoryChannel 不经过网络 I/O：两个 HalfChannel 交叉配对成
    // 全双工管道，ch1（客户端）与 ch2（服务端）各持一端。
    var half1: HalfChannel = undefined;
    var half2: HalfChannel = undefined;
    try half1.init(allocator, 1024);
    try half2.init(allocator, 1024);
    defer half1.deinit(allocator);
    defer half2.deinit(allocator);

    const ch1: InMemoryChannel = .{ .max_slice_len = 1024, .half_self = &half1, .half_peer = &half2 };
    const ch2: InMemoryChannel = .{ .max_slice_len = 1024, .half_self = &half2, .half_peer = &half1 };

    const S = struct {
        fn clientFn(chan: *const InMemoryChannel, ctx: *i32) !void {
            try R.symmetric_run(.client, ctx, chan, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ &ch1, &client_context });

    try R.symmetric_run(.server, &server_context, &ch2, P.A, null);

    try group.wait();
    try testing.expectEqual(client_context, 1000);
}

test "symmetric_run: recv timeout" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P = CreateTestProtocol("timeout", Exit);
    const R = Runner(P.A);
    const StreamChannel = root.channel.StreamChannel;

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    // 客户端连接但什么都不发送——服务端 recv 应超时
    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(struct {
        fn run(addr: zio.net.Address) !void {
            var stream = try addr.connect(.{});
            defer stream.close();
            // 永远保持连接打开，不发送任何协议消息
            try zio.sleep(zio.Duration.fromSeconds(60));
        }
    }.run, .{listener.socket.address});

    var stream = try listener.accept(.{});
    defer stream.close();

    var ctx: i32 = 0;
    var ch: StreamChannel = undefined;
    try ch.init(allocator, stream, 128, 128, 4096);
    defer ch.deinit(allocator);

    // P.A 是客户端角色。服务端先 recv，客户端从不发送 → 超时
    // zio 的读取层会把 fiber 的 Canceled 转换为 ReadFailed
    try testing.expectError(error.ReadFailed, R.symmetric_run(.server, &ctx, &ch, P.A, 100));
    try testing.expectEqual(error.Canceled, ch.stream_reader.err.?);
}

test "tls channel: symmetric_run over encrypted channel" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;
    const crypto = std.crypto;
    const tls = @import("protocol/tls.zig");

    var kp_seed: [crypto.sign.Ed25519.KeyPair.seed_length]u8 = undefined;
    try zio.randomSecure(&kp_seed);
    const kp_c = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);
    try zio.randomSecure(&kp_seed);
    const kp_s = try crypto.sign.Ed25519.KeyPair.generateDeterministic(kp_seed);

    const StreamChannel = root.channel.StreamChannel;
    const TlsChannel = root.channel.TlsChannel;

    const P = CreateTestProtocol("tls_proto", Exit);
    const R_pp = Runner(P.A);
    const R_tls = Runner(tls.ClientHello);

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try localhost.listen(.{});
    defer listener.close();

    var client_counter: i32 = 0;
    var server_counter: i32 = 0;

    const ClientTask = struct {
        fn run(
            addr: zio.net.Address,
            kp: crypto.sign.Ed25519.KeyPair,
            peer_pk: crypto.sign.Ed25519.PublicKey,
            counter: *i32,
        ) !void {
            var stream = try addr.connect(.{});
            defer stream.close();

            // 阶段 1：TLS 握手
            var tls_ctx = tls.ClientContext.init(kp, peer_pk);

            var sc: StreamChannel = undefined;
            try sc.init(allocator, stream, 256, 256, 4096);
            defer sc.deinit(allocator);
            try R_tls.symmetric_run(.client, &tls_ctx, &sc, tls.ClientHello, null);

            // 阶段 2：加密协议——复用 sc
            var tc: TlsChannel = undefined;
            try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
            defer tc.deinit(allocator);

            // 密钥已复制到 TlsChannel——清零握手上下文
            tls_ctx.deinit();

            try R_pp.symmetric_run(.client, counter, &tc, P.A, null);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(ClientTask.run, .{
        listener.socket.address,
        kp_c,
        kp_s.public_key,
        &client_counter,
    });

    var stream = try listener.accept(.{});
    defer stream.close();

    // 阶段 1：TLS 握手
    var tls_ctx = tls.ServerContext.init(kp_s, kp_c.public_key);
    var sc: StreamChannel = undefined;
    try sc.init(allocator, stream, 256, 256, 4096);
    defer sc.deinit(allocator);
    try R_tls.symmetric_run(.server, &tls_ctx, &sc, tls.ClientHello, null);

    // 阶段 2：加密协议——复用 sc
    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc, tls_ctx.write_key, tls_ctx.read_key, 512);
    defer tc.deinit(allocator);

    // 密钥已复制到 TlsChannel——清零握手上下文
    tls_ctx.deinit();

    try R_pp.symmetric_run(.server, &server_counter, &tc, P.A, null);

    try group.wait();
    try testing.expectEqual(client_counter, 1000);
}

test "mux test" {
    const testing = std.testing;
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    const allocator = testing.allocator;

    const P1 = CreateTestProtocol("p1", Exit);
    const R1 = Runner(P1.A);

    const P2 = CreateTestProtocol("p2", Exit);
    const R2 = Runner(P2.A);

    var client_context: i32 = 0;
    var server_context: i32 = 0;

    var client_context1: i32 = 0;
    var server_context1: i32 = 0;

    const protocol1: Protocol = .{
        .enter = P1.A,
        .runner = R1,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const protocol2: Protocol = .{
        .enter = P2.A,
        .runner = R2,
        .client_ct = i32,
        .server_ct = i32,
        .max_massage_size = 1024,
        .recv_timeout_ms = null,
    };

    const TmpMux = Mux(&.{
        protocol1,
        protocol2,
    });

    const localhost = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try localhost.listen(.{});
    defer server.close();

    const S = struct {
        fn clientFn(
            server_address: zio.net.Address,
            gpa: std.mem.Allocator,
            ctxs: struct { *i32, *i32 },
        ) !void {
            var stream = try server_address.connect(.{});
            defer stream.close();

            const rbuff = try gpa.alloc(u8, 1024);
            defer gpa.free(rbuff);
            const wbuff = try gpa.alloc(u8, 1024);
            defer gpa.free(wbuff);

            var stream_reader = stream.reader(rbuff);
            var stream_writer = stream.writer(wbuff);

            var mux: TmpMux = undefined;
            try mux.init(gpa, &stream_writer.interface, &stream_reader.interface);
            defer mux.deinit(gpa);

            try mux.run(.client, ctxs);
        }
    };

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(S.clientFn, .{ server.socket.address, allocator, .{ &client_context, &client_context1 } });

    var stream = try server.accept(.{});
    defer stream.close();

    const rbuff = try allocator.alloc(u8, 1024);
    defer allocator.free(rbuff);
    const wbuff = try allocator.alloc(u8, 1024);
    defer allocator.free(wbuff);

    var stream_reader = stream.reader(rbuff);
    var stream_writer = stream.writer(wbuff);

    var mux: TmpMux = undefined;
    try mux.init(allocator, &stream_writer.interface, &stream_reader.interface);
    defer mux.deinit(allocator);

    try mux.run(.server, .{ &server_context, &server_context1 });

    // 服务端跑完时客户端可能还在收 C 阶段的剩余消息,必须等客户端完成再断言。
    try group.wait();
    try testing.expectEqual(client_context, 1000);
    try testing.expectEqual(client_context1, 1000);
}
