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
        //payload_len 传输时使用大端序, 需要判断本机的端序
        std.mem.writeInt(u16, self.send_buff[1..3], payload_len, .big);
        try self.send_end.send(self.id);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        try self.recv_start.send({});
        try self.recv_end.receive();
        var reader = Io.Reader.fixed(self.recv_buff[0..self.len]);
        const res = try codec.decode(&reader, state_id, T);
        return res;
    }
};

pub const MuxKeys = struct {
    write_key: [32]u8,
    read_key: [32]u8,
};

pub const ErrorInfo = struct {
    /// protocols 数组下标
    protocol_id: usize,
    err: anyerror,
};

pub fn Mux(comptime protocols: []const Protocol, comptime encrypt: bool) type {
    const protocol_count = protocols.len;
    const StreamChannel = root.channel.StreamChannel;
    const TlsChannel = root.channel.TlsChannel;

    return struct {
        send_end_buf: [protocol_count]usize,
        send_end: zio.Channel(usize),

        sub_channels: [protocol_count]SubChannel,

        sc: *StreamChannel,
        tls: if (encrypt) TlsChannel else void = if (encrypt) undefined else {},

        send_id_collect_buff: [protocol_count]usize,
        send_id_collect: std.ArrayList(usize),

        send_buf: []u8,
        recv_buf: if (encrypt) void else []u8 = if (encrypt) {} else undefined,

        reader_handle: zio.JoinHandle(anyerror!void),
        writer_handle: zio.JoinHandle(anyerror!void),

        /// 每个协议至多上报一条错误（容量 = 协议数），调用方应在 wait 后消费。
        error_channel_buf: [protocol_count]ErrorInfo,
        error_channel: zio.Channel(ErrorInfo),

        pub fn init(self: *@This(), gpa: std.mem.Allocator, sc: *StreamChannel, keys: ?MuxKeys) !void {
            self.send_end = .init(self.send_end_buf[0..]);
            self.sc = sc;

            var total: usize = 0;
            for (0..protocol_count) |id| {
                const buff_size = protocols[id].max_massage_size + 3;
                try self.sub_channels[id].init(gpa, id, buff_size, &self.send_end);
                total += buff_size;
            }

            self.send_buf = try gpa.alloc(u8, total + 4);
            if (comptime !encrypt) self.recv_buf = try gpa.alloc(u8, total + 4);

            if (comptime encrypt) {
                const k = keys orelse return error.MissingKeys;
                try self.tls.init(gpa, sc, k.write_key, k.read_key, total + 4);
            }

            self.send_id_collect = .initBuffer(self.send_id_collect_buff[0..]);
            self.send_id_collect.clearRetainingCapacity();

            self.reader_handle = try zio.spawn(reader_loop, .{self});
            self.writer_handle = try zio.spawn(writer_loop, .{self});

            self.error_channel = .init(self.error_channel_buf[0..]);
        }

        pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
            for (0..protocols.len) |id| {
                self.sub_channels[id].deinit(gpa);
            }
            gpa.free(self.send_buf);
            if (comptime !encrypt) gpa.free(self.recv_buf);
            if (comptime encrypt) self.tls.deinit(gpa);

            self.reader_handle.cancel();
            self.writer_handle.cancel();
        }

        pub fn run(
            self: *@This(),
            comptime role: Role,
            group: *zio.Group,
            ctxs: anytype,
        ) !void {
            inline for (0..protocol_count) |id| {
                const curr = protocols[id];
                const S = struct {
                    const Ctx = if (role == .client) curr.client_ct else curr.server_ct;
                    pub fn protocolTask(ctx: *Ctx, err_chan: *zio.Channel(ErrorInfo), channel: *SubChannel) void {
                        curr.runner.symmetric_run(role, ctx, channel, curr.enter, curr.recv_timeout_ms) catch |err| {
                            err_chan.send(.{ .protocol_id = id, .err = err }) catch |e| {
                                std.debug.print("mux: error_channel full while reporting protocol error: {s}\n", .{@errorName(e)});
                                @panic("mux: error_channel full");
                            };
                        };
                    }
                };
                try group.spawn(S.protocolTask, .{ ctxs[id], &self.error_channel, &self.sub_channels[id] });
            }
        }

        //[total_len u32 BE][protocol_id u8][payload_len u16 BE][payload ...]
        pub fn reader_loop(self: *@This()) anyerror!void {
            while (true) {
                if (comptime encrypt) {
                    const plain = try self.tls.recordReadRaw();
                    const total_len = std.mem.readInt(u32, plain[0..4], .big);
                    if (plain.len != total_len + 4) return error.BadLength;
                    try self.dispatchFrames(plain[4..]);
                } else {
                    var total_len_buff: [4]u8 = undefined;
                    try self.sc.stream_reader.interface.readSliceAll(&total_len_buff);
                    const total_len = std.mem.readInt(u32, &total_len_buff, .big);
                    if (total_len > self.recv_buf.len) return error.TotalLenTooLarge;
                    try self.sc.stream_reader.interface.readSliceAll(self.recv_buf[0..total_len]);
                    try self.dispatchFrames(self.recv_buf[0..total_len]);
                }
            }
        }

        /// 按帧头切分一个批明文，逐帧分发给对应 SubChannel。
        fn dispatchFrames(self: *@This(), frames: []const u8) anyerror!void {
            var pos: usize = 0;
            while (pos < frames.len) {
                const protocol_id: usize = @intCast(frames[pos]);
                if (protocol_id >= protocol_count) return error.ProtocolIdTooLarge;
                pos += 1;
                var tmp_u16: [2]u8 = undefined;
                tmp_u16[0] = frames[pos];
                tmp_u16[1] = frames[pos + 1];
                const payload_len: usize = @intCast(std.mem.readInt(u16, &tmp_u16, .big));
                const current = &self.sub_channels[protocol_id];
                if (payload_len > protocols[protocol_id].max_massage_size or
                    payload_len > current.recv_buff.len) return error.MessageTooLarge;
                pos += 2;
                if (pos + payload_len > frames.len) return error.BadLength;
                try current.recv_start.receive();
                current.len = payload_len;
                @memcpy(current.recv_buff[0..payload_len], frames[pos .. pos + payload_len]);
                try current.recv_end.send({});
                pos += payload_len;
            }
        }

        pub fn writer_loop(self: *@This()) anyerror!void {
            while (true) {
                self.send_id_collect.clearRetainingCapacity();
                {
                    const id = try self.send_end.receive();
                    self.send_id_collect.appendAssumeCapacity(id);
                }

                while (self.send_end.tryReceive()) |id| {
                    self.send_id_collect.appendAssumeCapacity(id);
                } else |err| {
                    switch (err) {
                        error.ChannelEmpty => {},
                        error.ChannelClosed => return err,
                    }
                }
                var pos: usize = 4;
                for (self.send_id_collect.items) |id| {
                    const curr = &self.sub_channels[id];
                    const len = curr.len;
                    @memcpy(self.send_buf[pos .. pos + len], curr.send_buff[0..len]);
                    pos += len;
                    try curr.send_start.send({});
                }
                std.mem.writeInt(u32, self.send_buf[0..4], @intCast(pos - 4), .big);
                if (comptime encrypt) {
                    try self.tls.sealAndSend(self.send_buf[0..pos]);
                } else {
                    try self.sc.stream_writer.interface.writeAll(self.send_buf[0..pos]);
                    try self.sc.stream_writer.interface.flush();
                }
            }
        }
    };
}
