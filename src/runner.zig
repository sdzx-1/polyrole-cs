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
    //max message size
    max_message_size: usize,
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
        errdefer gpa.free(send_buff);
        const recv_buff = try gpa.alloc(u8, buff_size);
        errdefer gpa.free(recv_buff);
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

    /// 协议任务退出时调用：关闭分发握手通道。
    /// reader 阻塞在 `recv_start.receive()` 时以 `ChannelClosed` 唤醒，
    /// 该协议的后续帧被丢弃，其余协议的分发与背压不受影响。
    pub fn closeRecv(self: *@This()) void {
        self.recv_start.close(.immediate);
    }

    /// 传输失败时由 supervisor 调用：关闭协议任务的两个阻塞点
    /// （等帧 `recv_end.receive` / 等发送许可 `send_start.receive`），
    /// 使其以 `ChannelClosed` 退出——不依赖 group.cancel，不会误伤同 group 的其他任务。
    pub fn poison(self: *@This()) void {
        self.send_start.close(.immediate);
        self.recv_end.close(.immediate);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        _ = try self.send_start.receive();
        var writer = Io.Writer.fixed(self.send_buff);
        try writer.writeByte(@intCast(self.id)); //协议id
        try writer.writeInt(u16, 0, .big); //payload_len，先占位置，后面再修改
        try codec.encode(&writer, state_id, val);
        self.len = writer.buffered().len;
        //payload_len = 总长度 - id(1) - payload_len(2)；wire 帧头是 u16，超过即不可表示
        const payload_len = self.len - 3;
        if (payload_len > std.math.maxInt(u16)) return error.MessageTooLarge;
        //payload_len 传输时使用大端序, 需要判断本机的端序
        std.mem.writeInt(u16, self.send_buff[1..3], @intCast(payload_len), .big);
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

/// Mux 内部事件：supervisor 通过单一通道接收协议完成与传输循环退出。
/// 容量 = 协议数 + 2（reader/writer 各至多一条），发送永不阻塞。
const Event = union(enum) {
    protocol_done: void,
    transport_exit: struct {
        err: ?anyerror,
    },
};

fn CreateContextTuple(comptime protocols: []const Protocol, comptime role: Role) type {
    const protocol_count = protocols.len;
    var types: [protocol_count]type = undefined;
    for (0..protocol_count) |i| {
        const CT = if (role == .client) protocols[i].client_ct else protocols[i].server_ct;
        types[i] = @Pointer(.one, .{}, CT, null);
    }
    return @Tuple(&types);
}

pub fn Mux(comptime protocols: []const Protocol, comptime role: Role, comptime encrypt: bool) type {
    const protocol_count = protocols.len;
    const StreamChannel = root.channel.StreamChannel;
    const TlsChannel = root.channel.TlsChannel;
    const Ctxs = CreateContextTuple(protocols, role);

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

        /// 事件通道：协议完成（×count）/ 传输循环退出（≤2），容量 = count + 2。
        events_buf: [protocol_count + 2]Event,
        events: zio.Channel(Event),

        /// writer 退出信号（排空最后一帧批后发出），supervisor 阶段 2 等待。
        writer_drained_buf: [1]void = undefined,
        writer_drained: zio.Channel(void),

        /// 传输级错误（supervisor 写入；调用方应在 group.wait() 后读取）。
        transport_err: ?anyerror = null,

        /// 每个协议的结果，索引 = protocol_id；err == null 表示正常到达 Exit。
        /// 每个协议任务只写自己的槽位（无竞争）；调用方应在 group.wait() 后读取
        /// （此时所有协议任务均已退出并写入）。
        results: [protocol_count]?anyerror = @splat(null),

        ctxs: Ctxs,

        pub fn init(self: *@This(), gpa: std.mem.Allocator, ctxs: Ctxs, sc: *StreamChannel, keys: ?MuxKeys) !void {
            self.send_end = .init(self.send_end_buf[0..]);
            self.sc = sc;

            // wire 帧头 protocol_id 是 u8：协议数最多 256（id 0..255）
            if (protocol_count > std.math.maxInt(u8) + 1) return error.TooManyProtocols;
            // wire 帧头 payload_len 是 u16：单帧载荷最多 65535（id + len 头不占 payload）
            inline for (protocols) |p| {
                if (p.max_message_size > std.math.maxInt(u16)) return error.MaxMessageSizeTooLarge;
            }

            var total: usize = 0;
            var sub_inited: usize = 0;
            for (0..protocol_count) |id| {
                const buff_size = protocols[id].max_message_size + 3;
                try self.sub_channels[id].init(gpa, id, buff_size, &self.send_end);
                sub_inited += 1;
                total += buff_size;
            }
            // 只清理已成功 init 的 sub_channel（未 init 的 send_buff/recv_buff 是 undefined，free 即 UB）
            errdefer for (0..sub_inited) |id| self.sub_channels[id].deinit(gpa);

            self.send_buf = try gpa.alloc(u8, total + 4);
            errdefer gpa.free(self.send_buf);
            if (comptime !encrypt) {
                self.recv_buf = try gpa.alloc(u8, total + 4);
                errdefer gpa.free(self.recv_buf);
            }

            if (comptime encrypt) {
                const k = keys orelse return error.MissingKeys;
                try self.tls.init(gpa, sc, k.write_key, k.read_key, total + 4);
            }

            self.send_id_collect = .initBuffer(self.send_id_collect_buff[0..]);
            self.send_id_collect.clearRetainingCapacity();

            self.events = .init(self.events_buf[0..]);
            self.writer_drained = .init(&self.writer_drained_buf);
            self.transport_err = null;
            self.results = @splat(null);

            self.ctxs = ctxs;
        }

        pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
            for (0..protocols.len) |id| {
                self.sub_channels[id].deinit(gpa);
            }
            gpa.free(self.send_buf);
            if (comptime !encrypt) gpa.free(self.recv_buf);
            if (comptime encrypt) self.tls.deinit(gpa);

            // 所有任务（协议/reader/writer/supervisor）都登记在调用方 group 中，
            // group.wait() 返回即全部退出——deinit 无需（也不应）cancel。
        }

        /// 启动全部任务进调用方 group：协议任务 ×count → reader → writer → supervisor。
        /// 顺序保证 supervisor 首次运行（可能触发收尾）时所有任务均已登记。
        /// group.wait() 返回后所有任务已退出，此时才可安全调用 deinit。
        pub fn run(self: *@This(), group: *zio.Group) !void {
            inline for (0..protocol_count) |id| {
                const curr = protocols[id];
                const S = struct {
                    const Ctx = if (role == .client) curr.client_ct else curr.server_ct;
                    pub fn protocolTask(ctx: *Ctx, channel: *SubChannel, events: *zio.Channel(Event), results: *[protocol_count]?anyerror) void {
                        var task_err: ?anyerror = null;
                        curr.runner.symmetric_run(role, ctx, channel, curr.enter, curr.recv_timeout_ms) catch |err| {
                            task_err = err;
                        };
                        // 先关闭接收侧：reader 不再为已死协议阻塞（其后续帧被丢弃）
                        channel.closeRecv();
                        results[id] = task_err;
                        events.send(.protocol_done) catch {};
                    }
                };
                try group.spawn(S.protocolTask, .{ self.ctxs[id], &self.sub_channels[id], &self.events, &self.results });
            }
            try group.spawn(reader_loop, .{ self, &self.events });
            try group.spawn(writer_loop, .{ self, &self.events, &self.writer_drained });
            try group.spawn(supervisor, .{ self, &self.events });
        }

        /// 读批 → 按帧分发。任何错误（EOF/坏帧/取消）都经 events 上报，
        /// 由 supervisor 决定级联取消；本任务始终以 void 退出，不污染 group 状态。
        fn reader_loop(self: *@This(), events: *zio.Channel(Event)) void {
            const err: ?anyerror = blk: {
                reader_loop_inner(self) catch |e| break :blk e;
                break :blk null;
            };
            events.send(.{ .transport_exit = .{ .err = err } }) catch {};
        }

        fn reader_loop_inner(self: *@This()) anyerror!void {
            while (true) {
                if (comptime encrypt) {
                    const plain = try self.tls.recordReadRaw();
                    // 批明文 = [total_len u32 BE][frames]，至少 4 字节
                    if (plain.len < 4) return error.BadLength;
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
        /// 协议已退出（recv_start 被 closeRecv 关闭）时跳过握手、丢弃该帧，
        /// 其余协议的分发与背压不受影响。
        fn dispatchFrames(self: *@This(), frames: []const u8) anyerror!void {
            var pos: usize = 0;
            while (pos < frames.len) {
                const protocol_id: usize = @intCast(frames[pos]);
                if (protocol_id >= protocol_count) return error.ProtocolIdTooLarge;
                pos += 1;
                // 帧头被截断（缺 payload_len 字段）→ 拒绝，不能越界读 frames[pos]/frames[pos+1]
                if (pos + 2 > frames.len) return error.BadLength;
                var tmp_u16: [2]u8 = undefined;
                tmp_u16[0] = frames[pos];
                tmp_u16[1] = frames[pos + 1];
                const payload_len: usize = @intCast(std.mem.readInt(u16, &tmp_u16, .big));
                const current = &self.sub_channels[protocol_id];
                if (payload_len > protocols[protocol_id].max_message_size or
                    payload_len > current.recv_buff.len) return error.MessageTooLarge;
                pos += 2;
                if (pos + payload_len > frames.len) return error.BadLength;
                // 分发握手：协议已退出（recv_start 已关闭）→ 丢弃该帧
                _ = current.recv_start.receive() catch |err| switch (err) {
                    error.ChannelClosed => {
                        pos += payload_len;
                        continue;
                    },
                    error.Canceled => return err,
                };
                current.len = payload_len;
                @memcpy(current.recv_buff[0..payload_len], frames[pos .. pos + payload_len]);
                try current.recv_end.send({});
                pos += payload_len;
            }
        }

        /// 聚合帧批并写出。优雅关闭（send_end 被 close(.graceful)）时先排空
        /// 已入队的帧再退出；任何写错误都经 events 上报，由 supervisor 决定级联。
        /// 退出契约（顺序固定）：flush 最后一帧批后，先发 writer_drained
        /// （"已停止写 socket"），再发 events 报告（err=null 表示干净排空）。
        fn writer_loop(self: *@This(), events: *zio.Channel(Event), writer_drained: *zio.Channel(void)) void {
            const err: ?anyerror = blk: {
                writer_loop_inner(self) catch |e| break :blk e;
                break :blk null;
            };
            writer_drained.send({}) catch {};
            events.send(.{ .transport_exit = .{ .err = err } }) catch {};
        }

        fn writer_loop_inner(self: *@This()) anyerror!void {
            while (true) {
                self.send_id_collect.clearRetainingCapacity();
                {
                    // graceful 关闭：receive 先返回缓冲值，排空后才给 ChannelClosed
                    const id = self.send_end.receive() catch |err| switch (err) {
                        error.ChannelClosed => return, // 无剩余帧，直接退出
                        else => return err,
                    };
                    self.send_id_collect.appendAssumeCapacity(id);
                }

                var closed: bool = false;
                while (self.send_end.tryReceive()) |id| {
                    self.send_id_collect.appendAssumeCapacity(id);
                } else |err| {
                    switch (err) {
                        error.ChannelEmpty => {},
                        error.ChannelClosed => closed = true, // 缓冲已排空，写完本批再退出
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
                if (closed) return;
            }
        }

        /// 督导任务：协调全部任务的退出，不依赖 group.cancel（不会误伤同 group 的
        /// 其他调用方任务），全部通过"通道关闭"驱动：
        ///
        /// 阶段 1：等待全部协议完成，或捕获首个传输失败。
        ///   - 传输失败 → 对每个 SubChannel 下毒丸（关 recv_end/send_start），
        ///     唤醒阻塞在内存通道上的协议任务 → 以 ChannelClosed 退出并上报。
        /// 阶段 2：全部协议完成 → 优雅关闭 send_end（writer 排空最后一帧批）→
        ///   等 writer_drained → shutdown 读写方向（不关 fd，调用方仍可 close）
        ///   → reader 以 EOF 退出 → 全组归零。
        ///
        /// 被 cancel（调用方发起）时，events.receive 抛 error.Canceled → 直接退出。
        fn supervisor(self: *@This(), events: *zio.Channel(Event)) void {
            var done: usize = 0;
            while (done < protocol_count) {
                const ev = events.receive() catch return;
                switch (ev) {
                    .protocol_done => done += 1,
                    .transport_exit => |t| {
                        if (t.err == null) continue; // writer 干净退出（仅阶段 2 可能），防御
                        if (self.transport_err == null) {
                            self.transport_err = t.err;
                            // 毒丸：唤醒阻塞中的协议任务，使其尽快上报并退出
                            for (0..protocol_count) |id| self.sub_channels[id].poison();
                        }
                    },
                }
            }
            // 阶段 2：全部协议完成，优雅关闭
            self.send_end.close(.graceful);
            // 等 writer 排空（或已因写错误退出——退出契约保证 writer_drained 必达）
            _ = self.writer_drained.receive() catch return;
            // writer 已停止写 socket：shutdown 读写方向唤醒 reader（fd 保持有效）
            self.sc.stream.shutdown(.both) catch {};
        }
    };
}


const testing = std.testing;

// 单协议测试用最小协议族（Mux 实例化需要）
const AuditProtocol = struct {
    const Info = ProtocolInfo("audit", i32, i32);
    pub const A = union(enum) {
        to_b: Data(void, B),
        pub const info: Info = .{ .agent = .client, .name = "A" };
        pub fn process(ctx: *i32) @This() { _ = ctx; return .to_b; }
    };
    pub const B = union(enum) {
        done: Data(void, Exit),
        pub const info: Info = .{ .agent = .server, .name = "B" };
        pub fn process(ctx: *i32) @This() { _ = ctx; return .done; }
    };
};

fn AuditMux() type {
    const protocol: Protocol = .{
        .enter = AuditProtocol.A,
        .runner = Runner(AuditProtocol.A),
        .client_ct = i32,
        .server_ct = i32,
        .max_message_size = 1024,
        .recv_timeout_ms = null,
    };
    return Mux(&.{protocol}, .server, false);
}

test "mux dispatchFrames: truncated frame header is rejected" {
    const M = AuditMux();
    var m: M = undefined;

    // 手动初始化一个 SubChannel（仅 dispatchFrames 需要，不启动 Mux）
    var send_end_buf: [1]usize = undefined;
    var send_end: zio.Channel(usize) = .init(&send_end_buf);
    try m.sub_channels[0].init(testing.allocator, 0, 1024 + 3, &send_end);
    defer m.sub_channels[0].deinit(testing.allocator);

    // 帧头被截断：只有 protocol_id，缺 2 字节 payload_len —— 必须返回错误而非越界读
    const truncated = [_]u8{0x00};
    try testing.expectError(error.BadLength, m.dispatchFrames(&truncated));

    // 只有 1 字节 payload_len 高位：同样截断
    const truncated2 = [_]u8{ 0x00, 0x01 };
    try testing.expectError(error.BadLength, m.dispatchFrames(&truncated2));

    // 正常帧头 + 超长 payload_len：BadLength（数据不足）
    const bad_len = [_]u8{ 0x00, 0x04, 0x00, 0x00 }; // 声明 1024 字节但只有 1 字节
    try testing.expectError(error.BadLength, m.dispatchFrames(&bad_len));

    // 协议 id 越界：ProtocolIdTooLarge
    const bad_id = [_]u8{ 0x01, 0x00, 0x00, 0x00 };
    try testing.expectError(error.ProtocolIdTooLarge, m.dispatchFrames(&bad_id));

    // 正常帧分发：模拟协议就绪（预置 recv_start token，即协议已 recv_start.send）
    try m.sub_channels[0].recv_start.send({});
    const good = [_]u8{ 0x00, 0x00, 0x01, 0xAB }; // protocol 0, payload_len=1, payload=0xAB
    try m.dispatchFrames(&good);
    try testing.expectEqual(@as(usize, 1), m.sub_channels[0].len);
    try testing.expectEqual(@as(u8, 0xAB), m.sub_channels[0].recv_buff[0]);
    _ = try m.sub_channels[0].recv_end.receive(); // 分发已放置就绪信号

    // 协议已退出（closeRecv）→ 帧被丢弃，dispatchFrames 正常返回
    m.sub_channels[0].closeRecv();
    try m.dispatchFrames(&good); // 不报错，帧被跳过
}

test "mux: init validates wire limits" {
    // max_message_size 超出 u16 帧头可表示范围 → 拒绝（校验先于任何分配，sc 不会被使用）
    const protocol: Protocol = .{
        .enter = AuditProtocol.A,
        .runner = Runner(AuditProtocol.A),
        .client_ct = i32,
        .server_ct = i32,
        .max_message_size = 70000,
        .recv_timeout_ms = null,
    };
    const M = Mux(&.{protocol}, .server, false);
    var m: M = undefined;
    var c: i32 = 0;
    try testing.expectError(error.MaxMessageSizeTooLarge, m.init(testing.allocator, .{&c}, undefined, null));
}

test "mux: init failure path releases all buffers" {
    // 明文单协议分配序列：sub.send_buff, sub.recv_buff, send_buf, recv_buf（共 4 次）。
    // 让第 k 次分配失败：init 必须返回错误，且 errdefer 只清理已 init 的 sub_channel
    // （无 double free / free(undefined) / 泄漏——testing.allocator 会检测）。
    const M = AuditMux();
    var ctx: i32 = 0;

    var fail_at: usize = 0;
    while (fail_at < 4) : (fail_at += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_at });
        var m: M = undefined;
        try testing.expectError(error.OutOfMemory, m.init(failing.allocator(), .{&ctx}, undefined, null));
    }

    // 第 5 次分配起全部成功：init 成功且 deinit 无泄漏
    {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 4 });
        var m: M = undefined;
        try m.init(failing.allocator(), .{&ctx}, undefined, null);
        m.deinit(failing.allocator());
    }
}
