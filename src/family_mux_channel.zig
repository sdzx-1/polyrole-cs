const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

/// 当帧到达时其子通道接收队列已满，该如何处理。
///
/// 框架的状态机是锁步严格有序的：每一帧就是一次协议步骤，丢弃一帧
/// 会让对端的状态机卡住。因此 Mux 对每个子通道保证有序、可靠投递
/// （等价于"连接内的 TCP"）；溢出只可能由恶意对端或"只发不收"的
/// 协议驱动引起，绝不会来自合法的锁步流量。
pub const OverflowPolicy = enum {
    /// 快速失败：只关闭该子通道，并让它的 `recv` 返回可区分的
    /// `error.ProtocolOverflow`。其他协议不受影响。
    close_channel,
    /// 阻塞 Reader 直到队列腾空，保留顺序与投递。
    /// 饱和的协议会拖慢整个连接（跨协议 HOL）；
    /// 仅在推送类协议（窗口 > 1）上选用。
    backpressure,
};

/// 协议族中每个协议成员的配置。
pub const SubChannelConfig = struct {
    /// 接收侧固定槽位数（rb 深度）。
    /// 锁步协议取 1（在途消息恒为 1，精确匹配）；
    /// 推送类协议取发送方在途窗口 W，配合 `.backpressure` 使用。
    capacity: u8 = 1,
    /// 该协议 codec 载荷的最大字节数。
    /// 必须 <= 65532，以保证 `payload_len + 3` 能放进 u16 段头。
    max_message_size: usize = 1024,
    /// 该协议接收队列的溢出行为。
    overflow: OverflowPolicy = .close_channel,
};

/// 字节传输契约。实现方提供帧级读写，使 Mux 与底层通道类型解耦。
///
/// 内置两种传输：
///  - `initFromChannel`：在 `StreamChannel` 上的明文传输；
///  - `TlsChannel.transport()`：每一帧作为一条被认证的 TLS 记录传输，
///    让整个协议族共享一次握手和一套密钥。
pub const Transport = struct {
    context: *anyopaque,
    /// 底层字节流；仅用于销毁流程（`owns_stream`）。
    stream: zio.net.Stream,
    /// 该 Mux 实例是否拥有 `stream` 并需在 deinit 时关闭它。
    owns_stream: bool = true,

    /// 写入一条完整帧（不含长度前缀；传输层自行加帧定界）。
    writeFrame: *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void,
    /// 读取一条完整帧（不含长度前缀）。返回的切片归传输层所有，
    /// 仅在下次 `readFrame` 调用前有效。
    readFrame: *const fn (ctx: *anyopaque) anyerror![]const u8,
    /// 关闭接收侧，使阻塞中的 `readFrame` 在销毁时解除阻塞。
    shutdownReceive: *const fn (ctx: *anyopaque) void,
};

/// 向后兼容的 shim：`Mux(n, max_size, capacity)` 等价于
/// N 个完全相同的子通道配置，帧预算按"每帧至少一条最大消息"计算。
pub fn Mux(comptime n: u8, comptime max_size: usize, comptime cap: u8) type {
    return MultiplexChannel(
        &[_]SubChannelConfig{
            .{ .capacity = cap, .max_message_size = max_size },
        } ** n,
        max_size + 4,
    );
}

/// 协议族传输层：让 N 个锁步状态机共享一条连接。
///
/// 帧格式（帧体，传输层再加长度定界）：
/// ```
/// [seg_count u8]
/// [每段: protocol_id u8][payload_len u16 BE][payload ...]
/// ```
///
/// 架构：一个 Reader fiber 顺序读帧并按段投递到各协议 rb；
/// 一个 Writer fiber 从各协议 wb 收集消息、合并成一个帧写出。
/// wb/rb 是每协议固定大小的槽位，通过通道交接所有权。
pub fn MultiplexChannel(
    comptime configs: []const SubChannelConfig,
    comptime frame_budget: usize,
) type {
    const protocol_count = configs.len;
    comptime {
        if (protocol_count == 0)
            @compileError("MultiplexChannel requires at least one sub-channel");
        if (protocol_count > 255)
            @compileError("MultiplexChannel supports at most 255 protocols");
        var max_msg: usize = 0;
        for (configs) |cfg| {
            if (cfg.max_message_size > max_msg) max_msg = cfg.max_message_size;
            if (cfg.max_message_size > std.math.maxInt(u16) - 3)
                @compileError("SubChannelConfig.max_message_size must be <= 65532");
        }
        if (frame_budget > std.math.maxInt(u16))
            @compileError("frame_budget must be <= 65535");
        if (frame_budget < 1 + max_msg + 3)
            @compileError("frame_budget must fit at least one max message");
    }

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        stream: zio.net.Stream,
        /// 流传输适配器状态；仅在传输由 `initFromChannel`
        /// 构建时使用（context == self）。
        writer: *Io.Writer,
        reader: *Io.Reader,
        owns_stream: bool,
        transport: Transport,

        reader_handle: zio.JoinHandle(anyerror!void),
        writer_handle: zio.JoinHandle(anyerror!void),

        /// 协议 fiber → writer 的通知通道（携带协议 id）。
        /// 无缓冲（MVar 语义）：send 阻塞直到 writer 取走，
        /// 保证 wb 中待打包的消息不会被客户端覆盖或丢弃。
        wb_data: zio.Channel(u8) = undefined,
        /// writer 打包用的固定帧缓冲。
        frame_buf: []u8,

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            config: SubChannelConfig,

            // ── 发送侧（wb）：固定缓冲 + 单槽握手 ──
            wb_buf: []u8 = &.{},
            /// p 写入 wb 的消息长度（writer 读取）。
            wb_msg_len: usize = 0,
            /// 该 wb 是否有待打包的消息。
            wb_pending: bool = false,
            /// writer 归还 wb 所有权的信号（容量 1，初始含一个令牌）。
            wb_free: zio.Channel(u8) = undefined,
            wb_free_buf: [1]u8 = undefined,

            // ── 接收侧（rb）：capacity 个固定槽位 + free/rb 通道 ──
            slots: [][]u8 = &.{},
            slot_lens: []usize = &.{},
            /// 空闲槽位索引（reader 取走填充，p 归还）。
            free: zio.Channel(usize) = undefined,
            free_buf: []usize = &.{},
            /// 已填充槽位索引（reader 投递，p 消费）。
            rb: zio.Channel(usize) = undefined,
            rb_buf: []usize = &.{},
            last_idx: ?usize = null,
            /// 该子通道因溢出/非法帧而关闭时设置，使 recv 能区分
            /// `error.ProtocolOverflow` 与普通的 EOF 关闭。
            closed_reason: ?anyerror = null,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                // 等待 wb 空闲（writer 已取走上一条消息）
                _ = self.wb_free.receive() catch |err| return err;
                errdefer self.wb_free.trySend(0) catch {};

                var w = Io.Writer.fixed(self.wb_buf);
                try codec.encode(&w, state_id, val);
                self.wb_msg_len = w.end;

                // 通知 writer；阻塞直到 writer 收到 id，保证 wb 状态可见。
                try self.mux.wb_data.send(self.protocol_id);
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                // 归还上一个槽位（旧数据已消费完）
                if (self.last_idx) |old| self.free.trySend(old) catch {};
                const idx = self.rb.receive() catch |err| {
                    if (err == error.ChannelClosed) {
                        if (self.closed_reason) |reason| return reason;
                    }
                    return err;
                };
                self.last_idx = idx;
                const msg = self.slots[idx][0..self.slot_lens[idx]];
                var r = Io.Reader.fixed(msg);
                return codec.decode(&r, state_id, T, self.config.max_message_size);
            }
        };

        pub fn initFromTransport(
            self: *Self,
            allocator: std.mem.Allocator,
            transport: Transport,
        ) !void {
            self.allocator = allocator;
            self.stream = transport.stream;
            self.owns_stream = transport.owns_stream;
            self.transport = transport;
            self.wb_data = zio.Channel(u8).init(&[_]u8{});

            var initialized: usize = 0;
            errdefer for (self.sub_channels[0..initialized]) |*sub| {
                allocator.free(sub.wb_buf);
                for (sub.slots) |s| allocator.free(s);
                allocator.free(sub.slots);
                allocator.free(sub.slot_lens);
                allocator.free(sub.free_buf);
                allocator.free(sub.rb_buf);
            };
            errdefer allocator.free(self.frame_buf);

            for (&self.sub_channels, 0..) |*sub, i| {
                const cfg = configs[i];
                sub.* = .{ .mux = self, .protocol_id = @intCast(i), .config = cfg };

                sub.wb_buf = try allocator.alloc(u8, cfg.max_message_size);
                errdefer allocator.free(sub.wb_buf);
                sub.slots = try allocator.alloc([]u8, cfg.capacity);
                errdefer allocator.free(sub.slots);
                for (sub.slots) |*s| {
                    s.* = try allocator.alloc(u8, cfg.max_message_size);
                    errdefer allocator.free(s.*);
                }
                sub.slot_lens = try allocator.alloc(usize, cfg.capacity);
                errdefer allocator.free(sub.slot_lens);
                sub.free_buf = try allocator.alloc(usize, cfg.capacity);
                errdefer allocator.free(sub.free_buf);
                sub.rb_buf = try allocator.alloc(usize, cfg.capacity);
                errdefer allocator.free(sub.rb_buf);

                sub.wb_free = zio.Channel(u8).init(&sub.wb_free_buf);
                _ = sub.wb_free.trySend(0) catch unreachable;
                sub.free = zio.Channel(usize).init(sub.free_buf);
                for (0..cfg.capacity) |k| _ = sub.free.trySend(k) catch unreachable;
                sub.rb = zio.Channel(usize).init(sub.rb_buf);

                initialized += 1;
            }

            self.frame_buf = try allocator.alloc(u8, frame_budget);

            self.writer_handle = try zio.spawn(Self.writerLoop, .{self});
            errdefer self.writer_handle.cancel();
            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
            errdefer self.reader_handle.cancel();
        }

        /// 在 StreamChannel（或任何暴露 `stream`、
        /// `stream_writer.interface`、`stream_reader.interface` 的通道）上的明文传输。
        pub fn initFromChannel(
            self: *Self,
            allocator: std.mem.Allocator,
            channel: anytype,
        ) !void {
            self.writer = &channel.stream_writer.interface;
            self.reader = &channel.stream_reader.interface;
            try self.initFromTransport(allocator, .{
                .context = self,
                .stream = channel.stream,
                .owns_stream = true,
                .writeFrame = streamWriteFrame,
                .readFrame = streamReadFrame,
                .shutdownReceive = streamShutdownReceive,
            });
        }

        pub fn deinit(self: *Self) void {
            // 1. 关闭所有通道，解除 reader/writer 与协议 fiber 的阻塞。
            self.wb_data.close(.immediate);
            for (&self.sub_channels) |*sub| {
                sub.rb.close(.immediate);
                sub.free.close(.immediate);
                sub.wb_free.close(.immediate);
            }
            // 2. 关闭接收侧，解除 readFrame 的阻塞。
            self.transport.shutdownReceive(self.transport.context);
            // 3. 等待两个 fiber 退出。
            self.reader_handle.join() catch {};
            self.writer_handle.join() catch {};
            // 4. 释放固定缓冲。
            self.allocator.free(self.frame_buf);
            for (&self.sub_channels) |*sub| {
                self.allocator.free(sub.wb_buf);
                for (sub.slots) |s| self.allocator.free(s);
                self.allocator.free(sub.slots);
                self.allocator.free(sub.slot_lens);
                self.allocator.free(sub.free_buf);
                self.allocator.free(sub.rb_buf);
            }
            if (self.owns_stream) self.stream.close();
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        /// 关闭单个子通道（溢出或非法帧），排空 rb 并让 recv 返回 closed_reason。
        fn failSub(sub: *SubChannel, reason: anyerror) void {
            if (sub.closed_reason != null) return;
            sub.closed_reason = reason;
            while (sub.rb.tryReceive()) |_| {} else |_| {}
            sub.rb.close(.immediate);
        }

        fn closeAll(self: *Self) void {
            for (&self.sub_channels) |*sub| {
                // 优雅关闭：p 先消费完已投递的帧，再收到 ChannelClosed。
                sub.rb.close(.graceful);
            }
        }

        fn readerLoop(self: *Self) anyerror!void {
            while (true) {
                const frame = self.transport.readFrame(self.transport.context) catch {
                    self.closeAll();
                    return;
                };
                if (frame.len == 0) continue;
                const seg_count = frame[0];
                var pos: usize = 1;
                for (0..seg_count) |_| {
                    if (pos + 3 > frame.len) {
                        self.closeAll();
                        return;
                    }
                    const id = frame[pos];
                    const payload_len: u16 = (@as(u16, frame[pos + 1]) << 8) | frame[pos + 2];
                    pos += 3;
                    if (pos + payload_len > frame.len) {
                        self.closeAll();
                        return;
                    }
                    const payload = frame[pos .. pos + payload_len];
                    pos += payload_len;
                    if (id >= protocol_count) continue;
                    if (payload_len > self.sub_channels[id].config.max_message_size) {
                        // 超限段：非法帧，只关该协议（防御性，正常对端不会发出）。
                        failSub(&self.sub_channels[id], error.MessageTooLarge);
                        continue;
                    }
                    self.deliver(id, payload) catch |err| {
                        if (err == error.ChannelClosed) return; // deinit
                        return err;
                    };
                }
            }
        }

        fn deliver(self: *Self, id: u8, payload: []const u8) anyerror!void {
            const sub = &self.sub_channels[id];
            if (sub.closed_reason != null) return;
            const idx = switch (sub.config.overflow) {
                .close_channel => sub.free.tryReceive() catch |err| {
                    if (err == error.ChannelEmpty) {
                        // 队列已满：接收方处理不过来 = 协议族设计错误。
                        failSub(sub, error.ProtocolOverflow);
                        return;
                    }
                    return err;
                },
                .backpressure => sub.free.receive() catch |err| return err,
            };
            @memcpy(sub.slots[idx][0..payload.len], payload);
            sub.slot_lens[idx] = payload.len;
            sub.rb.trySend(idx) catch |err| return err;
        }

        fn writerLoop(self: *Self) anyerror!void {
            while (true) {
                // 1. 收集通知
                var any_pending = false;
                while (self.wb_data.tryReceive()) |id| {
                    if (id < protocol_count and !self.sub_channels[id].wb_pending) {
                        self.sub_channels[id].wb_pending = true;
                    }
                    any_pending = true;
                } else |_| {}
                for (&self.sub_channels) |*sub| {
                    if (sub.wb_pending) any_pending = true;
                }
                // 2. 无待发消息则阻塞等待通知
                if (!any_pending) {
                    const id = self.wb_data.receive() catch return; // ChannelClosed → 退出
                    if (id < protocol_count) self.sub_channels[id].wb_pending = true;
                }
                // 3. 打包：每个待发协议取一条完整消息，装不下的留待下一帧
                self.frame_buf[0] = 0; // 段数占位
                var pos: usize = 1;
                var seg_count: usize = 0;
                for (&self.sub_channels) |*sub| {
                    if (!sub.wb_pending) continue;
                    const seg_len = 3 + sub.wb_msg_len;
                    if (pos + seg_len > frame_budget) continue;
                    self.frame_buf[pos] = sub.protocol_id;
                    self.frame_buf[pos + 1] = @intCast(sub.wb_msg_len >> 8);
                    self.frame_buf[pos + 2] = @intCast(sub.wb_msg_len & 0xff);
                    @memcpy(self.frame_buf[pos + 3 ..][0..sub.wb_msg_len], sub.wb_buf[0..sub.wb_msg_len]);
                    pos += seg_len;
                    seg_count += 1;
                    sub.wb_pending = false;
                    _ = sub.wb_free.trySend(0) catch {}; // 归还 wb 所有权
                }
                if (seg_count > 0) {
                    self.frame_buf[0] = @intCast(seg_count);
                    self.transport.writeFrame(self.transport.context, self.frame_buf[0..pos]) catch {
                        // 写失败：连接已死，解除所有等待中的 send。
                        self.wb_data.close(.immediate);
                        for (&self.sub_channels) |*sub| sub.wb_free.close(.immediate);
                        return;
                    };
                }
            }
        }

        // ── 流传输适配器 ──────────────────────────────────────────

        fn streamWriteFrame(ctx: *anyopaque, frame: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            try self.writer.writeInt(u16, @intCast(frame.len), .big);
            try self.writer.writeAll(frame);
            try self.writer.flush();
        }

        fn streamReadFrame(ctx: *anyopaque) anyerror![]const u8 {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const len = try self.reader.takeInt(u16, .big);
            if (len > frame_budget) {
                // 消费超长载荷以保持帧同步，然后失败。
                self.reader.toss(len);
                return error.MessageTooLarge;
            }
            return try self.reader.take(len);
        }

        fn streamShutdownReceive(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.stream.socket.shutdown(.receive) catch {};
        }
    };
}
