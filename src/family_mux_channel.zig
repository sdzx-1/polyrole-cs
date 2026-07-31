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
    /// 仅在"连接必须不计代价存活"时选用。
    backpressure,
};

/// 协议族中每个协议成员的配置。
pub const SubChannelConfig = struct {
    /// 该子通道有界接收队列的大小（帧数）。
    capacity: u8 = 8,
    /// 该协议 codec 载荷的最大字节数。
    /// 必须 <= 65532，以保证 `payload_len + 3` 能放进 u16 帧头。
    max_message_size: usize = 1024,
    /// 该协议接收队列的溢出行为。
    overflow: OverflowPolicy = .close_channel,
};

/// 传输层交回的一帧解码结果。
pub const ReadFrame = struct {
    id: u8,
    /// 仅载荷——在传输层下一次 `readFrame` 调用之前有效。
    payload: []const u8,
};

/// Mux 的字节传输契约。实现方提供帧级读写，
/// 使 Mux 与底层通道类型解耦。
///
/// 内置两种传输：
///  - `initFromChannel`：在 `StreamChannel` 上的明文传输。
///  - `TlsChannel.transport()`：每一帧作为一条被认证的 TLS 记录传输，
///    让整个协议族共享一次握手和一套密钥。
pub const Transport = struct {
    context: *anyopaque,
    /// 底层字节流；仅用于销毁流程（`owns_stream`）。
    stream: zio.net.Stream,
    /// 该 Mux 实例是否拥有 `stream` 并需在 deinit 时关闭它。
    owns_stream: bool = true,

    /// 写入一条完整帧 `[id(1) || len(2 BE) || payload]`。
    writeFrame: *const fn (ctx: *anyopaque, id: u8, payload: []const u8) anyerror!void,
    /// 读取一条完整帧。返回的切片归传输层所有，
    /// 仅在下次 `readFrame` 调用前有效。
    readFrame: *const fn (ctx: *anyopaque) anyerror!ReadFrame,
    /// 关闭接收侧，使阻塞中的 `readFrame` 在销毁时解除阻塞。
    shutdownReceive: *const fn (ctx: *anyopaque) void,
};

/// 向后兼容的 shim：`Mux(n, max_size, capacity)` 等价于
/// N 个完全相同的子通道配置。
pub fn Mux(comptime n: u8, comptime max_size: usize, comptime cap: u8) type {
    return MultiplexChannel(&[_]SubChannelConfig{
        .{ .capacity = cap, .max_message_size = max_size },
    } ** n);
}

pub fn MultiplexChannel(comptime configs: []const SubChannelConfig) type {
    const protocol_count = configs.len;
    comptime {
        for (configs) |cfg| {
            if (cfg.max_message_size > std.math.maxInt(u16) - 3)
                @compileError("SubChannelConfig.max_message_size must be <= 65532");
        }
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
        write_mu: zio.Mutex = .{},

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            config: SubChannelConfig,
            send_buf: []u8,
            rb: zio.Channel([]const u8) = undefined,
            rb_buf: []([]const u8) = &.{},
            /// 下次 recv 时释放——确保 codec.decode 返回的切片仍然有效。
            last_recv_data: ?[]const u8 = null,
            /// 该子通道因溢出而关闭时设置，使 recv 能区分
            /// `error.ProtocolOverflow` 与普通的 EOF 关闭。
            closed_reason: ?anyerror = null,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var w = Io.Writer.fixed(self.send_buf);
                try codec.encode(&w, state_id, val);
                // 帧原子性：在共享传输层上串行化所有写入，
                // 保证一帧永远不会与其他协议的帧交错。
                self.mux.write_mu.lockUncancelable();
                defer self.mux.write_mu.unlock();
                try self.mux.transport.writeFrame(
                    self.mux.transport.context,
                    self.protocol_id,
                    self.send_buf[0..w.end],
                );
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                if (self.last_recv_data) |old| self.mux.allocator.free(old);
                const data = self.rb.receive() catch |err| {
                    self.last_recv_data = null;
                    if (err == error.ChannelClosed) {
                        if (self.closed_reason) |reason| return reason;
                    }
                    return err;
                };
                self.last_recv_data = data;
                var r = Io.Reader.fixed(data);
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
            self.write_mu = .{};

            for (&self.sub_channels, 0..) |*sub, i| {
                const cfg = configs[i];
                sub.mux = self;
                sub.protocol_id = @intCast(i);
                sub.config = cfg;
                sub.last_recv_data = null;
                sub.closed_reason = null;

                sub.send_buf = try allocator.alloc(u8, cfg.max_message_size);
                errdefer allocator.free(sub.send_buf);
                sub.rb_buf = try allocator.alloc([]const u8, cfg.capacity);
                errdefer allocator.free(sub.rb_buf);
                sub.rb = zio.Channel([]const u8).init(sub.rb_buf);
            }

            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
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
            for (&self.sub_channels) |*sub| {
                while (sub.rb.tryReceive()) |queued| {
                    self.allocator.free(queued);
                } else |_| {}
                sub.rb.close(.immediate);
                self.allocator.free(sub.send_buf);
                if (sub.last_recv_data) |old| self.allocator.free(old);
                self.allocator.free(sub.rb_buf);
            }
            self.transport.shutdownReceive(self.transport.context);
            self.reader_handle.join() catch {};
            if (self.owns_stream) self.stream.close();
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        fn readerLoop(self: *Self) anyerror!void {
            while (true) {
                const frame = self.transport.readFrame(self.transport.context) catch {
                    for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
                    return;
                };
                if (frame.id >= protocol_count) continue;
                const sub = &self.sub_channels[frame.id];
                const copy = self.allocator.dupe(u8, frame.payload) catch {
                    sub.rb.close(.immediate);
                    continue;
                };
                switch (sub.config.overflow) {
                    .close_channel => sub.rb.trySend(copy) catch |err| {
                        self.allocator.free(copy);
                        if (err == error.ChannelClosed) continue;
                        if (err == error.ChannelFull) {
                            sub.closed_reason = error.ProtocolOverflow;
                            // close(.immediate) 会丢弃缓冲帧而不释放——
                            // 先排空队列。
                            while (sub.rb.tryReceive()) |queued| {
                                self.allocator.free(queued);
                            } else |_| {}
                            sub.rb.close(.immediate);
                            continue;
                        }
                        return err;
                    },
                    .backpressure => sub.rb.send(copy) catch |err| {
                        self.allocator.free(copy);
                        if (err == error.ChannelClosed) continue;
                        return err;
                    },
                }
            }
        }

        // ── 流传输适配器 ──────────────────────────────────────────

        fn streamWriteFrame(ctx: *anyopaque, id: u8, payload: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            try self.writer.writeByte(id);
            try self.writer.writeInt(u16, @intCast(payload.len), .big);
            try self.writer.writeAll(payload);
            try self.writer.flush();
        }

        fn streamReadFrame(ctx: *anyopaque) anyerror!ReadFrame {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const id = try self.reader.takeByte();
            const len = try self.reader.takeInt(u16, .big);
            if (len + 3 > max_frame_size) {
                // 消费超长载荷以保持帧同步，然后失败。
                self.reader.toss(len);
                return error.MessageTooLarge;
            }
            return .{ .id = id, .payload = try self.reader.take(len) };
        }

        fn streamShutdownReceive(ctx: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            self.stream.socket.shutdown(.receive) catch {};
        }

        const max_frame_size = blk: {
            var max_msg: usize = 0;
            for (configs) |cfg| {
                if (cfg.max_message_size > max_msg) max_msg = cfg.max_message_size;
            }
            break :blk max_msg + 3;
        };
    };
}
