const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

/// What happens when an incoming frame arrives for a sub-channel whose
/// receive queue is full.
///
/// The framework's state machines are lockstep and strictly ordered: every
/// frame is one protocol step, and dropping a frame would leave the peer's
/// state machine stuck. The Mux therefore guarantees ordered, reliable
/// delivery per sub-channel (equivalent to "TCP inside the connection");
/// overflow can only be caused by a misbehaving peer or a protocol driver
/// that sends without receiving, never by legitimate lockstep traffic.
pub const OverflowPolicy = enum {
    /// Fail fast: close only this sub-channel and surface `error.ProtocolOverflow`
    /// to its `recv` calls. Other protocols are unaffected.
    close_channel,
    /// Block the reader until the queue drains, preserving order and delivery.
    /// A saturated protocol stalls the whole connection (cross-protocol HOL);
    /// choose this only when the connection must survive at any cost.
    backpressure,
};

/// Per-protocol member configuration of a protocol family.
pub const SubChannelConfig = struct {
    /// Size of this sub-channel's bounded receive queue (frames).
    capacity: u8 = 8,
    /// Maximum codec payload size for this protocol, in bytes.
    /// Must be <= 65532 so that `payload_len + 3` fits in the u16 frame header.
    max_message_size: usize = 1024,
    /// Overflow behavior for this protocol's receive queue.
    overflow: OverflowPolicy = .close_channel,
};

/// One decoded frame handed back by a transport.
pub const ReadFrame = struct {
    id: u8,
    /// Payload only — valid until the next `readFrame` call on the transport.
    payload: []const u8,
};

/// Byte-transport contract for the Mux. Implementations provide frame-level
/// read/write so the Mux is decoupled from the underlying channel type.
///
/// Two built-in transports exist:
///  - `initFromChannel`: plaintext over a `StreamChannel`.
///  - `TlsChannel.transport()`: every frame travels as one authenticated
///    TLS record, giving the whole family a single handshake and key set.
pub const Transport = struct {
    context: *anyopaque,
    /// Underlying byte stream; used only for teardown (`owns_stream`).
    stream: zio.net.Stream,
    /// Whether this Mux instance owns `stream` and must close it on deinit.
    owns_stream: bool = true,

    /// Write one complete frame `[id(1) || len(2 BE) || payload]`.
    writeFrame: *const fn (ctx: *anyopaque, id: u8, payload: []const u8) anyerror!void,
    /// Read one complete frame. The returned slice is transport-owned and
    /// valid only until the next `readFrame` call.
    readFrame: *const fn (ctx: *anyopaque) anyerror!ReadFrame,
    /// Shut down the receive side so a blocked `readFrame` unblocks during
    /// teardown.
    shutdownReceive: *const fn (ctx: *anyopaque) void,
};

/// Backwards-compatible shim: `Mux(n, max_size, capacity)` is equivalent to
/// N identical sub-channel configs.
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
        /// Stream-transport adapter state; only used when the transport was
        /// built by `initFromChannel` (context == self).
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
            /// Freed on next recv — ensures slices from codec.decode remain valid.
            last_recv_data: ?[]const u8 = null,
            /// Set when this sub-channel is closed due to overflow, so recv can
            /// distinguish `error.ProtocolOverflow` from a plain EOF close.
            closed_reason: ?anyerror = null,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var w = Io.Writer.fixed(self.send_buf);
                try codec.encode(&w, state_id, val);
                // Frame atomicity: serialize all writes on the shared transport
                // so a frame is never interleaved with another protocol's frame.
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

        /// Plaintext transport over a StreamChannel (or any channel exposing
        /// `stream`, `stream_writer.interface`, `stream_reader.interface`).
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
                            // close(.immediate) would drop buffered frames
                            // without freeing them — drain first.
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

        // ── Stream transport adapter ──────────────────────────────

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
                // Consume the oversized payload to keep frame sync, then fail.
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
