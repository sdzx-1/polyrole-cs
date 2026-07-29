const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

pub fn MultiplexChannel(
    comptime protocol_count: u8,
    comptime max_message_size: usize,
    comptime channel_capacity: u8,
) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        stream: zio.net.Stream,
        writer: *std.Io.Writer,
        reader: *std.Io.Reader,

        reader_handle: zio.JoinHandle(anyerror!void),
        write_mu: zio.Mutex = .{},

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            send_buf: []u8,
            rb: zio.Channel([]const u8) = undefined,
            rb_buf: [channel_capacity][]const u8 = @splat(undefined),
            /// Freed on next recv — ensures slices from codec.decode remain valid.
            last_recv_data: ?[]const u8 = null,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var w = Io.Writer.fixed(self.send_buf);
                try codec.encode(&w, state_id, val);

                self.mux.write_mu.lockUncancelable();
                defer self.mux.write_mu.unlock();

                try self.mux.writer.writeByte(self.protocol_id);
                try self.mux.writer.writeInt(u16, @intCast(w.end), .big);
                try self.mux.writer.writeAll(self.send_buf[0..w.end]);
                try self.mux.writer.flush();
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                if (self.last_recv_data) |old| self.mux.allocator.free(old);
                const data = self.rb.receive() catch |err| {
                    self.last_recv_data = null;
                    return err;
                };
                self.last_recv_data = data;
                var r = Io.Reader.fixed(data);
                return codec.decode(&r, state_id, T);
            }
        };

        pub fn initFromChannel(
            self: *Self,
            allocator: std.mem.Allocator,
            channel: anytype,
        ) !void {
            self.allocator = allocator;
            self.stream = channel.stream;
            self.writer = &channel.stream_writer.interface;
            self.reader = &channel.stream_reader.interface;
            self.write_mu = .{};

            for (&self.sub_channels, 0..) |*sub, i| {
                sub.mux = self;
                sub.protocol_id = @intCast(i);
                sub.send_buf = try allocator.alloc(u8, max_message_size);
                errdefer allocator.free(sub.send_buf);
                sub.last_recv_data = null;
                sub.rb = zio.Channel([]const u8).init(&sub.rb_buf);
            }

            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
        }

        pub fn deinit(self: *Self) void {
            for (&self.sub_channels) |*sub| {
                sub.rb.close(.immediate);
                self.allocator.free(sub.send_buf);
                if (sub.last_recv_data) |old| self.allocator.free(old);
            }
            self.stream.socket.shutdown(.receive) catch {};
            self.reader_handle.join() catch {};
            self.stream.close();
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        fn readerLoop(self: *Self) anyerror!void {
            while (true) {
                const id = self.reader.takeByte() catch {
                    for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
                    return;
                };
                const len = self.reader.takeInt(u16, .big) catch {
                    for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
                    return;
                };
                const raw = self.reader.take(len) catch {
                    for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
                    return;
                };
                if (id >= protocol_count) continue;
                const copy = self.allocator.dupe(u8, raw) catch {
                    self.sub_channels[id].rb.close(.immediate);
                    continue;
                };
                self.sub_channels[id].rb.trySend(copy) catch |err| {
                    self.allocator.free(copy);
                    if (err == error.ChannelClosed) continue;
                    if (err == error.ChannelFull) {
                        self.sub_channels[id].rb.close(.immediate);
                        continue;
                    }
                    return err;
                };
            }
        }
    };
}
