const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

pub const max_message_size = 1024;

pub fn MultiplexChannel(comptime protocol_count: u8) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        stream: zio.net.Stream,
        rbuff: []u8,
        wbuff: []u8,
        stream_reader: zio.net.Stream.Reader,
        stream_writer: zio.net.Stream.Writer,

        write_lock: zio.Mutex,
        read_lock: zio.Mutex,

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            /// Messages routed here by other SubChannels' recv() calls.
            buf: std.ArrayList([]const u8),
            closed: bool = false,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var buf: [max_message_size]u8 = undefined;
                var writer = Io.Writer.fixed(&buf);
                try codec.encode(&writer, state_id, val);
                const data = buf[0..writer.end];

                self.mux.write_lock.lockUncancelable();
                defer self.mux.write_lock.unlock();
                const sw = &self.mux.stream_writer.interface;
                try sw.writeByte(self.protocol_id);
                try sw.writeInt(u16, @intCast(data.len), .big);
                try sw.writeAll(data);
                try sw.flush();
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                while (true) {
                    // Check own buffer first
                    if (self.buf.items.len > 0) {
                        const data = self.buf.orderedRemove(0);
                        defer self.mux.allocator.free(data);
                        var reader = Io.Reader.fixed(data);
                        return try codec.decode(&reader, state_id, T);
                    }
                    // Read from TCP and route
                    const data = try self.readOne();
                    defer self.mux.allocator.free(data);
                    var reader = Io.Reader.fixed(data);
                    return try codec.decode(&reader, state_id, T);
                }
            }

            /// Read one frame from TCP. If it belongs to another protocol,
            /// route to that protocol's buffer and loop.
            fn readOne(self: *SubChannel) ![]const u8 {
                self.mux.read_lock.lockUncancelable();
                defer self.mux.read_lock.unlock();

                while (true) {
                    const sr = &self.mux.stream_reader.interface;
                    const id = try sr.takeByte();
                    const len = try sr.takeInt(u16, .big);
                    const data = try sr.take(len);

                    if (id >= protocol_count) continue;

                    const target = &self.mux.sub_channels[id];
                    if (target.closed) continue;

                    const copy = try self.mux.allocator.dupe(u8, data);
                    if (id == self.protocol_id) return copy;

                    target.buf.append(self.mux.allocator, copy) catch {
                        self.mux.allocator.free(copy);
                        continue;
                    };
                }
            }

            pub fn deinit(self: *SubChannel) void {
                for (self.buf.items) |item| self.mux.allocator.free(item);
                self.buf.deinit(self.mux.allocator);
            }
        };

        pub fn init(
            self: *Self,
            allocator: std.mem.Allocator,
            stream: zio.net.Stream,
            r_size: usize,
            w_size: usize,
        ) !void {
            self.allocator = allocator;
            self.stream = stream;

            const rbuff = try allocator.alloc(u8, r_size);
            errdefer allocator.free(rbuff);
            const wbuff = try allocator.alloc(u8, w_size);
            errdefer allocator.free(wbuff);
            self.rbuff = rbuff;
            self.wbuff = wbuff;

            self.stream_reader = stream.reader(rbuff);
            self.stream_writer = stream.writer(wbuff);
            self.write_lock = .{};
            self.read_lock = .{};

            for (&self.sub_channels, 0..) |*sc, i| {
                sc.* = .{
                    .mux = self,
                    .protocol_id = @intCast(i),
                    .buf = .empty,
                };
            }
        }

        pub fn deinit(self: *Self) void {
            for (&self.sub_channels) |*sc| sc.deinit();
            self.stream.close();
            self.allocator.free(self.rbuff);
            self.allocator.free(self.wbuff);
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }
    };
}
