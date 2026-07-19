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

        pub const Frame = struct {
            id: u8,
            data: []const u8,
        };

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            buf: std.ArrayList([]const u8) = .empty,
            closed: bool = false,

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var buf: [max_message_size]u8 = undefined;
                var writer = Io.Writer.fixed(&buf);
                try codec.encode(&writer, state_id, val);
                const data = buf[0..writer.end];

                const sw = &self.mux.stream_writer.interface;
                try sw.writeByte(self.protocol_id);
                try sw.writeInt(u16, @intCast(data.len), .big);
                try sw.writeAll(data);
                try sw.flush();
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                while (true) {
                    if (self.buf.items.len > 0) {
                        const data = self.buf.orderedRemove(0);
                        defer self.mux.allocator.free(data);
                        var reader = Io.Reader.fixed(data);
                        return codec.decode(&reader, state_id, T);
                    }
                    const frame = try self.mux.readFrame();
                    if (frame.id == self.protocol_id) {
                        defer self.mux.allocator.free(frame.data);
                        var reader = Io.Reader.fixed(frame.data);
                        return codec.decode(&reader, state_id, T);
                    }
                    const target = &self.mux.sub_channels[frame.id];
                    target.buf.append(self.mux.allocator, frame.data) catch {
                        self.mux.allocator.free(frame.data);
                    };
                }
            }

            pub fn push(self: *SubChannel, data: []const u8) !void {
                try self.buf.append(self.mux.allocator, data);
            }

            pub fn deinit(self: *SubChannel) void {
                for (self.buf.items) |item| self.mux.allocator.free(item);
                self.buf.deinit(self.mux.allocator);
            }
        };

        /// Read one complete frame from TCP. Caller owns the returned data.
        pub fn readFrame(self: *Self) !Frame {
            self.read_lock.lockUncancelable();
            defer self.read_lock.unlock();
            const sr = &self.stream_reader.interface;
            const id = sr.takeByte() catch |err| {
                for (&self.sub_channels) |*sc| sc.closed = true;
                return err;
            };
            const len = try sr.takeInt(u16, .big);
            const raw = try sr.take(len);
            const data = try self.allocator.dupe(u8, raw);
            return .{ .id = id, .data = data };
        }

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
