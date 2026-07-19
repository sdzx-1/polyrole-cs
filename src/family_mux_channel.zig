const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

pub const max_message_size = 1024;
const channel_capacity = 8;

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
        reader_handle: zio.JoinHandle(anyerror!void),

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            recv_ch: zio.Channel([]const u8) = undefined,
            recv_buf: [channel_capacity][]const u8 = @splat(undefined),

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
                const data = self.recv_ch.receive() catch |err| {
                    return err;
                };
                defer self.mux.allocator.free(data);
                var reader = Io.Reader.fixed(data);
                return codec.decode(&reader, state_id, T);
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

            for (&self.sub_channels, 0..) |*sc, i| {
                sc.* = .{ .mux = self, .protocol_id = @intCast(i) };
                sc.recv_ch = zio.Channel([]const u8).init(&sc.recv_buf);
            }

            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
        }

        pub fn deinit(self: *Self) void {
            // Signal EOF to wake up the reader fiber
            self.stream.socket.shutdown(.receive) catch {};
            self.reader_handle.join() catch {};
            for (&self.sub_channels) |*sc| sc.recv_ch.close(.immediate);
            self.stream.close();
            self.allocator.free(self.rbuff);
            self.allocator.free(self.wbuff);
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        fn readerLoop(self: *Self) anyerror!void {
            const sr = &self.stream_reader.interface;
            while (true) {
                const id = sr.takeByte() catch {
                    for (&self.sub_channels) |*sc| sc.recv_ch.close(.immediate);
                    return;
                };
                const len = sr.takeInt(u16, .big) catch {
                    for (&self.sub_channels) |*sc| sc.recv_ch.close(.immediate);
                    return;
                };
                const raw = sr.take(len) catch {
                    for (&self.sub_channels) |*sc| sc.recv_ch.close(.immediate);
                    return;
                };
                if (id >= protocol_count) continue;
                const copy = self.allocator.dupe(u8, raw) catch {
                    self.sub_channels[id].recv_ch.close(.immediate);
                    continue;
                };
                self.sub_channels[id].recv_ch.send(copy) catch |err| {
                    self.allocator.free(copy);
                    if (err == error.ChannelClosed) continue;
                };
            }
        }
    };
}
