const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const zio = @import("zio");

pub const max_message_size = 1024;
const channel_capacity = 8;

pub fn MultiplexChannel(comptime protocol_count: u8) type {
    return struct {
        const Self = @This();

        /// Shared write channel item.
        pub const WriteMsg = struct {
            protocol_id: u8,
            data: []const u8,
        };

        allocator: std.mem.Allocator,
        stream: zio.net.Stream,
        rbuff: []u8,
        wbuff: []u8,
        stream_reader: zio.net.Stream.Reader,
        stream_writer: zio.net.Stream.Writer,

        reader_handle: zio.JoinHandle(anyerror!void),
        writer_handle: zio.JoinHandle(anyerror!void),

        /// Shared write channel — all SubChannel.send() calls push here.
        /// Writer fiber blocks on receive(), serializing all TCP writes.
        write_ch: zio.Channel(WriteMsg),
        write_ch_buf: [channel_capacity]WriteMsg = @splat(undefined),

        sub_channels: [protocol_count]SubChannel,

        pub const SubChannel = struct {
            mux: *Self,
            protocol_id: u8,
            rb: zio.Channel([]const u8) = undefined,
            rb_buf: [channel_capacity][]const u8 = @splat(undefined),

            pub fn send(self: *SubChannel, state_id: anytype, _: type, val: anytype) !void {
                var buf: [max_message_size]u8 = undefined;
                var writer = Io.Writer.fixed(&buf);
                try codec.encode(&writer, state_id, val);
                const copy = try self.mux.allocator.dupe(u8, buf[0..writer.end]);
                errdefer self.mux.allocator.free(copy);
                try self.mux.write_ch.send(.{ .protocol_id = self.protocol_id, .data = copy });
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                const data = self.rb.receive() catch |err| return err;
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

            self.write_ch = zio.Channel(WriteMsg).init(&self.write_ch_buf);

            for (&self.sub_channels, 0..) |*sc, i| {
                sc.* = .{ .mux = self, .protocol_id = @intCast(i) };
                sc.rb = zio.Channel([]const u8).init(&sc.rb_buf);
            }

            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
            self.writer_handle = try zio.spawn(Self.writerLoop, .{self});
        }

        pub fn deinit(self: *Self) void {
            self.write_ch.close(.immediate);
            self.writer_handle.join() catch {};
            for (&self.sub_channels) |*sc| sc.rb.close(.immediate);
            self.stream.socket.shutdown(.receive) catch {};
            self.reader_handle.join() catch {};
            self.stream.close();
            self.allocator.free(self.rbuff);
            self.allocator.free(self.wbuff);
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        fn writerLoop(self: *Self) anyerror!void {
            const sw = &self.stream_writer.interface;
            while (true) {
                const msg = self.write_ch.receive() catch break;
                defer self.allocator.free(msg.data);
                try sw.writeByte(msg.protocol_id);
                try sw.writeInt(u16, @intCast(msg.data.len), .big);
                try sw.writeAll(msg.data);
                try sw.flush();
            }
        }

        fn readerLoop(self: *Self) anyerror!void {
            const sr = &self.stream_reader.interface;
            while (true) {
                const id = sr.takeByte() catch {
                    for (&self.sub_channels) |*sc| sc.rb.close(.immediate);
                    return;
                };
                const len = sr.takeInt(u16, .big) catch {
                    for (&self.sub_channels) |*sc| sc.rb.close(.immediate);
                    return;
                };
                const raw = sr.take(len) catch {
                    for (&self.sub_channels) |*sc| sc.rb.close(.immediate);
                    return;
                };
                if (id >= protocol_count) continue;
                const copy = self.allocator.dupe(u8, raw) catch {
                    self.sub_channels[id].rb.close(.immediate);
                    continue;
                };
                self.sub_channels[id].rb.send(copy) catch |err| {
                    self.allocator.free(copy);
                    if (err == error.ChannelClosed) continue;
                };
            }
        }
    };
}
