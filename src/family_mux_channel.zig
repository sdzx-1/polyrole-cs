const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const crypto = std.crypto;
const zio = @import("zio");

pub const max_message_size = 1024;
const channel_capacity = 8;

pub fn MultiplexChannel(comptime protocol_count: u8) type {
    return struct {
        const Self = @This();

        pub const WriteMsg = struct {
            protocol_id: u8,
            data: []const u8,
        };

        allocator: std.mem.Allocator,
        stream: zio.net.Stream,
        writer: *std.Io.Writer,
        reader: *std.Io.Reader,

        reader_handle: zio.JoinHandle(anyerror!void),
        writer_handle: zio.JoinHandle(anyerror!void),

        /// AEAD keys. Zero = plaintext.
        write_key: [32]u8 = [_]u8{0} ** 32,
        read_key: [32]u8 = [_]u8{0} ** 32,
        write_counter: u64 = 0,
        read_counter: u64 = 0,

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
                var w = Io.Writer.fixed(&buf);
                try codec.encode(&w, state_id, val);
                const copy = try self.mux.allocator.dupe(u8, buf[0..w.end]);
                errdefer self.mux.allocator.free(copy);
                try self.mux.write_ch.send(.{ .protocol_id = self.protocol_id, .data = copy });
            }

            pub fn recv(self: *SubChannel, state_id: anytype, T: type) !T {
                const data = self.rb.receive() catch |err| return err;
                defer self.mux.allocator.free(data);
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

            self.write_ch = zio.Channel(WriteMsg).init(&self.write_ch_buf);

            for (&self.sub_channels, 0..) |*sub, i| {
                sub.* = .{ .mux = self, .protocol_id = @intCast(i) };
                sub.rb = zio.Channel([]const u8).init(&sub.rb_buf);
            }

            self.write_key = [_]u8{0} ** 32;
            self.read_key = [_]u8{0} ** 32;

            self.reader_handle = try zio.spawn(Self.readerLoop, .{self});
            self.writer_handle = try zio.spawn(Self.writerLoop, .{self});
        }

        pub fn setKeys(self: *Self, write_key: [32]u8, read_key: [32]u8) void {
            self.write_key = write_key;
            self.read_key = read_key;
        }

        pub fn deinit(self: *Self) void {
            self.write_ch.close(.immediate);
            self.writer_handle.join() catch {};
            for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
            self.stream.socket.shutdown(.receive) catch {};
            self.reader_handle.join() catch {};
            self.stream.close();
        }

        pub fn subChannel(self: *Self, id: u8) *SubChannel {
            return &self.sub_channels[id];
        }

        fn writerLoop(self: *Self) anyerror!void {
            while (true) {
                const msg = self.write_ch.receive() catch break;
                defer self.allocator.free(msg.data);
                if (isZero(&self.write_key)) {
                    try self.writer.writeByte(msg.protocol_id);
                    try self.writer.writeInt(u16, @intCast(msg.data.len), .big);
                    try self.writer.writeAll(msg.data);
                } else {
                    try writeEncrypted(self, msg);
                }
                try self.writer.flush();
            }
        }

        fn readerLoop(self: *Self) anyerror!void {
            while (true) {
                if (isZero(&self.read_key)) {
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
                    self.sub_channels[id].rb.send(copy) catch |err| {
                        self.allocator.free(copy);
                        if (err == error.ChannelClosed) continue;
                    };
                } else {
                    const frame = readEncrypted(self) catch |err| {
                        if (err == error.EndOfStream) {
                            for (&self.sub_channels) |*sub| sub.rb.close(.immediate);
                            return;
                        }
                        return err;
                    };
                    if (frame.id >= protocol_count) {
                        self.allocator.free(frame.data);
                        continue;
                    }
                    self.sub_channels[frame.id].rb.send(frame.data) catch |err| {
                        self.allocator.free(frame.data);
                        if (err == error.ChannelClosed) continue;
                    };
                }
            }
        }
    };
}

fn isZero(key: *const [32]u8) bool {
    for (key) |b| if (b != 0) return false;
    return true;
}

fn writeEncrypted(self: anytype, msg: anytype) !void {
    const this_counter = self.write_counter;
    self.write_counter += 1;

    var nonce: [24]u8 = [_]u8{0} ** 24;
    std.mem.writeInt(u64, nonce[0..8], this_counter, .big);

    var frame: [max_message_size + 3]u8 = undefined;
    frame[0] = msg.protocol_id;
    std.mem.writeInt(u16, frame[1..3], @intCast(msg.data.len), .big);
    @memcpy(frame[3..][0..msg.data.len], msg.data);
    const plaintext = frame[0 .. 3 + msg.data.len];

    var combined: [max_message_size + 3 + 16]u8 = undefined;
    crypto.nacl.SecretBox.seal(combined[0 .. plaintext.len + 16], plaintext, nonce, self.write_key);
    const ct = combined[16..][0..plaintext.len];

    try self.writer.writeAll(&nonce);
    try self.writer.writeAll(combined[0..16]);
    try self.writer.writeInt(u16, @intCast(ct.len), .big);
    try self.writer.writeAll(ct);
}

const DecryptedFrame = struct {
    id: u8,
    data: []const u8,
};

fn readEncrypted(self: anytype) !DecryptedFrame {
    const nonce = (try self.reader.take(24))[0..24].*;
    const tag = try self.reader.take(16);
    const ct_len = try self.reader.takeInt(u16, .big);
    if (ct_len < 3) return error.MessageTooLarge;
    const ct = try self.reader.take(ct_len);

    var combined: [max_message_size + 3 + 16]u8 = undefined;
    @memcpy(combined[0..16], tag);
    @memcpy(combined[16..][0..ct_len], ct);

    var plain: [max_message_size + 3]u8 = undefined;
    crypto.nacl.SecretBox.open(plain[0..ct_len], combined[0 .. ct_len + 16], nonce, self.read_key) catch return error.DecryptFailed;

    const counter = std.mem.readInt(u64, nonce[0..8], .big);
    if (counter != self.read_counter) return error.ReplayDetected;
    self.read_counter += 1;

    const id = plain[0];
    const payload_len = std.mem.readInt(u16, plain[1..3], .big);
    const copy = try self.allocator.dupe(u8, plain[3..][0..payload_len]);
    return .{ .id = id, .data = copy };
}
