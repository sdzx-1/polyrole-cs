const std = @import("std");
const codec = @import("codec.zig");
const Io = std.Io;

//stream channel

pub const StreamChannel = struct {
    writer: *std.Io.Writer,
    reader: *std.Io.Reader,

    pub fn send(self: @This(), state_id: anytype, _: type, val: anytype) !void {
        try codec.encode(self.writer, state_id, val);
    }

    pub fn recv(self: @This(), state_id: anytype, T: type) !T {
        const res = try codec.decode(self.reader, state_id, T);
        return res;
    }
};
