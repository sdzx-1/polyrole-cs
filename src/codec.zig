const std = @import("std");

pub fn encode(writer: *std.Io.Writer, state_id: anytype, val: anytype) !void {
    const id: u32 = @intFromEnum(state_id);
    try writer.writeInt(u32, id, .big);
    switch (val) {
        inline else => |msg, tag| {
            try writer.writeByte(@intFromEnum(tag));
            const data = msg.data;
            try encode_anytype(writer, data);
        },
    }
}

pub fn decode(reader: *std.Io.Reader, state_id: anytype, T: type, max_slice_len: usize) !T {
    const id: u32 = @intFromEnum(state_id);
    const rid = try reader.takeInt(u32, .big);
    if (id != rid) {
        std.log.err("curr_id: {d}, reciv_id: {d}", .{ id, rid });
        return error.IncorrectStatusReceived;
    }
    const recv_tag_num = try reader.takeByte();
    const Tag = std.meta.Tag(T);
    if (recv_tag_num >= @typeInfo(Tag).@"enum".fields.len) return error.InvalidValue;
    const tag: Tag = @enumFromInt(recv_tag_num);
    switch (tag) {
        inline else => |t| {
            const Data = @FieldType(TagPayloadByName(T, @tagName(t)), "data");
            return @unionInit(T, @tagName(t), .{ .data = try decode_type(reader, Data, max_slice_len) });
        },
    }
}

pub fn encode_anytype(writer: *std.Io.Writer, data: anytype) !void {
    switch (@typeInfo(@TypeOf(data))) {
        .void => {},
        .bool => {
            const v: u8 = if (data) 1 else 0;
            try writer.writeByte(v);
        },
        .int => {
            try writer.writeInt(@TypeOf(data), data, .big);
        },
        .pointer => |p| {
            if (p.is_const == true and p.child == u8) {
                const len: usize = data.len;
                try writer.writeInt(usize, len, .big);
                try writer.writeAll(data);
            } else {
                @compileError("Not impl!");
            }
        },
        .array => |arr| {
            if (arr.child == u8) {
                try writer.writeAll(&data);
            } else {
                for (data) |item| {
                    try encode_anytype(writer, item);
                }
            }
        },
        .@"struct" => |stru| {
            inline for (stru.fields) |struct_field| {
                try encode_anytype(writer, @field(data, struct_field.name));
            }
        },
        else => @compileError("Not impl!"),
    }
}

pub fn decode_type(reader: *std.Io.Reader, Data: type, max_slice_len: usize) !Data {
    switch (@typeInfo(Data)) {
        .void => return {},
        .bool => {
            const data = try reader.takeByte();
            const bv: bool = switch (data) {
                0 => false,
                1 => true,
                else => return error.InvalidValue,
            };
            return bv;
        },
        .int => {
            const data = try reader.takeInt(Data, .big);
            return data;
        },
        .pointer => |p| {
            if (p.is_const == true and p.child == u8) {
                const len = try reader.takeInt(usize, .big);
                if (len > max_slice_len) return error.MessageTooLarge;
                const str = try reader.take(len);
                return str;
            } else {
                @compileError("Not impl!");
            }
        },
        .array => |arr| {
            if (arr.child == u8) {
                var result: [arr.len]u8 = undefined;
                const bytes = try reader.take(arr.len);
                @memcpy(&result, bytes);
                return result;
            } else {
                var result: [arr.len]arr.child = undefined;
                for (&result) |*item| {
                    item.* = try decode_type(reader, arr.child, max_slice_len);
                }
                return result;
            }
        },
        .@"struct" => |stru| {
            var data: Data = undefined;
            inline for (stru.fields) |struct_field| {
                @field(data, struct_field.name) = try decode_type(reader, struct_field.type, max_slice_len);
            }
            return data;
        },
        else => @compileError("Not impl!"),
    }
}

pub fn TagPayloadByName(comptime U: type, comptime tag_name: []const u8) type {
    const info = @typeInfo(U).@"union";

    inline for (info.fields) |field_info| {
        if (comptime std.mem.eql(u8, field_info.name, tag_name))
            return field_info.type;
    }

    @compileError("no field '" ++ tag_name ++ "' in union '" ++ @typeName(U) ++ "'");
}

// ─────────────────── malformed-input tests ───────────────────

const BoolMsg = union(enum) {
    m: struct { data: bool },
};

const SliceMsg = union(enum) {
    m: struct { data: []const u8 },
};

const TestStateId = enum { s0 };

test "decode: invalid bool byte is an error, not a panic" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // state_id
    buf[4] = 0; // tag
    buf[5] = 2; // not 0 or 1
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.InvalidValue, decode(&r, TestStateId.s0, BoolMsg, 1024));
}

test "decode: out-of-range tag is an error, not a panic" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // state_id
    buf[4] = 7; // tag outside the single-field union
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.InvalidValue, decode(&r, TestStateId.s0, BoolMsg, 1024));
}

test "decode: oversized slice length is rejected" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // state_id
    buf[4] = 0; // tag
    std.mem.writeInt(usize, buf[5..13], 4096, .big); // len > max_slice_len
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.MessageTooLarge, decode(&r, TestStateId.s0, SliceMsg, 1024));
}
