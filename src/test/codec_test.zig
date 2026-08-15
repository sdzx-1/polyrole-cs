const std = @import("std");
const codec = @import("polyrole_cs").codec;

const BoolMsg = union(enum) {
    m: struct { data: bool },
};

const SliceMsg = union(enum) {
    m: struct { data: []const u8 },
};

const TestStateId = enum { s0 };

// 解码遇到非 0/1 的布尔字节时报错而非崩溃
test "decode: invalid bool byte is an error, not a panic" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // state_id
    buf[4] = 0; // 标签
    buf[5] = 2; // 非 0 或 1
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.InvalidValue, codec.decode(&r, TestStateId.s0, BoolMsg));
}

// 解码遇到超出枚举范围的标签时报错而非崩溃
test "decode: out-of-range tag is an error, not a panic" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // 状态 ID
    buf[4] = 7; // 超出单字段 union 的标签
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.InvalidValue, codec.decode(&r, TestStateId.s0, BoolMsg));
}

// 解码超长切片长度（越过 reader 边界）时被拒绝
test "decode: oversized slice length is rejected" {
    var buf = [_]u8{0} ** 16;
    std.mem.writeInt(u32, buf[0..4], 0, .big); // 状态 ID
    buf[4] = 0; // 标签
    std.mem.writeInt(usize, buf[5..13], 4096, .big); // 长度超过 reader 边界
    var r = std.Io.Reader.fixed(&buf);
    try std.testing.expectError(error.EndOfStream, codec.decode(&r, TestStateId.s0, SliceMsg));
}
