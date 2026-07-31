const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const crypto = std.crypto;
const zio = @import("zio");
const family_mux = @import("family_mux_channel.zig");
const Stream = zio.net.Stream;

/// 流通道
pub const StreamChannel = struct {
    stream: Stream,
    rbuff: []u8,
    wbuff: []u8,
    /// 从线上解码单个 `[]const u8` 切片的上限。
    /// 防止攻击者控制长度前缀导致流路径上的无界阻塞读取或缓冲膨胀。
    max_slice_len: usize,
    stream_writer: Stream.Writer,
    stream_reader: Stream.Reader,

    pub fn init(
        self: *@This(),
        gpa: std.mem.Allocator,
        stream: zio.net.Stream,
        r_size: usize,
        w_size: usize,
        max_slice_len: usize,
    ) !void {
        self.stream = stream;
        const rbuff = try gpa.alloc(u8, r_size);
        const wbuff = try gpa.alloc(u8, w_size);
        self.rbuff = rbuff;
        self.wbuff = wbuff;
        self.max_slice_len = max_slice_len;
        self.stream_reader = stream.reader(rbuff);
        self.stream_writer = stream.writer(wbuff);
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.rbuff);
        gpa.free(self.wbuff);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        try codec.encode(&self.stream_writer.interface, state_id, val);
        try (&self.stream_writer.interface).flush();
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        const res = try codec.decode(&self.stream_reader.interface, state_id, T, self.max_slice_len);
        return res;
    }
};

/// 加密传输通道——包装一个借用的 StreamChannel，使用先前 TLS 握手
/// 派生的密钥进行 AEAD 加密。
///
/// 每条消息的线上格式：
///   nonce(24) || tag(16) || ct_len(2 BE) || ciphertext(ct_len)
///
/// 密文内的载荷为：msg_len(2 BE) || protocol_message(msg_len)。
/// 线上的 ct_len 是帧定界符；AEAD 信封内被认证的 msg_len 才是可信来源。
///
/// nonce 是单调计数器（u64 大端，零填充到 24 字节）。
/// 每个方向有独立的计数器，从 0 开始。
pub const TlsChannel = struct {
    /// 借用——调用方拥有 StreamChannel，必须在 TlsChannel 之后 deinit 它。
    inner: *StreamChannel,
    write_key: [32]u8,
    read_key: [32]u8,
    write_counter: u64,
    read_counter: u64,

    /// 加密前编码协议消息的缓冲区。
    /// 前 2 字节保留给被认证的长度前缀。
    encode_buf: []u8,
    /// 解码前存放解密明文的缓冲区
    decode_buf: []u8,
    /// seal/open 组合输出（tag || ciphertext）的临时缓冲区
    combined_buf: []u8,

    pub fn init(
        self: *@This(),
        gpa: std.mem.Allocator,
        inner: *StreamChannel,
        write_key: [32]u8,
        read_key: [32]u8,
        buf_size: usize,
    ) !void {
        std.debug.assert(buf_size >= 2);
        std.debug.assert(buf_size <= 65535);
        self.inner = inner;
        self.encode_buf = try gpa.alloc(u8, buf_size);
        self.decode_buf = try gpa.alloc(u8, buf_size);
        self.combined_buf = try gpa.alloc(u8, buf_size + 16);
        self.write_key = write_key;
        self.read_key = read_key;
        self.write_counter = 0;
        self.read_counter = 0;
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.encode_buf);
        gpa.free(self.decode_buf);
        gpa.free(self.combined_buf);
        @memset(&self.write_key, 0);
        @memset(&self.read_key, 0);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        // 在 2 字节长度前缀之后编码协议消息
        const buf = self.encode_buf[2..];
        var writer = Io.Writer.fixed(buf);
        try codec.encode(&writer, state_id, val);
        const msg = buf[0..writer.end];

        // 前置被认证的长度前缀
        std.mem.writeInt(u16, self.encode_buf[0..2], @intCast(msg.len), .big);
        try self.sealAndSend(self.encode_buf[0 .. 2 + msg.len]);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        const plain = try self.recordRead();
        const msg_len = std.mem.readInt(u16, plain[0..2], .big);
        var reader = Io.Reader.fixed(plain[2..][0..msg_len]);
        return try codec.decode(&reader, state_id, T, self.decode_buf.len);
    }

    /// 记录模式写入：将一条不透明帧作为单个记录加密。
    ///
    /// 帧格式为 `[protocol_id(1) || len(2 BE) || payload]`——与
    /// `family_mux_channel.MultiplexChannel` 帧的线上格式完全一致。
    /// 用于通过 `transport()` 在 TLS 之上叠加 Mux。
    pub fn recordWrite(self: *@This(), id: u8, payload: []const u8) !void {
        const frame_len = 3 + payload.len;
        std.debug.assert(frame_len <= std.math.maxInt(u16));
        std.debug.assert(frame_len + 2 <= self.encode_buf.len);
        std.mem.writeInt(u16, self.encode_buf[0..2], @intCast(frame_len), .big);
        self.encode_buf[2] = id;
        std.mem.writeInt(u16, self.encode_buf[3..5], @intCast(payload.len), .big);
        @memcpy(self.encode_buf[5..][0..payload.len], payload);
        try self.sealAndSend(self.encode_buf[0 .. 2 + frame_len]);
    }

    /// 记录模式读取：读取一个记录并返回其明文。
    ///
    /// 返回的切片布局为 `[frame_len(2 BE) || frame]`，在下次读取本通道之前有效。
    pub fn recordRead(self: *@This()) ![]const u8 {
        const sr = &self.inner.stream_reader.interface;

        const nonce = (try sr.take(24))[0..24].*;
        const tag = try sr.take(16);
        const ct_len = try sr.takeInt(u16, .big);
        if (ct_len < 2 or ct_len > self.decode_buf.len) return error.MessageTooLarge;

        const ct = try sr.take(ct_len);

        // AEAD 解密
        const combined = self.combined_buf[0 .. ct_len + 16];
        @memcpy(combined[0..16], tag);
        @memcpy(combined[16..][0..ct_len], ct);
        crypto.nacl.SecretBox.open(
            self.decode_buf[0..ct_len],
            combined,
            nonce,
            self.read_key,
        ) catch return error.DecryptFailed;

        // 读取被认证的消息长度
        const msg_len = std.mem.readInt(u16, self.decode_buf[0..2], .big);
        if (msg_len != ct_len - 2) return error.BadLength;

        // 校验并推进计数器
        const counter = std.mem.readInt(u64, nonce[0..8], .big);
        if (counter != self.read_counter) return error.ReplayDetected;
        std.debug.assert(self.read_counter < std.math.maxInt(u64));
        self.read_counter += 1;

        return self.decode_buf[0 .. 2 + msg_len];
    }

    /// 将已建立的加密会话暴露为 Mux 传输层。
    ///
    /// `MultiplexChannel` 直接叠在 TLS 记录层之上：
    /// 每条 mux 帧作为一条被认证的记录传输，因此整个协议族
    /// 共享一次握手和一套密钥。
    ///
    /// 返回的传输层不拥有流；调用方保留对 TlsChannel 及其底层
    /// StreamChannel 的所有权。
    pub fn transport(self: *@This()) family_mux.Transport {
        return .{
            .context = self,
            .stream = self.inner.stream,
            .owns_stream = false,
            .writeFrame = tlsWriteFrame,
            .readFrame = tlsReadFrame,
            .shutdownReceive = tlsShutdownReceive,
        };
    }

    /// 原子性地推进计数器并发送一条 AEAD 记录。
    /// nonce 永不复用——即使后续 flush 失败且调用方重试。
    fn sealAndSend(self: *@This(), plaintext: []const u8) !void {
        const this_counter = self.write_counter;
        std.debug.assert(this_counter < std.math.maxInt(u64));
        self.write_counter += 1;

        // 从已提交的计数器构造 nonce
        var nonce: [24]u8 = [_]u8{0} ** 24;
        std.mem.writeInt(u64, nonce[0..8], this_counter, .big);

        // AEAD 加密
        const combined = self.combined_buf[0 .. plaintext.len + 16];
        crypto.nacl.SecretBox.seal(combined, plaintext, nonce, self.write_key);
        const ct = combined[16..][0..plaintext.len];

        // 线上格式：nonce || tag || ct_len || ciphertext
        const sw = &self.inner.stream_writer.interface;
        try sw.writeAll(&nonce);
        try sw.writeAll(combined[0..16]); // 标签
        try sw.writeInt(u16, @intCast(ct.len), .big);
        try sw.writeAll(ct);
        try sw.flush();
    }
};

// ─────────────────── Mux 传输适配器 ───────────────────

fn tlsWriteFrame(ctx: *anyopaque, id: u8, payload: []const u8) anyerror!void {
    const tc: *TlsChannel = @ptrCast(@alignCast(ctx));
    try tc.recordWrite(id, payload);
}

fn tlsReadFrame(ctx: *anyopaque) anyerror!family_mux.ReadFrame {
    const tc: *TlsChannel = @ptrCast(@alignCast(ctx));
    const plain = try tc.recordRead();
    const frame_len = std.mem.readInt(u16, plain[0..2], .big);
    if (plain.len != frame_len + 2) return error.BadLength;
    const id = plain[2];
    const payload_len = std.mem.readInt(u16, plain[3..5], .big);
    if (frame_len != 3 + payload_len) return error.BadLength;
    return .{ .id = id, .payload = plain[5..][0..payload_len] };
}

fn tlsShutdownReceive(ctx: *anyopaque) void {
    const tc: *TlsChannel = @ptrCast(@alignCast(ctx));
    tc.inner.stream.socket.shutdown(.receive) catch {};
}
