const std = @import("std");
const codec = @import("codec.zig");
const crypto = std.crypto;
const Io = std.Io;
const Stream = Io.net.Stream;

///stream channel
pub const StreamChannel = struct {
    stream: Stream,
    rbuff: []u8,
    wbuff: []u8,
    stream_writer: Stream.Writer,
    stream_reader: Stream.Reader,

    pub fn init(
        self: *@This(),
        io: Io,
        gpa: std.mem.Allocator,
        stream: Io.net.Stream,
        r_size: usize,
        w_size: usize,
    ) !void {
        self.stream = stream;
        const rbuff = try gpa.alloc(u8, r_size);
        const wbuff = try gpa.alloc(u8, w_size);
        self.rbuff = rbuff;
        self.wbuff = wbuff;
        self.stream_reader = stream.reader(io, rbuff);
        self.stream_writer = stream.writer(io, wbuff);
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.rbuff);
        gpa.free(self.wbuff);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        try codec.encode(&self.stream_writer.interface, state_id, val);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        const res = try codec.decode(&self.stream_reader.interface, state_id, T);
        return res;
    }
};

/// Encrypted transport channel — wraps a StreamChannel with AEAD encryption
/// using keys derived from a prior TLS handshake.
///
/// Wire format per message:
///   nonce(24) || tag(16) || ct_len(2 BE) || ciphertext(ct_len)
///
/// The nonce is a monotonic counter (u64 big-endian, zero-padded to 24 bytes).
/// Each direction has its own counter starting from 0.
pub const TlsChannel = struct {
    inner: StreamChannel,
    write_key: [32]u8,
    read_key: [32]u8,
    write_counter: u64,
    read_counter: u64,

    /// Buffer for encoding protocol messages before encryption
    encode_buf: []u8,
    /// Buffer for decrypted plaintext before decoding
    decode_buf: []u8,

    pub fn init(
        self: *@This(),
        io: Io,
        gpa: std.mem.Allocator,
        stream: Io.net.Stream,
        write_key: [32]u8,
        read_key: [32]u8,
        buf_size: usize,
    ) !void {
        try self.inner.init(io, gpa, stream, buf_size, buf_size);
        self.encode_buf = try gpa.alloc(u8, buf_size);
        self.decode_buf = try gpa.alloc(u8, buf_size);
        self.write_key = write_key;
        self.read_key = read_key;
        self.write_counter = 0;
        self.read_counter = 0;
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        self.inner.deinit(gpa);
        gpa.free(self.encode_buf);
        gpa.free(self.decode_buf);
    }

    pub fn send(self: *@This(), state_id: anytype, _: type, val: anytype) !void {
        // Encode protocol message to plaintext buffer
        var buf = self.encode_buf;
        var writer = Io.Writer.fixed(buf);
        try codec.encode(&writer, state_id, val);
        const plaintext = buf[0..writer.end];

        // Build nonce from counter
        var nonce: [24]u8 = [_]u8{0} ** 24;
        std.mem.writeInt(u64, nonce[0..8], self.write_counter, .big);
        self.write_counter += 1;

        // AEAD encrypt
        const ct_overhead = 16;
        const combined_len = plaintext.len + ct_overhead;
        var combined: [4096]u8 = undefined;
        if (combined_len > combined.len) return error.MessageTooLarge;
        crypto.nacl.SecretBox.seal(combined[0..combined_len], plaintext, nonce, self.write_key);

        // Wire format: nonce || tag || ct_len || ciphertext
        const sw = &self.inner.stream_writer.interface;
        try sw.writeAll(&nonce);
        try sw.writeAll(combined[0..16]); // tag
        try sw.writeInt(u16, @intCast(plaintext.len), .big);
        try sw.writeAll(combined[16..][0..plaintext.len]);
        try sw.flush();
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        const sr = &self.inner.stream_reader.interface;

        const nonce = try sr.take(24);
        const tag = try sr.take(16);
        const ct_len = try sr.takeInt(u16, .big);
        if (ct_len > self.decode_buf.len) return error.MessageTooLarge;

        const ct = try sr.take(ct_len);

        // AEAD decrypt
        var combined: [4096 + 16]u8 = undefined;
        @memcpy(combined[0..16], tag);
        @memcpy(combined[16..][0..ct_len], ct);
        crypto.nacl.SecretBox.open(
            self.decode_buf[0..ct_len],
            combined[0 .. ct_len + 16],
            nonce[0..24].*,
            self.read_key,
        ) catch return error.DecryptFailed;

        // Verify and advance counter
        const counter = std.mem.readInt(u64, nonce[0..8], .big);
        if (counter != self.read_counter) return error.ReplayDetected;
        self.read_counter += 1;

        // Decode protocol message from decrypted plaintext
        var reader = Io.Reader.fixed(self.decode_buf[0..ct_len]);
        return try codec.decode(&reader, state_id, T);
    }
};
