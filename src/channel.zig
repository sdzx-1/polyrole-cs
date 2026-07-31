const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const crypto = std.crypto;
const zio = @import("zio");
const family_mux = @import("family_mux_channel.zig");
const Stream = zio.net.Stream;

///stream channel
pub const StreamChannel = struct {
    stream: Stream,
    rbuff: []u8,
    wbuff: []u8,
    /// Upper bound for a single `[]const u8` slice decoded from the wire.
    /// Guards against attacker-controlled length prefixes causing unbounded
    /// blocking reads or buffer growth on the stream path.
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

/// Encrypted transport channel — wraps a borrowed StreamChannel with AEAD
/// encryption using keys derived from a prior TLS handshake.
///
/// Wire format per message:
///   nonce(24) || tag(16) || ct_len(2 BE) || ciphertext(ct_len)
///
/// The payload inside ciphertext is:  msg_len(2 BE) || protocol_message(msg_len).
/// ct_len on the wire is a frame delimiter; the authenticated msg_len inside the
/// AEAD envelope is the source of truth.
///
/// The nonce is a monotonic counter (u64 big-endian, zero-padded to 24 bytes).
/// Each direction has its own counter starting from 0.
pub const TlsChannel = struct {
    /// Borrowed — caller owns the StreamChannel and must deinit it after TlsChannel.
    inner: *StreamChannel,
    write_key: [32]u8,
    read_key: [32]u8,
    write_counter: u64,
    read_counter: u64,

    /// Buffer for encoding protocol messages before encryption.
    /// First 2 bytes reserved for the authenticated length prefix.
    encode_buf: []u8,
    /// Buffer for decrypted plaintext before decoding
    decode_buf: []u8,
    /// Scratch buffer for seal/open combined output (tag || ciphertext)
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
        // Encode protocol message after 2-byte length prefix
        const buf = self.encode_buf[2..];
        var writer = Io.Writer.fixed(buf);
        try codec.encode(&writer, state_id, val);
        const msg = buf[0..writer.end];

        // Prepend authenticated length prefix
        std.mem.writeInt(u16, self.encode_buf[0..2], @intCast(msg.len), .big);
        try self.sealAndSend(self.encode_buf[0 .. 2 + msg.len]);
    }

    pub fn recv(self: *@This(), state_id: anytype, T: type) !T {
        const plain = try self.recordRead();
        const msg_len = std.mem.readInt(u16, plain[0..2], .big);
        var reader = Io.Reader.fixed(plain[2..][0..msg_len]);
        return try codec.decode(&reader, state_id, T, self.decode_buf.len);
    }

    /// Record-mode write: encrypt one opaque frame as a single record.
    ///
    /// The frame is `[protocol_id(1) || len(2 BE) || payload]` — the exact
    /// wire shape a `family_mux_channel.MultiplexChannel` frame has. Used to
    /// layer Mux over TLS via `transport()`.
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

    /// Record-mode read: read one record and return its plaintext.
    ///
    /// Returned slice layout: `[frame_len(2 BE) || frame]`, valid until the
    /// next read on this channel.
    pub fn recordRead(self: *@This()) ![]const u8 {
        const sr = &self.inner.stream_reader.interface;

        const nonce = (try sr.take(24))[0..24].*;
        const tag = try sr.take(16);
        const ct_len = try sr.takeInt(u16, .big);
        if (ct_len < 2 or ct_len > self.decode_buf.len) return error.MessageTooLarge;

        const ct = try sr.take(ct_len);

        // AEAD decrypt
        const combined = self.combined_buf[0 .. ct_len + 16];
        @memcpy(combined[0..16], tag);
        @memcpy(combined[16..][0..ct_len], ct);
        crypto.nacl.SecretBox.open(
            self.decode_buf[0..ct_len],
            combined,
            nonce,
            self.read_key,
        ) catch return error.DecryptFailed;

        // Read authenticated message length
        const msg_len = std.mem.readInt(u16, self.decode_buf[0..2], .big);
        if (msg_len != ct_len - 2) return error.BadLength;

        // Verify and advance counter
        const counter = std.mem.readInt(u64, nonce[0..8], .big);
        if (counter != self.read_counter) return error.ReplayDetected;
        std.debug.assert(self.read_counter < std.math.maxInt(u64));
        self.read_counter += 1;

        return self.decode_buf[0 .. 2 + msg_len];
    }

    /// Exposes the established encrypted session as a Mux transport.
    ///
    /// The `MultiplexChannel` is layered directly on the TLS record layer:
    /// every mux frame travels as one authenticated record, so the whole
    /// protocol family shares a single handshake and key set.
    ///
    /// The returned transport does not own the stream; the caller keeps
    /// ownership of the TlsChannel and its underlying StreamChannel.
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

    /// Atomically advance the counter and send one AEAD record.
    /// Nonce is never reused, even if a later flush fails and the caller retries.
    fn sealAndSend(self: *@This(), plaintext: []const u8) !void {
        const this_counter = self.write_counter;
        std.debug.assert(this_counter < std.math.maxInt(u64));
        self.write_counter += 1;

        // Build nonce from the already-committed counter
        var nonce: [24]u8 = [_]u8{0} ** 24;
        std.mem.writeInt(u64, nonce[0..8], this_counter, .big);

        // AEAD encrypt
        const combined = self.combined_buf[0 .. plaintext.len + 16];
        crypto.nacl.SecretBox.seal(combined, plaintext, nonce, self.write_key);
        const ct = combined[16..][0..plaintext.len];

        // Wire format: nonce || tag || ct_len || ciphertext
        const sw = &self.inner.stream_writer.interface;
        try sw.writeAll(&nonce);
        try sw.writeAll(combined[0..16]); // tag
        try sw.writeInt(u16, @intCast(ct.len), .big);
        try sw.writeAll(ct);
        try sw.flush();
    }
};

// ─────────────────── Mux transport adapter ───────────────────

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
