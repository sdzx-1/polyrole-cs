const std = @import("std");
const crypto = std.crypto;
const zio = @import("zio");
const polyrole = @import("polyrole_cs");
const StreamChannel = polyrole.channel.StreamChannel;
const TlsChannel = polyrole.channel.TlsChannel;
const InMemoryChannel = polyrole.channel.InMemoryChannel;
const HalfChannel = polyrole.channel.HalfChannel;
const deriveRotationKey = polyrole.channel.deriveRotationKey;

// ─────────────────── 记录层错误路径测试夹具 ───────────────────

const SocketPair = struct {
    client: zio.net.Stream,
    server: zio.net.Stream,

    fn connect() !SocketPair {
        const lh = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
        var listener = try lh.listen(.{});
        defer listener.close();
        const client = try listener.socket.address.connect(.{});
        errdefer client.close();
        const server = try listener.accept(.{});
        return .{ .client = client, .server = server };
    }
};

/// 用给定密钥手工构造一条 AEAD 记录（nonce || tag || ct_len || ct），
/// 返回写入 out 的字节数。`typ` 编码进 nonce 末字节（0=数据，1=KeyUpdate）。
fn craftRecord(key: [32]u8, counter: u64, typ: u8, plaintext: []const u8, out: []u8) usize {
    var nonce: [24]u8 = [_]u8{0} ** 24;
    std.mem.writeInt(u64, nonce[0..8], counter, .big);
    nonce[23] = typ;
    var scratch: [2048]u8 = undefined;
    const combined = scratch[0 .. plaintext.len + 16];
    crypto.nacl.SecretBox.seal(combined, plaintext, nonce, key);
    @memcpy(out[0..24], &nonce);
    @memcpy(out[24..40], combined[0..16]); // tag
    std.mem.writeInt(u32, out[40..44], @intCast(plaintext.len), .big);
    @memcpy(out[44..][0..plaintext.len], combined[16..][0..plaintext.len]);
    return 44 + plaintext.len;
}

fn writeRaw(sc: *StreamChannel, bytes: []const u8) !void {
    const sw = &sc.stream_writer.interface;
    try sw.writeAll(bytes);
    try sw.flush();
}

test "tls channel: replayed record is rejected" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc_server, key, key, 256);
    defer tc.deinit(allocator);

    // 帧：id=7, payload="hi" → frame_len=5；明文 = [len(2) || frame]
    const frame = [_]u8{ 7, 0, 2, 'h', 'i' };
    var plain: [7]u8 = undefined;
    std.mem.writeInt(u16, plain[0..2], @intCast(frame.len), .big);
    @memcpy(plain[2..], &frame);

    var record: [2048]u8 = undefined;
    const n = craftRecord(key, 0, 0, &plain, &record);

    try writeRaw(&sc_client, record[0..n]);
    _ = try tc.recordRead(); // 第一帧：counter 0 通过

    // 原样重放同一条记录 → ReplayDetected
    try writeRaw(&sc_client, record[0..n]);
    try testing.expectError(error.ReplayDetected, tc.recordRead());
}

test "tls channel: corrupted tag fails AEAD" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc_server, key, key, 256);
    defer tc.deinit(allocator);

    const plain = [_]u8{ 0x00, 0x03, 1, 0, 1, 'x' };
    var record: [2048]u8 = undefined;
    const n = craftRecord(key, 0, 0, &plain, &record);
    record[24] ^= 0x01; // 破坏 tag 的第一个字节

    try writeRaw(&sc_client, record[0..n]);
    try testing.expectError(error.DecryptFailed, tc.recordRead());
}

test "tls channel: authenticated length mismatch is rejected" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc_server, key, key, 256);
    defer tc.deinit(allocator);

    // 前缀声称 2 字节，但实际载荷 3 字节 → ct_len=5, msg_len=2 ≠ 3 → BadLength
    const plain = [_]u8{ 0x00, 0x02, 0xAA, 0xBB, 0xCC };
    var record: [2048]u8 = undefined;
    const n = craftRecord(key, 0, 0, &plain, &record);

    try writeRaw(&sc_client, record[0..n]);
    try testing.expectError(error.BadLength, tc.recordRead());
}

test "tls channel: oversized record is rejected before reading its body" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    // 缓冲区只有 32 字节，记录却声明 100 字节正文。
    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc_server, key, key, 32);
    defer tc.deinit(allocator);

    // 只写头部（nonce + tag + ct_len=100），recordRead 在读取正文前就会拒绝。
    var hdr: [44]u8 = [_]u8{0} ** 44;
    std.mem.writeInt(u64, hdr[0..8], 0, .big); // counter 0
    std.mem.writeInt(u32, hdr[40..44], 100, .big);

    try writeRaw(&sc_client, &hdr);
    try testing.expectError(error.MessageTooLarge, tc.recordRead());
}

test "tls channel: key rotation keeps channel working" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    var tc_client: TlsChannel = undefined;
    try tc_client.init(allocator, &sc_client, key, key, 256);
    defer tc_client.deinit(allocator);
    var tc_server: TlsChannel = undefined;
    try tc_server.init(allocator, &sc_server, key, key, 256);
    defer tc_server.deinit(allocator);

    // 服务端每写 2 条记录就轮换（时间触发禁用）
    tc_server.setRotationConfig(.{ .interval_ns = 0, .record_threshold = 2 });

    // 4 条消息：第 3 条发送前触发一次轮换（KeyUpdate + 新密钥续传）
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        var buf: [8]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "msg-{d}", .{i});
        try tc_server.sealAndSend(msg);
        const got = try tc_client.recordReadRaw();
        try testing.expectEqualStrings(msg, got);
    }

    // 轮换确实发生：两侧各自推进了一个 epoch，写密钥已派生
    try testing.expectEqual(@as(u32, 1), tc_server.write_epoch);
    try testing.expectEqual(@as(u32, 1), tc_client.read_epoch);
    try testing.expect(!std.mem.eql(u8, &key, &tc_server.write_key));
    try testing.expect(!std.mem.eql(u8, &key, &tc_client.read_key));
}

test "tls channel: out-of-order key update rejected" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const pair = try SocketPair.connect();
    defer pair.client.close();
    defer pair.server.close();

    const key = [_]u8{0x42} ** 32;
    var sc_client: StreamChannel = undefined;
    try sc_client.init(allocator, pair.client, 256, 256, 4096);
    defer sc_client.deinit(allocator);
    var sc_server: StreamChannel = undefined;
    try sc_server.init(allocator, pair.server, 256, 256, 4096);
    defer sc_server.deinit(allocator);

    var tc: TlsChannel = undefined;
    try tc.init(allocator, &sc_server, key, key, 256);
    defer tc.deinit(allocator);

    // 合法 KeyUpdate：epoch=1，原密钥，counter 0
    var epoch1: [4]u8 = undefined;
    std.mem.writeInt(u32, &epoch1, 1, .big);
    var rec: [2048]u8 = undefined;
    var n = craftRecord(key, 0, 1, &epoch1, &rec);
    try writeRaw(&sc_client, rec[0..n]);

    // 普通数据（新密钥 derive(key,1)，counter 0）→ recordReadRaw 返回，
    // 证明 KeyUpdate 已被吸收
    const new_key = deriveRotationKey(key, 1);
    n = craftRecord(new_key, 0, 0, "hi", &rec);
    try writeRaw(&sc_client, rec[0..n]);
    const got = try tc.recordReadRaw();
    try testing.expectEqualStrings("hi", got);

    // 乱序 KeyUpdate：声明 epoch=3（预期 2），新密钥 counter 1 →
    // 计数器匹配、解密成功，但 epoch 校验失败
    var epoch3: [4]u8 = undefined;
    std.mem.writeInt(u32, &epoch3, 3, .big);
    n = craftRecord(new_key, 1, 1, &epoch3, &rec);
    try writeRaw(&sc_client, rec[0..n]);
    try testing.expectError(error.KeyRotationOutOfOrder, tc.recordReadRaw());
}

// ─────────────────── InMemoryChannel 全双工验证测试 ───────────────────

const SmcState = enum { hello };
const SmcMsg = union(SmcState) {
    hello: struct { data: []const u8 },
};

/// 期望消息为 "{prefix}-{i}"。
fn expectPrefixed(prefix: []const u8, i: usize, data: []const u8) !void {
    var buf: [32]u8 = undefined;
    const expected = try std.fmt.bufPrint(&buf, "{s}-{d}", .{ prefix, i });
    try std.testing.expectEqualStrings(expected, data);
}

test "smc: full-duplex - both directions flow concurrently" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var half1: HalfChannel = undefined;
    var half2: HalfChannel = undefined;
    try half1.init(allocator, 1024);
    try half2.init(allocator, 1024);
    defer half1.deinit(allocator);
    defer half2.deinit(allocator);

    const ch_c: InMemoryChannel = .{ .max_slice_len = 4096, .half_self = &half1, .half_peer = &half2 };
    const ch_s: InMemoryChannel = .{ .max_slice_len = 4096, .half_self = &half2, .half_peer = &half1 };

    const n = 200;
    const Side = struct {
        // 客户端：先发后收。第 i+1 条发送只依赖对端收走 c-i，
        // 不依赖 s-i 到达——两个方向并行流动。
        fn client(c: *const InMemoryChannel, count: usize) !void {
            var i: usize = 0;
            while (i < count) : (i += 1) {
                var buf: [32]u8 = undefined;
                const text = try std.fmt.bufPrint(&buf, "c-{d}", .{i});
                try c.send(SmcState.hello, SmcMsg, @as(SmcMsg, .{ .hello = .{ .data = text } }));
                const m = try c.recv(SmcState.hello, SmcMsg);
                switch (m) {
                    .hello => |h| try expectPrefixed("s", i, h.data),
                }
            }
        }

        // 服务端：先收后发，相位与客户端相反。
        fn server(c: *const InMemoryChannel, count: usize) !void {
            var i: usize = 0;
            while (i < count) : (i += 1) {
                const m = try c.recv(SmcState.hello, SmcMsg);
                switch (m) {
                    .hello => |h| try expectPrefixed("c", i, h.data),
                }
                var buf: [32]u8 = undefined;
                const text = try std.fmt.bufPrint(&buf, "s-{d}", .{i});
                try c.send(SmcState.hello, SmcMsg, @as(SmcMsg, .{ .hello = .{ .data = text } }));
            }
        }
    };

    var t1 = try rt.spawn(Side.client, .{ &ch_c, n });
    var t2 = try rt.spawn(Side.server, .{ &ch_s, n });
    try t1.join();
    try t2.join();
}

test "smc: server sends first - client never receives its own message" {
    const allocator = std.testing.allocator;
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    var half1: HalfChannel = undefined;
    var half2: HalfChannel = undefined;
    try half1.init(allocator, 1024);
    try half2.init(allocator, 1024);
    defer half1.deinit(allocator);
    defer half2.deinit(allocator);

    const ch_c: InMemoryChannel = .{ .max_slice_len = 4096, .half_self = &half1, .half_peer = &half2 };
    const ch_s: InMemoryChannel = .{ .max_slice_len = 4096, .half_self = &half2, .half_peer = &half1 };

    const Side = struct {
        fn serverFirst(c: *const InMemoryChannel) !void {
            try c.send(SmcState.hello, SmcMsg, @as(SmcMsg, .{ .hello = .{ .data = "s-first" } }));
            const m = try c.recv(SmcState.hello, SmcMsg);
            switch (m) {
                .hello => |h| try std.testing.expectEqualStrings("c-first", h.data),
            }
        }

        fn clientLater(c: *const InMemoryChannel) !void {
            // 确保 server 先完成 send 再开始收：旧实现（两端共享单一
            // 信号量）下 server 会取回自己刚发的消息。
            try zio.sleep(zio.Duration.fromMilliseconds(100));
            const m = try c.recv(SmcState.hello, SmcMsg);
            switch (m) {
                .hello => |h| try std.testing.expectEqualStrings("s-first", h.data),
            }
            try c.send(SmcState.hello, SmcMsg, @as(SmcMsg, .{ .hello = .{ .data = "c-first" } }));
        }
    };

    var t1 = try rt.spawn(Side.serverFirst, .{&ch_s});
    var t2 = try rt.spawn(Side.clientLater, .{&ch_c});
    try t1.join();
    try t2.join();
}
