const std = @import("std");
const Io = std.Io;
const codec = @import("codec.zig");
const crypto = std.crypto;
const zio = @import("zio");
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
        const res = try codec.decode(&self.stream_reader.interface, state_id, T);
        return res;
    }
};

/// 进程内内存通道的单向“工作区”——等价于一个容量 1 的 MVar，
/// 用一对容量 1 的 channel 表达 MVar 的 full/empty 两态：
///
///  - `send_start`（发送许可，= empty）：init 预置 1 个 token，
///    send 取走一个、对端 recv 归还一个，保证该方向同一时刻
///    至多一条消息在途；
///  - `send_end`（数据就绪，= full）：send 写入数据后放置一个
///    token，对端 recv 取走。
///
/// HalfChannel 本身不提供 send/recv——由 `InMemoryChannel` 按方向引用：
/// 本端 `send` 写入自己的 `half_self`，对端 `recv` 从 `half_peer` 读取。
/// 所有权归调用方；配对的两个 HalfChannel 必须活得比引用它们的
/// InMemoryChannel 更久。
pub const HalfChannel = struct {
    /// send_start 的容量 1 缓冲（发送许可 token 槽位）。
    send_start_buf: [1]void = undefined,
    /// send_end 的容量 1 缓冲（数据就绪 token 槽位）。
    send_end_buf: [1]void = undefined,
    /// 发送许可信号量：init 预置 1 个 token，send 取走、对端 recv 归还。
    send_start: zio.Channel(void),
    /// 数据就绪信号量：send 写入数据后放置，对端 recv 取走。
    send_end: zio.Channel(void),

    /// send 编码后的消息字节数（≤ send_buff.len）。
    len: usize,
    /// 本端发送缓冲：本端 send 编码写入，对端 recv 从这里拷贝。
    send_buff: []u8,
    /// 本端接收缓冲：本端 recv 从对端 send_buff 拷贝后在此解码。
    recv_buff: []u8,

    pub fn init(
        self: *@This(),
        gpa: std.mem.Allocator,
        buff_size: usize,
    ) !void {
        const send_buff = try gpa.alloc(u8, buff_size);
        const recv_buff = try gpa.alloc(u8, buff_size);

        self.send_buff = send_buff;
        self.recv_buff = recv_buff;

        // send_start 用容量 1 的缓冲 channel 作“发送许可”信号量：
        // init 预置一个 token，send 取走一个，recv 归还一个，严格交替。
        self.send_start = .init(&self.send_start_buf);
        // send_end 用容量 1 的缓冲 channel 作“数据就绪”事件：
        // init 不预置 token——token 只能由 send 在真实写入数据后产生。
        // 缓冲使 send 不必阻塞等待 recv 取走；send_start 的许可仍保证
        // 同一时刻至多一条消息在途。
        self.send_end = .init(&self.send_end_buf);

        self.len = 0;

        try self.send_start.send({});
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.send_buff);
        gpa.free(self.recv_buff);
    }
};

/// 进程内内存通道的一端——不经过网络 I/O，全双工：两个配对的
/// InMemoryChannel（交叉引用两个 HalfChannel）组成 client↔server
/// 双向消息管道，每个方向至多一条消息在途，方向互不影响。
///
/// 方向由配对结构保证：`send` 只写入 `half_self` 的发送侧并在
/// `half_peer` 放置就绪信号，`recv` 只读取 `half_peer` 的发送侧——
/// 任一端永远不会收到自己发送的消息。
///
/// 所有权：`half_self`/`half_peer` 为借用，调用方负责两个 HalfChannel
/// 的分配与释放，且须保证它们活得比本通道久。
///
/// 失败语义：`send` 编码失败（消息超过缓冲大小 → error.WriteFailed）时
/// 发送许可已消耗且不归还，该方向通道报废，须重建；
/// `recv` 解码失败（IncorrectStatusReceived/MessageTooLarge）时许可已
/// 归还，通道可继续使用。
pub const InMemoryChannel = struct {
    /// 从线上解码单个 `[]const u8` 切片的上限，语义同 `StreamChannel`。
    max_slice_len: usize,
    /// 本端工作区：send 的写入目标，recv 的暂存/解码缓冲。
    half_self: *HalfChannel,
    /// 对端工作区：recv 的数据来源（对端 send 的产物）。
    half_peer: *HalfChannel,

    /// 发送一条消息：等发送许可 → 编码进 half_self.send_buff →
    /// 在 half_peer 放置数据就绪信号。
    pub fn send(self: *const @This(), state_id: anytype, _: type, val: anytype) !void {
        _ = try self.half_self.send_start.receive();
        var writer = Io.Writer.fixed(self.half_self.send_buff);
        try codec.encode(&writer, state_id, val);
        self.half_self.len = writer.buffered().len;
        try self.half_peer.send_end.send({});
    }

    /// 接收一条消息：等本端就绪信号 → 从 half_peer 拷贝已编码数据 →
    /// 归还对端发送许可 → 解码。
    pub fn recv(self: *const @This(), state_id: anytype, T: type) !T {
        _ = try self.half_self.send_end.receive();
        const len = self.half_peer.len;
        @memcpy(self.half_self.recv_buff[0..len], self.half_peer.send_buff[0..len]);
        try self.half_peer.send_start.send({});
        var reader = Io.Reader.fixed(self.half_self.recv_buff[0..len]);
        const res = try codec.decode(&reader, state_id, T);
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
///
/// 并发契约：
///  - 发送路径（send）与接收路径（recv/recordRead）状态完全独立
///    （各自的缓冲区与计数器），可在不同 fiber 上并发执行；
///  - 多个发送不得并发——`write_counter` 与编码缓冲由调用方串行化
///    （`symmetric_run` 单 fiber 天然满足）。
/// 密钥轮换配置。
///
/// 触发条件在 `sealAndSend` 入口检查（发送侧单 fiber，无竞争）：
///  - `record_threshold`：写记录数上限，防 AEAD 用量超限（数学安全硬性）；
///  - `interval_ns`：时间间隔，懒触发——只有有消息要发时才检查，
///    空闲连接不产生空轮换包。
/// 任一为 0 表示禁用对应触发。默认 10 分钟 + 2^28 条记录。
pub const RotationConfig = struct {
    interval_ns: u64 = 10 * std.time.ns_per_min,
    record_threshold: u64 = 1 << 28,
};

/// AEAD 记录类型，编码在 nonce 第 24 字节——nonce 是 AEAD 输入，
/// 因此类型字节被认证，攻击者无法篡改。
const RecordType = enum(u8) {
    data = 0,
    key_update = 1,
};

/// 从当前密钥单向派生下一代密钥（RFC 5869 HKDF-Expand 单次迭代）：
/// `new = HMAC-SHA256(key = current, "polyrole-key-rotate" || epoch || 0x01)`。
/// HKDF 单向性保证：旧密钥泄露推不出新密钥（前向保密保持）。
fn deriveRotationKey(current: [32]u8, epoch: u32) [32]u8 {
    var info: [24]u8 = undefined;
    @memcpy(info[0..19], "polyrole-key-rotate");
    std.mem.writeInt(u32, info[19..23], epoch, .big);
    info[23] = 0x01; // HKDF-Expand 单次迭代计数器
    var out: [32]u8 = undefined;
    crypto.auth.hmac.sha2.HmacSha256.create(&out, &info, &current);
    return out;
}

pub const TlsChannel = struct {
    /// 借用——调用方拥有 StreamChannel，必须在 TlsChannel 之后 deinit 它。
    inner: *StreamChannel,
    write_key: [32]u8,
    read_key: [32]u8,
    write_counter: u64,
    read_counter: u64,

    /// 本端写入方向的 epoch（发出的记录所属轮次），仅发送侧读写。
    write_epoch: u32 = 0,
    /// 对端写入方向的 epoch（预期收到的记录的轮次），仅接收侧读写。
    read_epoch: u32 = 0,
    /// 上次轮换时间，仅发送侧读写。
    last_rotation: u64,
    /// 轮换配置，init 后只读（可用 setRotationConfig 调整）。
    rotation: RotationConfig = .{},

    /// 加密前编码协议消息的缓冲区。
    /// 前 2 字节保留给被认证的长度前缀。
    encode_buf: []u8,
    /// 解码前存放解密明文的缓冲区
    decode_buf: []u8,
    /// 加密（seal）路径专用临时缓冲（tag || ciphertext）。
    /// 与 open_buf 分离，使发送与接收可并发执行。
    seal_buf: []u8,
    /// 解密（open）路径专用临时缓冲（tag || ciphertext）。
    /// 与 seal_buf 分离，使发送与接收可并发执行。
    open_buf: []u8,

    pub fn init(
        self: *@This(),
        gpa: std.mem.Allocator,
        inner: *StreamChannel,
        write_key: [32]u8,
        read_key: [32]u8,
        buf_size: usize,
    ) !void {
        std.debug.assert(buf_size >= 2);
        self.inner = inner;
        self.encode_buf = try gpa.alloc(u8, buf_size);
        errdefer gpa.free(self.encode_buf);
        self.decode_buf = try gpa.alloc(u8, buf_size);
        errdefer gpa.free(self.decode_buf);
        self.seal_buf = try gpa.alloc(u8, buf_size + 16);
        errdefer gpa.free(self.seal_buf);
        self.open_buf = try gpa.alloc(u8, buf_size + 16);
        self.write_key = write_key;
        self.read_key = read_key;
        self.write_counter = 0;
        self.read_counter = 0;
        self.write_epoch = 0;
        self.read_epoch = 0;
        self.last_rotation = zio.time.Timestamp.now(.realtime).toNanoseconds();
    }

    /// 覆盖密钥轮换配置（测试或特殊部署使用）。
    pub fn setRotationConfig(self: *@This(), config: RotationConfig) void {
        self.rotation = config;
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        gpa.free(self.encode_buf);
        gpa.free(self.decode_buf);
        gpa.free(self.seal_buf);
        gpa.free(self.open_buf);
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
        return try codec.decode(&reader, state_id, T);
    }

    /// 记录模式读取：读取一条 AEAD 记录并返回其明文（`[msg_len(2 BE) || msg]`，
    /// 单消息语义）。在下次读取本通道之前有效。
    pub fn recordRead(self: *@This()) ![]const u8 {
        const plain = try self.recordReadRaw();
        const msg_len = std.mem.readInt(u16, plain[0..2], .big);
        if (msg_len != plain.len - 2) return error.BadLength;
        return plain;
    }

    /// 读取并打开一条 AEAD 记录，返回完整明文，不做消息语义解释。
    /// 返回的切片在下次读取本通道之前有效。
    ///
    /// KeyUpdate 记录在此被吸收：按序校验并推进读密钥后继续读下一条，
    /// 上层永远只看到普通数据记录。
    pub fn recordReadRaw(self: *@This()) ![]const u8 {
        while (true) {
            const sr = &self.inner.stream_reader.interface;

            const nonce = (try sr.take(24))[0..24].*;

            // 先校验计数器：nonce 在明文中，可避免为无效 nonce 的记录浪费一次解密。
            const counter = std.mem.readInt(u64, nonce[0..8], .big);
            if (counter != self.read_counter) return error.ReplayDetected;
            if (self.read_counter == std.math.maxInt(u64)) return error.NonceExhausted;
            const typ: RecordType = @enumFromInt(nonce[23]);

            const tag = try sr.take(16);
            const ct_len = try sr.takeInt(u32, .big);
            if (ct_len < 2 or ct_len > self.decode_buf.len) return error.MessageTooLarge;

            const ct = try sr.take(ct_len);

            // AEAD 解密
            const combined = self.open_buf[0 .. ct_len + 16];
            @memcpy(combined[0..16], tag);
            @memcpy(combined[16..][0..ct_len], ct);
            crypto.nacl.SecretBox.open(
                self.decode_buf[0..ct_len],
                combined,
                nonce,
                self.read_key,
            ) catch return error.DecryptFailed;

            self.read_counter += 1;

            if (typ == .key_update) {
                // 明文 = [epoch u32 BE]。计数器检查已保证按序到达，
                // epoch 校验是第二道防线（防乱序/重放旧轮换）。
                const declared = std.mem.readInt(u32, self.decode_buf[0..4], .big);
                if (declared != self.read_epoch + 1) return error.KeyRotationOutOfOrder;
                self.read_key = deriveRotationKey(self.read_key, declared);
                self.read_epoch = declared;
                self.read_counter = 0;
                continue;
            }

            return self.decode_buf[0..ct_len];
        }
    }

    /// 原子性地推进计数器并发送一条 AEAD 记录。
    /// nonce 永不复用——即使后续 flush 失败且调用方重试。
    /// 线上格式：nonce(24) || tag(16) || ct_len(4 BE) || ciphertext(ct_len)。
    /// nonce = counter(8 BE) || 0(15) || type(1)。
    pub fn sealAndSend(self: *@This(), plaintext: []const u8) !void {
        try self.maybeRotate();
        try self.sealRecord(plaintext, .data);
    }

    /// 发送侧触发检查（懒触发：只在有消息要发时运行）。
    fn maybeRotate(self: *@This()) !void {
        const rot = self.rotation;
        if (rot.record_threshold != 0 and self.write_counter >= rot.record_threshold) {
            try self.sendKeyUpdate();
            return;
        }
        if (rot.interval_ns != 0) {
            const now = zio.time.Timestamp.now(.realtime).toNanoseconds();
            if (now -| self.last_rotation >= rot.interval_ns) {
                try self.sendKeyUpdate();
            }
        }
    }

    /// 发起一次密钥轮换：用当前写密钥发 KeyUpdate（明文 = 新 epoch），
    /// 随后本地推进写密钥并重置写计数器。切密钥与计数器归零原子绑定，
    /// 保证 nonce 不重用。
    fn sendKeyUpdate(self: *@This()) !void {
        const new_epoch = self.write_epoch + 1;
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, new_epoch, .big);
        try self.sealRecord(&buf, .key_update);
        self.write_key = deriveRotationKey(self.write_key, new_epoch);
        self.write_epoch = new_epoch;
        self.write_counter = 0;
        self.last_rotation = zio.time.Timestamp.now(.realtime).toNanoseconds();
    }

    fn sealRecord(self: *@This(), plaintext: []const u8, typ: RecordType) !void {
        const this_counter = self.write_counter;
        if (this_counter == std.math.maxInt(u64)) return error.NonceExhausted;
        self.write_counter += 1;

        // 从已提交的计数器构造 nonce；type 编码在 nonce 末字节（被 AEAD 认证）
        var nonce: [24]u8 = [_]u8{0} ** 24;
        std.mem.writeInt(u64, nonce[0..8], this_counter, .big);
        nonce[23] = @intFromEnum(typ);

        // AEAD 加密
        const combined = self.seal_buf[0 .. plaintext.len + 16];
        crypto.nacl.SecretBox.seal(combined, plaintext, nonce, self.write_key);
        const ct = combined[16..][0..plaintext.len];

        // 线上格式：nonce || tag || ct_len || ciphertext
        const sw = &self.inner.stream_writer.interface;
        try sw.writeAll(&nonce);
        try sw.writeAll(combined[0..16]); // 标签
        try sw.writeInt(u32, @intCast(ct.len), .big);
        try sw.writeAll(ct);
        try sw.flush();
    }
};

// ─────────────────── 记录层错误路径测试 ───────────────────

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
