// 多人聊天室 demo —— 协议定义
//
// 两个协议跑在同一条 TCP 连接的 MultiplexChannel 上（见 docs/family.md）：
//   - Ctrl（子通道 0）：锁步控制协议 —— 注册昵称、发送消息、心跳、退出
//   - Push（子通道 1）：服务器 → 客户端真推送 —— 聊天消息与系统通知
//
// Ctrl 状态机：
//   Login(client) ─Register{nickname}─▶ Welcome(server) ─Welcome{id,members}─▶ Send(client)
//   Send(client) ─Msg{poll}/Quit─▶ Ack(server) ─Ack(void)─▶ Send(client)
//   Send.quit ─▶ Exit（两端同时终止）
//
// Push 状态机：
//   Deliver(server) ─Push{...}─▶ Deliver(server)（自环）
//
// 设计原则（与 src/protocol 下的协议一致）：
//   - 协议只建模正确流；超时、重连、断线清理都是调用方（server.zig / client.zig）的事。
//   - 载荷全部定长，codec 原生支持，wire 路径零分配。
//   - 房间成员表由 Room fiber 独占访问（Channel 串行化），不依赖线程模型。

const std = @import("std");
const zio = @import("zio");
const polyrole = @import("polyrole_cs");

pub const Data = polyrole.Data;
pub const ProtocolInfo = polyrole.ProtocolInfo;
pub const Exit = polyrole.Exit;
pub const Channel = zio.Channel;
pub const Allocator = std.mem.Allocator;

// ─── 常量 ────────────────────────────────────────────────────────────

pub const MAX_MEMBERS = 64;
pub const MAX_NICK = 32;
pub const MAX_TEXT = 256;
pub const INVALID_CLIENT_ID: u32 = std.math.maxInt(u32);
/// 客户端空转时的轮询间隔（毫秒）。既是消息发送延迟上限，也是心跳周期。
pub const POLL_INTERVAL_MS: u64 = 100;

// ─── 载荷 ────────────────────────────────────────────────────────────

pub const RegisterPayload = struct {
    nickname: [MAX_NICK]u8,
};

pub const WelcomePayload = struct {
    client_id: u32,
    member_count: u8,
    members: [MAX_MEMBERS][MAX_NICK]u8,
};

pub const MsgPayload = struct {
    seq: u64,
    text: [MAX_TEXT]u8,
};

pub const PushPayload = struct {
    /// 0 = 聊天消息，1 = 系统通知（加入/离开）
    kind: u8,
    /// 客户端消息序号原样透传；系统通知为 0
    seq: u64,
    /// 发送者 client_id；系统通知为 0
    from_id: u32,
    from_name: [MAX_NICK]u8,
    text: [MAX_TEXT]u8,
    /// 服务器收到消息的单调时钟毫秒（仅作显示参考，跨机不可比）
    ts_ms: u64,
};

pub const PushKind = enum(u8) {
    chat = 0,
    system = 1,
};

// ─── 上下文 ──────────────────────────────────────────────────────────

/// stdin 线程/fiber 投递的用户输入。
pub const UserInput = union(enum) {
    msg: [MAX_TEXT]u8,
    quit,
};

/// 控制协议客户端上下文。
pub const ClientContext = struct {
    /// 输出目标（stdout 或 null 关闭打印，测试用）
    out: ?zio.File,
    /// 输出串行化锁：Ctrl 与 Push 两个 fiber 并发写 stdout 会交错
    out_lock: ?*zio.Mutex = null,
    nickname: [MAX_NICK]u8,
    /// 用户输入队列，Send.process 每轮 tryReceive
    input: *Channel(UserInput),
    seq: u64 = 0,
    client_id: u32 = INVALID_CLIENT_ID,
    member_count: u8 = 0,
    members: [MAX_MEMBERS][MAX_NICK]u8 = [_][MAX_NICK]u8{[_]u8{0} ** MAX_NICK} ** MAX_MEMBERS,

    pub fn init(out: ?zio.File, nickname: [MAX_NICK]u8, input: *Channel(UserInput)) ClientContext {
        return .{ .out = out, .nickname = nickname, .input = input };
    }
};

/// 控制协议服务器上下文（每个连接一个）。
pub const ServerContext = struct {
    room: *Room,
    /// 本连接的广播队列，与 Push 协议共享
    inbox: *Channel(PushPayload),
    client_id: u32 = INVALID_CLIENT_ID,
    nickname: [MAX_NICK]u8 = [_]u8{0} ** MAX_NICK,
    member_count: u8 = 0,
    members: [MAX_MEMBERS][MAX_NICK]u8 = [_][MAX_NICK]u8{[_]u8{0} ** MAX_NICK} ** MAX_MEMBERS,

    pub fn init(room: *Room, inbox: *Channel(PushPayload)) ServerContext {
        return .{ .room = room, .inbox = inbox };
    }

    /// 从房间移除本连接并广播离开通知。幂等：未注册或已移除时无操作。
    pub fn leave(self: *ServerContext) !void {
        if (self.client_id == INVALID_CLIENT_ID) return;
        var reply_buf: [1]void = undefined;
        var reply: Channel(void) = .init(&reply_buf);
        try self.room.ops.send(.{ .remove = .{ .client_id = self.client_id, .reply = &reply } });
        try reply.receive();
        self.client_id = INVALID_CLIENT_ID;
    }
};

/// 推送协议客户端上下文。
pub const PushClientContext = struct {
    /// 输出目标（demo 客户端为 stdout）；设置了 `inbox` 时不打印
    out: ?zio.File = null,
    /// 输出串行化锁（与 ClientContext 共享同一把）
    out_lock: ?*zio.Mutex = null,
    /// 收到的推送（测试注入，用于断言；demo 客户端为 null）
    inbox: ?*Channel(PushPayload) = null,
};

/// 推送协议服务器上下文（每个连接一个）。
pub const PushServerContext = struct {
    inbox: *Channel(PushPayload),
};

// ─── Room：房间成员表 ────────────────────────────────────────────────

/// 成员操作。所有操作经 ops 队列由 Room fiber 串行处理（单写者）。
pub const RegisterOp = struct {
    nickname: [MAX_NICK]u8,
    inbox: *Channel(PushPayload),
    reply: *Channel(WelcomePayload),
};

pub const RemoveOp = struct {
    client_id: u32,
    reply: *Channel(void),
};

pub const BroadcastOp = struct {
    from_id: u32,
    payload: PushPayload,
};

pub const RoomOp = union(enum) {
    register: RegisterOp,
    remove: RemoveOp,
    broadcast: BroadcastOp,
};

pub const Room = struct {
    slots: [MAX_MEMBERS]Slot,
    count: u8 = 0,
    ops: Channel(RoomOp) = undefined,
    ops_buf: [64]RoomOp = undefined,
    fiber: ?zio.JoinHandle(anyerror!void) = null,

    pub const Slot = struct {
        active: bool = false,
        nickname: [MAX_NICK]u8 = [_]u8{0} ** MAX_NICK,
        inbox: ?*Channel(PushPayload) = null,
    };

    pub fn init(self: *Room) void {
        self.slots = [_]Slot{.{}} ** MAX_MEMBERS;
        self.count = 0;
        self.ops = Channel(RoomOp).init(&self.ops_buf);
        self.fiber = null;
    }

    /// 启动 Room 后台 fiber：串行处理所有成员操作。
    pub fn spawn(self: *Room) !void {
        self.fiber = try zio.spawn(Room.run, .{self});
    }

    /// 关闭 ops 队列并等待 Room fiber 退出（测试清理用）。
    pub fn stop(self: *Room) void {
        self.ops.close(.immediate);
        if (self.fiber) |f| {
            var h = f;
            h.join() catch {};
            self.fiber = null;
        }
    }

    fn run(self: *Room) anyerror!void {
        while (true) {
            const op = self.ops.receive() catch return; // ChannelClosed → 退出
            self.handle(op);
        }
    }

    /// 同步排空 ops 队列（单测用；网络模式下由 run() 驱动）。
    pub fn drain(self: *Room) void {
        while (self.ops.tryReceive()) |op| self.handle(op) else |_| {}
    }

    fn handle(self: *Room, op: RoomOp) void {
        switch (op) {
            .register => |r| self.handleRegister(r),
            .remove => |r| self.handleRemove(r),
            .broadcast => |b| self.handleBroadcast(b),
        }
    }

    fn handleRegister(self: *Room, r: RegisterOp) void {
        if (self.count >= MAX_MEMBERS) {
            r.reply.trySend(.{ .client_id = INVALID_CLIENT_ID, .member_count = 0, .members = undefined }) catch {};
            return;
        }
        const id = self.allocSlot() orelse {
            r.reply.trySend(.{ .client_id = INVALID_CLIENT_ID, .member_count = 0, .members = undefined }) catch {};
            return;
        };
        self.slots[id] = .{ .active = true, .nickname = r.nickname, .inbox = r.inbox };
        self.count += 1;

        // 成员快照（排除自己，供 Welcome 发送）
        var welcome = WelcomePayload{
            .client_id = @intCast(id),
            .member_count = 0,
            .members = [_][MAX_NICK]u8{[_]u8{0} ** MAX_NICK} ** MAX_MEMBERS,
        };
        for (&self.slots, 0..) |*s, i| {
            if (s.active and i != id) {
                welcome.members[welcome.member_count] = s.nickname;
                welcome.member_count += 1;
            }
        }
        r.reply.trySend(welcome) catch {};

        self.notifyAllExcept(@intCast(id), r.nickname, "加入了房间");
    }

    fn handleRemove(self: *Room, r: RemoveOp) void {
        if (r.client_id < MAX_MEMBERS) {
            const s = &self.slots[r.client_id];
            if (s.active) {
                const name = s.nickname;
                s.active = false;
                s.inbox = null;
                self.count -= 1;
                self.notifyAllExcept(r.client_id, name, "离开了房间");
            }
        }
        r.reply.trySend({}) catch {};
    }

    fn handleBroadcast(self: *Room, b: BroadcastOp) void {
        for (&self.slots, 0..) |*s, i| {
            if (!s.active or i == b.from_id) continue;
            const inbox = s.inbox orelse continue;
            inbox.trySend(b.payload) catch {
                // 慢消费者：队列已满，fail-fast —— 关闭其 inbox，
                // Push fiber 收到 ChannelClosed 退出，监督 fiber 收拾连接。
                const name = s.nickname;
                s.active = false;
                s.inbox = null;
                self.count -= 1;
                inbox.close(.graceful);
                self.notifyAllExcept(@intCast(i), name, "因接收过慢被断开");
            };
        }
    }

    fn allocSlot(self: *Room) ?usize {
        for (&self.slots, 0..) |*s, i| {
            if (!s.active) return i;
        }
        return null;
    }

    /// 向除 except_id 外的所有在线成员广播系统通知。
    fn notifyAllExcept(self: *Room, except_id: u32, name: [MAX_NICK]u8, verb: []const u8) void {
        var text_buf: [MAX_TEXT]u8 = undefined;
        const text = std.fmt.bufPrint(&text_buf, "{s} {s}", .{ name[0..cstrLen(&name)], verb }) catch return;
        var payload = PushPayload{
            .kind = @intFromEnum(PushKind.system),
            .seq = 0,
            .from_id = 0,
            .from_name = name,
            .text = [_]u8{0} ** MAX_TEXT,
            .ts_ms = monotonicMs(),
        };
        @memcpy(payload.text[0..text.len], text);
        for (&self.slots, 0..) |*s, i| {
            if (!s.active or i == except_id) continue;
            const inbox = s.inbox orelse continue;
            inbox.trySend(payload) catch {
                s.active = false;
                s.inbox = null;
                self.count -= 1;
                inbox.close(.graceful);
            };
        }
    }
};

// ─── 控制协议状态机 ──────────────────────────────────────────────────

const CtrlInfo = ProtocolInfo("chat_ctrl", ClientContext, ServerContext);

pub const Login = union(enum) {
    register: Data(RegisterPayload, Welcome),

    pub const info: CtrlInfo = .{ .agent = .client, .name = "Login" };

    pub fn process(ctx: *ClientContext) @This() {
        return .{ .register = .{ .data = .{ .nickname = ctx.nickname } } };
    }

    /// 服务器端：向 Room 注册，分配 client_id，存成员快照。
    pub fn preprocess(ctx: *ServerContext, result: @This()) !void {
        const nick = result.register.data.nickname;
        ctx.nickname = nick;

        var reply_buf: [1]WelcomePayload = undefined;
        var reply: Channel(WelcomePayload) = .init(&reply_buf);
        try ctx.room.ops.send(.{ .register = .{ .nickname = nick, .inbox = ctx.inbox, .reply = &reply } });
        const reg = try reply.receive();
        if (reg.client_id == INVALID_CLIENT_ID) return error.RoomFull;
        ctx.client_id = reg.client_id;
        ctx.member_count = reg.member_count;
        ctx.members = reg.members;
    }
};

pub const Welcome = union(enum) {
    to_client: Data(WelcomePayload, Send),

    pub const info: CtrlInfo = .{ .agent = .server, .name = "Welcome" };

    pub fn process(ctx: *ServerContext) @This() {
        return .{ .to_client = .{ .data = .{
            .client_id = ctx.client_id,
            .member_count = ctx.member_count,
            .members = ctx.members,
        } } };
    }

    /// 客户端：保存自己的 ID 与成员快照，打印欢迎语。
    pub fn preprocess(ctx: *ClientContext, result: @This()) !void {
        const w = result.to_client.data;
        ctx.client_id = w.client_id;
        ctx.member_count = w.member_count;
        ctx.members = w.members;
        if (ctx.out) |f| {
            var buf: [MAX_TEXT * 2]u8 = undefined;
            try writeLineLocked(f, ctx.out_lock, welcomeText(w, &ctx.nickname, &buf));
        }
    }
};

pub const Send = union(enum) {
    msg: Data(MsgPayload, Ack),
    poll: Data(void, Ack),
    quit: Data(void, Exit),

    pub const info: CtrlInfo = .{ .agent = .client, .name = "Send" };

    /// 客户端：取一条用户输入（消息/退出）；无输入则空转一个间隔后发心跳。
    pub fn process(ctx: *ClientContext) !@This() {
        const input = ctx.input.tryReceive() catch |err| switch (err) {
            error.ChannelEmpty => null,
            else => return err,
        };
        if (input) |i| switch (i) {
            .quit => return .quit,
            .msg => |text| {
                ctx.seq += 1;
                return .{ .msg = .{ .data = .{ .seq = ctx.seq, .text = text } } };
            },
        };
        try sleepMs(POLL_INTERVAL_MS);
        return .poll;
    }

    /// 服务器端：聊天消息广播给房间，退出则移除并广播离开。
    pub fn preprocess(ctx: *ServerContext, result: @This()) !void {
        switch (result) {
            .msg => |m| {
                const payload = PushPayload{
                    .kind = @intFromEnum(PushKind.chat),
                    .seq = m.data.seq,
                    .from_id = ctx.client_id,
                    .from_name = ctx.nickname,
                    .text = m.data.text,
                    .ts_ms = monotonicMs(),
                };
                try ctx.room.ops.send(.{ .broadcast = .{ .from_id = ctx.client_id, .payload = payload } });
            },
            .poll => {},
            .quit => try ctx.leave(),
        }
    }
};

pub const Ack = union(enum) {
    ack: Data(void, Send),

    pub const info: CtrlInfo = .{ .agent = .server, .name = "Ack" };

    pub fn process(ctx: *ServerContext) @This() {
        _ = ctx;
        return .ack;
    }
};

// ─── 推送协议状态机 ──────────────────────────────────────────────────

const PushInfo = ProtocolInfo("chat_push", PushClientContext, PushServerContext);

pub const Deliver = union(enum) {
    push: Data(PushPayload, Deliver),

    pub const info: PushInfo = .{ .agent = .server, .name = "Deliver" };

    /// 服务器端：从本连接的广播队列取一条消息推送；队列关闭则退出。
    pub fn process(ctx: *PushServerContext) !@This() {
        const payload = try ctx.inbox.receive();
        return .{ .push = .{ .data = payload } };
    }

    /// 客户端：投递到收件箱（测试）或格式化打印（demo）。
    pub fn preprocess(ctx: *PushClientContext, result: @This()) !void {
        const p = result.push.data;
        if (ctx.inbox) |q| {
            try q.send(p); // 阻塞：消费慢时背压到服务器
            return;
        }
        if (ctx.out) |f| {
            var buf: [MAX_TEXT + MAX_NICK + 8]u8 = undefined;
            const line = switch (p.kind) {
                0 => std.fmt.bufPrint(&buf, "[{s}] {s}", .{
                    p.from_name[0..cstrLen(&p.from_name)],
                    p.text[0..cstrLen(&p.text)],
                }) catch return,
                1 => std.fmt.bufPrint(&buf, "*** {s}", .{p.text[0..cstrLen(&p.text)]}) catch return,
                else => return,
            };
            try writeLineLocked(f, ctx.out_lock, line);
        }
    }
};

// ─── 工具 ────────────────────────────────────────────────────────────

/// 返回当前单调时钟的毫秒时间戳。
pub fn monotonicMs() u64 {
    const ts = zio.Timestamp.now(.monotonic);
    return @intCast(ts.toNanoseconds() / 1_000_000);
}

/// 在单调时钟上休眠 `ms` 毫秒。
pub fn sleepMs(ms: u64) !void {
    try zio.sleep(zio.Duration.fromMilliseconds(@intCast(ms)));
}

/// 定长字节数组的 C 风格字符串长度（第一个 0 之前）。
pub fn cstrLen(buf: []const u8) usize {
    return std.mem.indexOfScalar(u8, buf, 0) orelse buf.len;
}

/// 向 `out` 写入一行（含换行）。
///
/// 强制 streaming 模式：stdout 重定向到文件时 zio 会按 seekable 判定为
/// positional（pwrite offset 从 0 起），导致每行都覆盖文件开头。
pub fn writeLine(out: zio.File, line: []const u8) !void {
    var buf: [2048]u8 = undefined;
    var w = out.writer(&buf);
    w.mode = .streaming;
    try w.interface.writeAll(line);
    try w.interface.writeAll("\n");
    try w.interface.flush();
}

/// 带锁版本：客户端 Ctrl 与 Push 两个 fiber 并发写 stdout 会交错，
/// demo 客户端把同一把 Mutex 传给两个上下文。
pub fn writeLineLocked(out: zio.File, lock: ?*zio.Mutex, line: []const u8) !void {
    if (lock) |mu| {
        try mu.lock();
        defer mu.unlock();
    }
    try writeLine(out, line);
}

/// 构造欢迎语，写入 `buf`。
fn welcomeText(w: WelcomePayload, own_nick: *const [MAX_NICK]u8, buf: []u8) []const u8 {
    var pos: usize = 0;
    pos += (std.fmt.bufPrint(buf[pos..], "== 欢迎，{s}！你的 ID 是 {d}。在线成员：", .{
        own_nick[0..cstrLen(own_nick)],
        w.client_id,
    }) catch return buf[0..pos]).len;
    if (w.member_count == 0) {
        pos += (std.fmt.bufPrint(buf[pos..], "（无）", .{}) catch return buf[0..pos]).len;
    } else {
        for (0..w.member_count) |i| {
            const name = w.members[i][0..cstrLen(&w.members[i])];
            const sep = if (i > 0) ", " else "";
            pos += (std.fmt.bufPrint(buf[pos..], "{s}{s}", .{ sep, name }) catch return buf[0..pos]).len;
        }
    }
    return buf[0..pos];
}
