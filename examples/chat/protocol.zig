// 多人聊天室 demo —— 协议定义
//
// 两个协议跑在同一条 TCP 连接的 MultiplexChannel 上（见 docs/family.md）：
//   - Ctrl（子通道 0）：锁步控制协议 —— 注册昵称、发送消息、心跳、退出
//   - Push（子通道 1）：服务器 → 客户端真推送 —— 聊天消息与系统通知
//
// Ctrl 状态机：
//   Login(client) ─Register{nickname}─▶ Welcome(server) ─Welcome{id,members}─▶ Send(client)
//   Send(client) ─Msg{heartbeat}/Quit─▶ Ack(server) ─Ack(void)─▶ Send(client)
//   Send.quit ─▶ Exit（两端同时终止）
//
// Push 状态机（SharedBoard 游标拉取）：
//   Poll(server) ─batch{8条}/idle─▶ Poll；kick ─▶ Exit
//   服务器端按游标从共享消息板批量拉取推送；无新消息时发 idle 帧。
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

/// 成员表容量动态（无上限），见 docs/chat-scale-10000.md §3.1。
pub const MAX_NICK = 32;
pub const MAX_TEXT = 256;
/// 单次 /who 响应的成员名单上限（截断显示，避免超大帧）。
pub const WHO_LIST_LIMIT = 32;
pub const INVALID_CLIENT_ID: u32 = std.math.maxInt(u32);
/// 心跳间隔（毫秒）：客户端空闲时发心跳的周期，也是服务器掉线检测的依据。
pub const HEARTBEAT_INTERVAL_MS: u64 = 1000;
/// Send.process 检查输入队列的节拍（毫秒），决定消息发送延迟上界。
pub const PROCESS_SLICE_MS: u64 = 100;
/// Push 协议每次批量拉取的消息条数（每帧）。
pub const PUSH_CHUNK = 8;
/// Push 协议空转轮询间隔（毫秒）：无新消息时服务器发 idle 帧的周期，
/// 也是推送延迟上界。
pub const PUSH_POLL_MS: u64 = 100;
/// 消息板水位：新连接只拉取最近 BOARD_KEEP 条历史。
pub const BOARD_KEEP: usize = 1024;

// ─── 载荷 ────────────────────────────────────────────────────────────

pub const RegisterPayload = struct {
    nickname: [MAX_NICK]u8,
};

/// Welcome 只带身份与在线人数；成员列表按需获取（/who），见 §3.2。
pub const WelcomePayload = struct {
    client_id: u32,
    member_count: u32,
};

pub const MsgPayload = struct {
    seq: u64,
    text: [MAX_TEXT]u8,
};

pub const PushPayload = struct {
    /// 0 = 聊天消息，1 = 系统通知（加入/离开），2 = /who 成员列表响应
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
    member_list = 2,
};

// ─── 上下文 ──────────────────────────────────────────────────────────

/// stdin 线程/fiber 投递的用户输入。
pub const UserInput = union(enum) {
    msg: [MAX_TEXT]u8,
    /// 请求在线成员列表（服务器经 Push 通道回 member_list 响应）
    who,
    quit,
};

/// 控制协议客户端上下文。
pub const ClientContext = struct {
    /// 输出目标（stdout 或 null 关闭打印，测试用）
    out: ?zio.File,
    /// 输出串行化锁：Ctrl 与 Push 两个 fiber 并发写 stdout 会交错
    out_lock: ?*zio.Mutex = null,
    nickname: [MAX_NICK]u8,
    /// 用户输入队列，Send.process 每 PROCESS_SLICE_MS 轮询
    input: *Channel(UserInput),
    seq: u64 = 0,
    client_id: u32 = INVALID_CLIENT_ID,
    member_count: u32 = 0,

    pub fn init(out: ?zio.File, nickname: [MAX_NICK]u8, input: *Channel(UserInput)) ClientContext {
        return .{ .out = out, .nickname = nickname, .input = input };
    }
};

/// 控制协议服务器上下文（每个连接一个）。
pub const ServerContext = struct {
    room: *Room,
    client_id: u32 = INVALID_CLIENT_ID,
    nickname: [MAX_NICK]u8 = [_]u8{0} ** MAX_NICK,
    member_count: u32 = 0,

    pub fn init(room: *Room) ServerContext {
        return .{ .room = room };
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
    /// inbox 是否接收全部推送（含 system 加入/离开通知）。
    /// 压测客户端设 false 只收 chat 消息（压测关心 chat 广播，
    /// 不关心加入通知；真实客户端用 out 打印全部）。
    inbox_all: bool = true,
};

/// 推送协议服务器上下文（每个连接一个）。
pub const PushServerContext = struct {
    /// 共享消息板（跨连接）
    board: *SharedBoard,
    /// 本连接已消费到的板索引（游标）
    cursor: usize,
    /// Ctrl 连接结束时置位，让 Poll 走 .quit 退出
    kick: bool = false,
};

// ─── SharedBoard：共享消息板 ─────────────────────────────────────────

/// 跨连接共享的消息板（借鉴早期 chat 版本的"无锁读/有锁写"设计）：
/// 广播 = 一次 append（O(1)），每个连接的 Push 协议按自己的游标批量拉取，
/// 消除逐连接 trySend 的 O(N) 广播与加入通知 O(N²) 风暴，慢消费者只是
/// 游标落后而不会被断开。
///
/// 并发契约：
///   - 预分配（ensureTotalCapacity）后 items 永不搬迁 → Reader 无锁读安全
///   - Writer 唯一（Room fiber）：lock → appendAssumeCapacity → committed.store(.release)
///   - Reader（各连接的 Push fiber）：committed.load(.acquire) → 直接读 items[from..to)
///   - release/acquire 保证 Reader 看到 committed 新值时必然看到数据写入
///   - 容量固定（appendAssumeCapacity 超出即 panic）：消息只增不减，需按
///     BOARD_KEEP 水位控制新连接的历史起点；物理回收留作后续（与无锁读冲突）
pub const SharedBoard = struct {
    items: std.ArrayList(PushPayload),
    mu: zio.Mutex,
    committed: std.atomic.Value(usize),

    pub fn init(self: *SharedBoard, gpa: Allocator, capacity: usize) void {
        self.items = std.ArrayList(PushPayload).empty;
        self.items.ensureTotalCapacity(gpa, capacity) catch @panic("board oom");
        self.mu = .{};
        self.committed = std.atomic.Value(usize).init(0);
    }

    pub fn deinit(self: *SharedBoard, gpa: Allocator) void {
        self.items.deinit(gpa);
    }

    /// 广播：追加一条消息（O(1)，唯一写者持锁）。
    pub fn append(self: *SharedBoard, payload: PushPayload) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        self.items.appendAssumeCapacity(payload);
        self.committed.store(self.items.items.len, .release);
    }

    /// 已追加的消息总数。
    pub fn len(self: *SharedBoard) usize {
        return self.committed.load(.acquire);
    }

    /// 水位：新连接的历史起点（保留最近 BOARD_KEEP 条）。
    pub fn watermark(self: *SharedBoard) usize {
        const c = self.committed.load(.acquire);
        return if (c > BOARD_KEEP) c - BOARD_KEEP else 0;
    }

    /// 无锁读 [from..to)。调用方保证 to <= len()。
    pub fn slice(self: *SharedBoard, from: usize, to: usize) []const PushPayload {
        return self.items.items[from..to];
    }
};

// ─── Room：房间成员表 ────────────────────────────────────────────────

/// 成员操作。所有操作经 ops 队列由 Room fiber 串行处理（单写者）。
pub const RegisterOp = struct {
    nickname: [MAX_NICK]u8,
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

/// /who 响应的成员名单（截断到 WHO_LIST_LIMIT）。
pub const MemberListReply = struct {
    count: u32,
    /// 实际填入的名字数量（<= WHO_LIST_LIMIT）
    name_count: u32,
    names: [WHO_LIST_LIMIT][MAX_NICK]u8,
    truncated: bool,
};

pub const WhoOp = struct {
    client_id: u32,
    reply: *Channel(MemberListReply),
};

pub const RoomOp = union(enum) {
    register: RegisterOp,
    remove: RemoveOp,
    broadcast: BroadcastOp,
    who: WhoOp,
};

/// 房间成员表：动态扩容 + 空闲槽位复用（见 docs/chat-scale-10000.md §3.1）。
/// 槽位索引即 client_id；删除的槽位进 free 栈，注册时 O(1) 复用。
/// 所有广播（聊天消息、加入/离开通知）经 SharedBoard.append 一次性写入
/// （O(1)），各连接的 Push 协议按游标拉取——无逐连接 trySend。
pub const Room = struct {
    allocator: Allocator,
    slots: std.ArrayList(Slot),
    free: std.ArrayList(u32),
    count: usize = 0,
    board: *SharedBoard,
    /// 加入/离开通知开关（大群或压测可关闭，避免注册风暴刷板）
    notify_joins: bool = true,
    /// ops 队列容量按连接规模配置（1024 足够万级连接的注册/广播排队）
    ops: Channel(RoomOp) = undefined,
    ops_buf: [1024]RoomOp = undefined,
    fiber: ?zio.JoinHandle(anyerror!void) = null,

    pub const Slot = struct {
        active: bool = false,
        nickname: [MAX_NICK]u8 = [_]u8{0} ** MAX_NICK,
    };

    pub fn init(self: *Room, allocator: Allocator, board: *SharedBoard) void {
        self.allocator = allocator;
        self.slots = std.ArrayList(Slot).empty;
        self.free = std.ArrayList(u32).empty;
        self.count = 0;
        self.board = board;
        // 注意：`var x: Room = undefined` 初始化不应用字段默认值，
        // init 必须显式设置（否则 notify_joins 是随机内存）
        self.notify_joins = true;
        self.ops = Channel(RoomOp).init(&self.ops_buf);
        self.fiber = null;
    }

    pub fn deinit(self: *Room) void {
        self.slots.deinit(self.allocator);
        self.free.deinit(self.allocator);
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
            .who => |w| self.handleWho(w),
        }
    }

    fn handleRegister(self: *Room, r: RegisterOp) void {
        if (self.free.items.len > 0) {
            const id: u32 = self.free.pop().?;
            const s = &self.slots.items[@intCast(id)];
            s.active = true;
            s.nickname = r.nickname;
            self.count += 1;
            self.finishRegister(r, id);
        } else {
            self.slots.append(self.allocator, .{ .active = true, .nickname = r.nickname }) catch {
                r.reply.trySend(.{ .client_id = INVALID_CLIENT_ID, .member_count = 0 }) catch {};
                return;
            };
            self.count += 1;
            self.finishRegister(r, @intCast(self.slots.items.len - 1));
        }
    }

    fn finishRegister(self: *Room, r: RegisterOp, id: u32) void {
        // 在线人数含自己，成员快照排除自己 → count - 1
        r.reply.trySend(.{ .client_id = id, .member_count = @intCast(self.count - 1) }) catch {};
        if (self.notify_joins) self.appendNotice(r.nickname, "加入了房间");
    }

    fn handleRemove(self: *Room, r: RemoveOp) void {
        if (r.client_id < self.slots.items.len) {
            const s = &self.slots.items[r.client_id];
            if (s.active) {
                const name = s.nickname;
                s.active = false;
                self.count -= 1;
                self.free.append(self.allocator, r.client_id) catch {}; // OOM 时槽位泄漏，可接受
                if (self.notify_joins) self.appendNotice(name, "离开了房间");
            }
        }
        r.reply.trySend({}) catch {};
    }

    /// 广播聊天消息：一次 append 到共享板（O(1)），各连接 Push 按游标拉取。
    fn handleBroadcast(self: *Room, b: BroadcastOp) void {
        self.board.append(b.payload);
    }

    /// /who：收集成员名单（截断到 WHO_LIST_LIMIT），Room fiber 独占访问成员表。
    fn handleWho(self: *Room, w: WhoOp) void {
        var reply = MemberListReply{
            .count = @intCast(self.count),
            .name_count = 0,
            .truncated = false,
            .names = undefined,
        };
        for (self.slots.items, 0..) |*s, i| {
            if (!s.active) continue;
            if (i == w.client_id) continue; // 不含自己
            if (reply.name_count < WHO_LIST_LIMIT) {
                reply.names[reply.name_count] = s.nickname;
                reply.name_count += 1;
            }
        }
        reply.truncated = reply.name_count < (self.count - 1);
        w.reply.trySend(reply) catch {};
    }

    /// 向消息板追加一条系统通知（加入/离开）。
    fn appendNotice(self: *Room, name: [MAX_NICK]u8, verb: []const u8) void {
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
        self.board.append(payload);
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

    /// 服务器端：向 Room 注册，分配 client_id，存在线人数。
    pub fn preprocess(ctx: *ServerContext, result: @This()) !void {
        const nick = result.register.data.nickname;
        ctx.nickname = nick;

        var reply_buf: [1]WelcomePayload = undefined;
        var reply: Channel(WelcomePayload) = .init(&reply_buf);
        try ctx.room.ops.send(.{ .register = .{ .nickname = nick, .reply = &reply } });
        const reg = try reply.receive();
        if (reg.client_id == INVALID_CLIENT_ID) return error.RoomFull;
        ctx.client_id = reg.client_id;
        ctx.member_count = reg.member_count;
    }
};

pub const Welcome = union(enum) {
    to_client: Data(WelcomePayload, Send),

    pub const info: CtrlInfo = .{ .agent = .server, .name = "Welcome" };

    pub fn process(ctx: *ServerContext) @This() {
        return .{ .to_client = .{ .data = .{
            .client_id = ctx.client_id,
            .member_count = ctx.member_count,
        } } };
    }

    /// 客户端：保存自己的 ID 与在线人数，打印欢迎语。
    pub fn preprocess(ctx: *ClientContext, result: @This()) !void {
        const w = result.to_client.data;
        ctx.client_id = w.client_id;
        ctx.member_count = w.member_count;
        if (ctx.out) |f| {
            var buf: [MAX_TEXT * 2]u8 = undefined;
            try writeLineLocked(f, ctx.out_lock, welcomeText(w, &ctx.nickname, &buf));
        }
    }
};

pub const Send = union(enum) {
    msg: Data(MsgPayload, Ack),
    heartbeat: Data(void, Ack),
    who: Data(void, Ack),
    quit: Data(void, Exit),

    pub const info: CtrlInfo = .{ .agent = .client, .name = "Send" };

    /// 客户端：每 100ms 检查一次用户输入（消息到达延迟 ≤100ms）；
    /// 累计 HEARTBEAT_INTERVAL_MS 无输入则发心跳（liveness + 锁步填充）。
    /// 不用 select 等 Channel：zio 的 select+asyncReceive 对 timer 分支不消费
    /// 队列值（tick 永在），会导致心跳忙循环。
    pub fn process(ctx: *ClientContext) !@This() {
        var waited: u64 = 0;
        while (waited < HEARTBEAT_INTERVAL_MS) {
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
                .who => return .who,
            };
            try sleepMs(PROCESS_SLICE_MS);
            waited += PROCESS_SLICE_MS;
        }
        return .heartbeat;
    }

    /// 服务器端：聊天消息广播给房间；/who 经 Push 通道回成员列表；退出则移除并广播离开。
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
            .heartbeat => {},
            .who => try replyMemberList(ctx),
            .quit => try ctx.leave(),
        }
    }

    /// /who 响应：向 Room 查询成员名单，格式化后追加到消息板（kind=member_list），
    /// 由本连接的 Push 按游标拉取。
    fn replyMemberList(ctx: *ServerContext) !void {
        var reply_buf: [1]MemberListReply = undefined;
        var reply: Channel(MemberListReply) = .init(&reply_buf);
        try ctx.room.ops.send(.{ .who = .{ .client_id = ctx.client_id, .reply = &reply } });
        const list = try reply.receive();

        var text_buf: [MAX_TEXT]u8 = undefined;
        var pos: usize = 0;
        pos += (std.fmt.bufPrint(text_buf[pos..], "在线 {d} 人：", .{list.count}) catch return).len;
        for (list.names[0..list.name_count], 0..) |*name, i| {
            if (i > 0) pos += (std.fmt.bufPrint(text_buf[pos..], ", ", .{}) catch break).len;
            pos += (std.fmt.bufPrint(text_buf[pos..], "{s}", .{name[0..cstrLen(name)]}) catch break).len;
        }
        if (list.truncated) pos += (std.fmt.bufPrint(text_buf[pos..], "…", .{}) catch return).len;

        var payload = PushPayload{
            .kind = @intFromEnum(PushKind.member_list),
            .seq = 0,
            .from_id = 0,
            .from_name = [_]u8{0} ** MAX_NICK,
            .text = [_]u8{0} ** MAX_TEXT,
            .ts_ms = monotonicMs(),
        };
        @memcpy(payload.text[0..pos], text_buf[0..pos]);
        ctx.room.board.append(payload);
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

/// 批量推送载荷：一帧最多 PUSH_CHUNK 条消息。
pub const ChunkPayload = struct {
    msgs: [PUSH_CHUNK]PushPayload,
    count: u8,
};

/// 服务器端按游标从 SharedBoard 批量拉取推送；无新消息时发 idle 帧
/// （同时作为 liveness，客户端 Push fiber 持续 recv）。
pub const Poll = union(enum) {
    batch: Data(ChunkPayload, Poll),
    idle: Data(void, Poll),
    quit: Data(void, Exit),

    pub const info: PushInfo = .{ .agent = .server, .name = "Poll" };

    /// 服务器端：游标后有新消息则批量拉取推送；否则空转 PUSH_POLL_MS 发 idle。
    pub fn process(ctx: *PushServerContext) !@This() {
        if (ctx.kick) return .quit;
        const c = ctx.board.len();
        if (c > ctx.cursor) {
            const end = @min(ctx.cursor + PUSH_CHUNK, c);
            const msgs = ctx.board.slice(ctx.cursor, end);
            var payload = ChunkPayload{ .msgs = undefined, .count = @intCast(msgs.len) };
            @memcpy(payload.msgs[0..msgs.len], msgs);
            ctx.cursor = end;
            return .{ .batch = .{ .data = payload } };
        }
        try sleepMs(PUSH_POLL_MS);
        return .idle;
    }

    /// 客户端：把批量消息逐条投递到收件箱（测试）或格式化打印（demo）。
    pub fn preprocess(ctx: *PushClientContext, result: @This()) !void {
        switch (result) {
            .batch => |b| try deliverChunk(ctx, b.data.msgs[0..b.data.count]),
            .idle => {},
            .quit => {},
        }
    }
};

/// 客户端处理一批推送。
fn deliverChunk(ctx: *PushClientContext, msgs: []const PushPayload) !void {
    for (msgs) |*p| {
        if (ctx.inbox) |q| {
            if (p.kind == @intFromEnum(PushKind.chat) or ctx.inbox_all) {
                try q.send(p.*); // 阻塞：消费慢时背压到服务器
            }
        } else if (ctx.out) |f| {
            var buf: [MAX_TEXT + MAX_NICK + 8]u8 = undefined;
            const line = switch (p.kind) {
                0 => std.fmt.bufPrint(&buf, "[{s}] {s}", .{
                    p.from_name[0..cstrLen(&p.from_name)],
                    p.text[0..cstrLen(&p.text)],
                }) catch continue,
                1 => std.fmt.bufPrint(&buf, "*** {s}", .{p.text[0..cstrLen(&p.text)]}) catch continue,
                2 => std.fmt.bufPrint(&buf, "*** {s}", .{p.text[0..cstrLen(&p.text)]}) catch continue,
                else => continue,
            };
            try writeLineLocked(f, ctx.out_lock, line);
        }
    }
}

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

/// 构造欢迎语，写入 `buf`（在线人数；成员名单按需 /who 获取）。
fn welcomeText(w: WelcomePayload, own_nick: *const [MAX_NICK]u8, buf: []u8) []const u8 {
    var pos: usize = 0;
    pos += (std.fmt.bufPrint(buf[pos..], "== 欢迎，{s}！你的 ID 是 {d}。当前在线 {d} 人（/who 查看名单）。", .{
        own_nick[0..cstrLen(own_nick)],
        w.client_id,
        w.member_count,
    }) catch return buf[0..pos]).len;
    return buf[0..pos];
}
