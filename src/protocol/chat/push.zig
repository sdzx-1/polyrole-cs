const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const CHUNK_SIZE: usize = 8;

pub const Info = ProtocolInfo("push", ClientContext, ServerContext);

pub const ClientContext = struct {
    gpa: std.mem.Allocator,
    recv: *std.ArrayList(Message),
    chunk_done: bool = false,
};

pub const ServerContext = struct {
    board: *SharedBoard,
    cursor: usize = 0,
    batch_end: usize = 0,
    poll_ms: u64 = 100,
    kick: bool = false,
};

pub const Message = struct { kind: u8, from: []const u8, text: []const u8 };
pub const KIND_MSG: u8 = 1;
pub const KIND_JOIN: u8 = 2;

pub const ChunkPayload = struct { msgs: [CHUNK_SIZE]Message, count: u8 };

pub const SharedBoard = struct {
    items: std.ArrayList(Message),
    mu: zio.Mutex,
    committed: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    pub fn init(gpa: std.mem.Allocator, capacity: usize) @This() {
        var self: @This() = .{ .items = .empty, .mu = .{} };
        self.items.ensureTotalCapacity(gpa, capacity) catch unreachable;
        return self;
    }

    pub fn deinit(self: *@This(), gpa: std.mem.Allocator) void {
        self.items.deinit(gpa);
    }

    pub fn append(self: *@This(), msg: Message) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        self.items.appendAssumeCapacity(msg);
        _ = self.committed.store(self.items.items.len, .release);
    }
};

fn fillPayload(msgs: []const Message) ChunkPayload {
    var payload: ChunkPayload = .{ .msgs = [_]Message{.{ .kind = 0, .from = &.{}, .text = &.{} }} ** CHUNK_SIZE, .count = @intCast(msgs.len) };
    @memcpy(payload.msgs[0..msgs.len], msgs);
    return payload;
}

/// Decision: history sync. Check committed, route to direct / chunk / done.
pub const Sync = union(enum) {
    direct: Data(ChunkPayload, AckSmall),
    chunk: Data(void, Chunk(Poll)),
    done: Data(void, Poll),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Sync" };

    pub fn process(ctx: *ServerContext) @This() {
        const c = ctx.board.committed.load(.acquire);
        if (c == 0) {
            if (ctx.kick) return .quit;
            return .done;
        }
        ctx.batch_end = c;
        const len = c;
        if (len <= CHUNK_SIZE) {
            ctx.cursor = len;
            const payload = fillPayload(ctx.board.items.items[0..len]);
            return .{ .direct = .{ .data = payload } };
        }
        return .chunk;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .direct => |d| appendMsgs(ctx, d.data.msgs[0..d.data.count]),
            else => {},
        }
    }
};

/// Decision: poll for new data. Check committed vs cursor, route to direct / chunk / wait.
pub const Poll = union(enum) {
    direct: Data(ChunkPayload, AckSmall),
    chunk: Data(void, Chunk(Poll)),
    wait: Data(void, @This()),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Poll" };

    pub fn process(ctx: *ServerContext) @This() {
        const c = ctx.board.committed.load(.acquire);
        if (c > ctx.cursor) {
            ctx.batch_end = c;
            const len = c - ctx.cursor;
            if (len <= CHUNK_SIZE) {
                const msgs = ctx.board.items.items[ctx.cursor..c];
                ctx.cursor = c;
                const payload = fillPayload(msgs);
                return .{ .direct = .{ .data = payload } };
            }
            return .chunk;
        }
        if (ctx.kick) return .quit;
        zio.sleep(zio.Duration.fromMilliseconds(ctx.poll_ms)) catch {};
        return .wait;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .direct => |d| appendMsgs(ctx, d.data.msgs[0..d.data.count]),
            else => {},
        }
    }
};

/// Execution: send one chunk. Self-loop until batch_end reached, then → Done.
pub fn Chunk(comptime Done: type) type {
    return union(enum) {
        items: Data(ChunkPayload, AckChunk(Done)),
        last: Data(ChunkPayload, AckChunk(Done)),

        pub const info: Info = .{ .agent = .server, .name = "Chunk" };

        pub fn process(ctx: *ServerContext) @This() {
            const end = @min(ctx.cursor + CHUNK_SIZE, ctx.batch_end);
            const msgs = ctx.board.items.items[ctx.cursor..end];
            ctx.cursor = end;
            const payload = fillPayload(msgs);
            if (end >= ctx.batch_end) {
                return .{ .last = .{ .data = payload } };
            } else {
                return .{ .items = .{ .data = payload } };
            }
        }

        pub fn preprocess(ctx: *ClientContext, result: @This()) void {
            switch (result) {
                .items => |d| {
                    ctx.chunk_done = false;
                    appendMsgs(ctx, d.data.msgs[0..d.data.count]);
                },
                .last => |d| {
                    ctx.chunk_done = true;
                    appendMsgs(ctx, d.data.msgs[0..d.data.count]);
                },
            }
        }
    };
}

fn appendMsgs(ctx: *ClientContext, msgs: []const Message) void {
    for (msgs) |m| {
        const from = ctx.gpa.dupe(u8, m.from) catch return;
        const text = ctx.gpa.dupe(u8, m.text) catch {
            ctx.gpa.free(from);
            return;
        };
        ctx.recv.append(ctx.gpa, .{ .kind = m.kind, .from = from, .text = text }) catch {};
    }
}

/// Ack for Chunk: continue chunking or finish → Done.
pub fn AckChunk(comptime Done: type) type {
    return union(enum) {
        ok: Data(void, Chunk(Done)),
        into: Data(void, Done),

        pub const info: Info = .{ .agent = .client, .name = "AckChunk" };

        pub fn process(ctx: *ClientContext) @This() {
            if (ctx.chunk_done) return .into;
            return .ok;
        }

        pub fn preprocess(ctx: *ServerContext, result: @This()) void {
            _ = ctx;
            _ = result;
        }
    };
}

/// Ack for Sync/Poll direct: simple confirm → Poll.
pub const AckSmall = union(enum) {
    ok: Data(void, Poll),

    pub const info: Info = .{ .agent = .client, .name = "AckSmall" };

    pub fn process(ctx: *ClientContext) @This() {
        _ = ctx;
        return .ok;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        _ = ctx;
        _ = result;
    }
};
