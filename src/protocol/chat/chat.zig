const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;
const push = @import("push.zig");

pub const BcMsg = push.Message;

pub const Info = ProtocolInfo("chat", ClientContext, ServerContext);

pub const ClientContext = struct {
    input_ch: *zio.Channel([]const u8),
};

/// A pub/sub channel: publish() sends to all subscribers.
pub const BroadcastChannel = struct {
    subs: std.ArrayList(*zio.Channel(BcMsg)),
    mu: zio.Mutex,
    gpa: std.mem.Allocator,

    pub fn subscribe(self: *@This(), ch: *zio.Channel(BcMsg)) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        self.subs.append(self.gpa, ch) catch {};
    }

    pub fn unsubscribe(self: *@This(), ch: *zio.Channel(BcMsg)) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        for (self.subs.items, 0..) |item, i| {
            if (item == ch) {
                _ = self.subs.swapRemove(i);
                break;
            }
        }
    }

    pub fn publish(self: *@This(), msg: Message) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        for (self.subs.items) |ch| {
            ch.send(.{ .kind = 1, .from = msg.from, .text = msg.text }) catch {};
        }
    }
};

pub const ServerContext = struct {
    gpa: std.mem.Allocator,
    board: *std.ArrayList(Message),
    mu: *zio.Mutex,
    username: []const u8,
    bc: *BroadcastChannel,
};

pub const Message = struct { from: []const u8, text: []const u8 };
pub const MsgPayload = struct { text: []const u8 };

/// Persistent loop: Say.send → Ack.ok → Say.send → ... → input_ch closed → Say.quit
pub const Say = union(enum) {
    send: Data(MsgPayload, Ack),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Say" };

    pub fn process(ctx: *ClientContext) @This() {
        const text = ctx.input_ch.receive() catch return .quit;
        return .{ .send = .{ .data = .{ .text = text } } };
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const from = ctx.gpa.dupe(u8, ctx.username) catch return;
                const text = ctx.gpa.dupe(u8, d.data.text) catch return;
                ctx.mu.lockUncancelable();
                defer ctx.mu.unlock();
                ctx.board.append(ctx.gpa, .{ .from = from, .text = text }) catch {};
                ctx.bc.publish(.{ .from = from, .text = text });
            },
            .quit => {},
        }
    }
};

pub const Ack = union(enum) {
    ok: Data(void, Say),

    pub const info: Info = .{ .agent = .server, .name = "Ack" };

    pub fn process(ctx: *ServerContext) @This() {
        _ = ctx;
        return .ok;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        _ = ctx;
        _ = result;
    }
};
