const std = @import("std");
const zio = @import("zio");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("chat", ClientContext, ServerContext);

pub const ClientContext = struct {
    pending_text: ?[]const u8 = null,
    done: bool = false,
};

pub const ServerContext = struct {
    gpa: std.mem.Allocator,
    messages: *std.ArrayList(Message),
    mu: *zio.Mutex,
    username: []const u8 = "",
};

pub const Message = struct {
    from: []const u8,
    text: []const u8,
};

pub const MsgPayload = struct {
    text: []const u8,
};

pub const Say = union(enum) {
    send: Data(MsgPayload, Ack),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Say" };

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.done) return .quit;
        if (ctx.pending_text) |text| {
            ctx.pending_text = null;
            return .{ .send = .{ .data = .{ .text = text } } };
        }
        return .quit;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .send => |d| {
                const from_dup = ctx.gpa.dupe(u8, ctx.username) catch return;
                const text_dup = ctx.gpa.dupe(u8, d.data.text) catch return;
                ctx.mu.lockUncancelable();
                defer ctx.mu.unlock();
                ctx.messages.ensureUnusedCapacity(ctx.gpa, 1) catch return;
                ctx.messages.appendAssumeCapacity(.{ .from = from_dup, .text = text_dup });
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
