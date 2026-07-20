const std = @import("std");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("push", ClientContext, ServerContext);
pub const MaxTextLen = 256;
pub const MaxNameLen = 32;

pub const ClientContext = struct {
    gpa: std.mem.Allocator,
    received: *std.ArrayList(Message),
};

pub const ServerContext = struct {
    pending: ?Message = null,
    kick: bool = false,
};

pub const Message = struct {
    kind: Kind,
    from: []const u8,
    text: []const u8,
};

pub const Kind = u8;
pub const KIND_MSG: u8 = 1;
pub const KIND_JOIN: u8 = 2;
pub const KIND_LEAVE: u8 = 3;
pub const KIND_KICK: u8 = 4;

pub const ItemPayload = struct {
    kind: Kind,
    from: [MaxNameLen]u8,
    from_len: usize,
    text: [MaxTextLen]u8,
    text_len: usize,
};

pub const Push = union(enum) {
    item: Data(ItemPayload, Ack),
    kick: Data(void, Exit),

    pub const info: Info = .{ .agent = .server, .name = "Push" };

    pub fn process(ctx: *ServerContext) @This() {
        if (ctx.kick) return .kick;
        if (ctx.pending) |msg| {
            var from_buf: [MaxNameLen]u8 = undefined;
            const from_len = @min(msg.from.len, MaxNameLen);
            @memcpy(from_buf[0..from_len], msg.from[0..from_len]);
            var text_buf: [MaxTextLen]u8 = undefined;
            const text_len = @min(msg.text.len, MaxTextLen);
            @memcpy(text_buf[0..text_len], msg.text[0..text_len]);
            ctx.pending = null;
            return .{ .item = .{ .data = .{
                .kind = msg.kind,
                .from = from_buf,
                .from_len = from_len,
                .text = text_buf,
                .text_len = text_len,
            } } };
        }
        return .kick;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .item => |d| {
                const from = ctx.gpa.dupe(u8, d.data.from[0..d.data.from_len]) catch return;
                const text = ctx.gpa.dupe(u8, d.data.text[0..d.data.text_len]) catch return;
                ctx.received.append(ctx.gpa, .{
                    .kind = d.data.kind,
                    .from = from,
                    .text = text,
                }) catch {};
            },
            .kick => {},
        }
    }
};

pub const Ack = union(enum) {
    ok: Data(void, Push),

    pub const info: Info = .{ .agent = .client, .name = "Ack" };

    pub fn process(ctx: *ClientContext) @This() {
        _ = ctx;
        return .ok;
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        _ = ctx;
        _ = result;
    }
};
