const std = @import("std");
const polyrole = @import("../../root.zig");
const Data = polyrole.Data;
const ProtocolInfo = polyrole.ProtocolInfo;
const Exit = polyrole.Exit;

pub const Info = ProtocolInfo("init", ClientContext, ServerContext);
pub const MaxNameLen = 32;

pub const ClientContext = struct {
    /// Client's chosen username, set before symmetric_run
    username: [MaxNameLen]u8 = undefined,
    name_len: usize = 0,
    /// Server accepted the name
    accepted: bool = false,
};

pub const ServerContext = struct {
    /// Registered usernames (borrowed pointer)
    users: *std.StringHashMap(void),
    /// Last proposed name (set by preprocess, used by process)
    pending_name: [MaxNameLen]u8 = undefined,
    pending_len: usize = 0,
};

pub const NamePayload = struct {
    name: [MaxNameLen]u8,
    name_len: usize,
};

pub const Send = union(enum) {
    propose: Data(NamePayload, Reply),
    quit: Data(void, Exit),

    pub const info: Info = .{ .agent = .client, .name = "Send" };

    pub fn process(ctx: *ClientContext) @This() {
        if (ctx.name_len == 0) return .quit;
        return .{ .propose = .{ .data = .{ .name = ctx.username, .name_len = ctx.name_len } } };
    }

    pub fn preprocess(ctx: *ServerContext, result: @This()) void {
        switch (result) {
            .propose => |d| {
                ctx.pending_name = d.data.name;
                ctx.pending_len = d.data.name_len;
            },
            .quit => {},
        }
    }
};

pub const Reply = union(enum) {
    accept: Data(void, Exit),
    reject: Data(void, Send),

    pub const info: Info = .{ .agent = .server, .name = "Reply" };

    pub fn process(ctx: *ServerContext) @This() {
        const name = ctx.pending_name[0..ctx.pending_len];
        if (ctx.users.contains(name)) return .reject;
        ctx.users.put(name, {}) catch unreachable;
        return .accept;
    }

    pub fn preprocess(ctx: *ClientContext, result: @This()) void {
        switch (result) {
            .accept => ctx.accepted = true,
            .reject => {},
        }
    }
};
