const std = @import("std");
const mem = std.mem;

pub const Kv = struct {
    key: []const u8,
    val: []const u8,

    pub fn init(gpa: mem.Allocator) error{OutOfMemory}!Kv {
        const alloc_key = try gpa.alloc(u8, 0);
        const alloc_val = try gpa.alloc(u8, 0);
        return Kv{ .key = alloc_key, .val = alloc_val };
    }

    pub fn deinit(self: *Kv, gpa: mem.Allocator) void {
        gpa.free(self.key);
        gpa.free(self.val);
    }

    pub fn alloc(
        self: *Kv,
        key: []const u8,
        val: []const u8,
        gpa: mem.Allocator,
    ) error{OutOfMemory}!void {
        self.deinit(gpa);
        const alloc_key = try gpa.dupe(u8, key);
        const alloc_val = try gpa.dupe(u8, val);
        self.key = alloc_key;
        self.val = alloc_val;
    }
};
