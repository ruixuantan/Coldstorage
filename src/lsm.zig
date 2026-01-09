const std = @import("std");
const skiplist = @import("lsm/skiplist.zig");
const memtable = @import("lsm/memtable.zig");
const lsm = @import("lsm/lsm.zig");
const lsm_iterator = @import("lsm/lsm_iterator.zig");
const iterator = @import("lsm/iterator.zig");
const block = @import("lsm/block.zig");
const table = @import("lsm/table.zig");
const manifest = @import("lsm/manifest.zig");
const wal = @import("lsm/wal.zig");

pub const Kv = @import("lsm/kv.zig").Kv;

pub const LsmStorageOptions = lsm.LsmStorageOptions;
pub const LsmIterator = lsm_iterator.LsmIterator;

pub const Lsm = struct {
    inner: lsm.LsmStorageInner,

    pub fn open(path: []const u8, options: LsmStorageOptions, gpa: std.mem.Allocator) !Lsm {
        return .{ .inner = try lsm.LsmStorageInner.open(path, options, gpa) };
    }

    pub fn close(self: *Lsm) !void {
        try self.inner.close();
    }

    pub fn get(self: Lsm, key: []const u8, buf: []u8) !?[]const u8 {
        return try self.inner.get(key, buf);
    }

    pub fn put(self: *Lsm, key: []const u8, val: []const u8) !void {
        try self.inner.put(key, val);
    }

    pub fn del(self: *Lsm, key: []const u8) !void {
        try self.inner.del(key);
    }

    // [lower, upper)
    pub fn scan(self: Lsm, lower: []const u8, upper: []const u8) !LsmIterator {
        return try self.inner.scan(lower, upper);
    }
};

test {
    _ = skiplist;
    _ = memtable;
    // _ = block;
    // _ = table;
    // _ = iterator;
    // _ = lsm;
    // _ = lsm_iterator;
    // _ = manifest;
    // _ = wal;
}
