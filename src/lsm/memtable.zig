const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const DefaultSkiplist = @import("skiplist.zig").DefaultSkiplist;
const SsTableBuilder = @import("table.zig").SsTableBuilder;
const Wal = @import("wal.zig").Wal;
const Kv = @import("kv.zig").Kv;

pub const Memtable = struct {
    pub const MemtableIterator = struct {
        curr: *DefaultSkiplist.Node,
        tail: *DefaultSkiplist.Node,
        end: []const u8,

        pub fn next(self: *MemtableIterator) !?Kv {
            if (self.curr == self.tail) {
                return null;
            } else if (mem.order(u8, self.curr.key, self.end) != .lt) {
                return null;
            } else {
                const key = self.curr.key;
                const val = self.curr.val;
                self.curr = self.curr.next[0];
                return Kv.init(key, val);
            }
        }
    };

    id: usize,
    buffer: []u8,
    buffer_size: usize, // 128 or 256 MB
    buffer_pos: usize = 0,
    gpa: mem.Allocator,
    skiplist: DefaultSkiplist,
    wal: ?Wal,

    pub fn init(id: usize, buffer_size: usize, gpa: mem.Allocator) !Memtable {
        const buffer = try gpa.alloc(u8, buffer_size);
        return Memtable{
            .id = id,
            .gpa = gpa,
            .skiplist = try DefaultSkiplist.init(gpa),
            .buffer = buffer,
            .buffer_size = buffer_size,
            .wal = null,
        };
    }

    pub fn deinit(self: Memtable) void {
        self.skiplist.deinit();
        self.gpa.free(self.buffer);
        if (self.wal) |*w| {
            w.deinit();
        }
    }

    pub fn delete_wal(self: Memtable, dir: *fs.Dir) !void {
        std.debug.assert(self.wal != null);
        const wal_filename = try Wal.get_wal_filename(self.id, self.gpa);
        defer self.gpa.free(wal_filename);
        try dir.deleteFile(wal_filename);
    }

    pub fn size(self: Memtable) usize {
        return self.skiplist.size;
    }

    pub fn get(self: Memtable, key: []const u8) ?[]const u8 {
        return self.skiplist.get(key);
    }

    pub fn willBeFull(self: Memtable, key: []const u8, val: []const u8) bool {
        return self.buffer_pos + key.len + val.len > self.buffer_size;
    }

    pub fn put(self: *Memtable, key: []const u8, val: []const u8) !void {
        std.debug.assert(!self.willBeFull(key, val));
        @memcpy(self.buffer[self.buffer_pos .. self.buffer_pos + key.len], key);
        const new_key_ptr = self.buffer[self.buffer_pos .. self.buffer_pos + key.len];
        self.buffer_pos += key.len;
        @memcpy(self.buffer[self.buffer_pos .. self.buffer_pos + val.len], val);
        const new_val_ptr = self.buffer[self.buffer_pos .. self.buffer_pos + val.len];
        self.buffer_pos += val.len;
        try self.skiplist.put(new_key_ptr, new_val_ptr);

        if (self.wal) |*w| {
            try w.put(new_key_ptr, new_val_ptr);
        }
    }

    // [lower, upper)
    pub fn scan(self: Memtable, lower: []const u8, upper: []const u8) MemtableIterator {
        const start_node = self.skiplist.get_greater_or_equal_to(lower);
        return MemtableIterator{
            .curr = start_node,
            .tail = self.skiplist.tail,
            .end = upper,
        };
    }

    pub fn flush(self: *Memtable, builder: *SsTableBuilder) !void {
        var node = self.skiplist.head.next[0];
        while (node != self.skiplist.tail) : (node = node.next[0]) {
            try builder.add(node.key, node.val);
        }
    }
};

test "Memtable: put, get" {
    const test_gpa = std.testing.allocator;
    var memtable = try Memtable.init(0, 128, test_gpa);
    defer memtable.deinit();

    try memtable.put("key1", "value1");
    try memtable.put("key2", "value2");
    try memtable.put("key3", "value3");
    try std.testing.expectEqualStrings("value1", memtable.get("key1").?);
    try std.testing.expectEqualStrings("value2", memtable.get("key2").?);
    try std.testing.expectEqualStrings("value3", memtable.get("key3").?);
    try std.testing.expect(memtable.get("key4") == null);
    try memtable.put("key2", "value2_updated");
    try std.testing.expectEqualStrings("value2_updated", memtable.get("key2").?);
}

test "Memtable: scan" {
    const test_gpa = std.testing.allocator;
    var memtable = try Memtable.init(0, 128, test_gpa);
    defer memtable.deinit();

    try memtable.put("1", "value1");
    try memtable.put("3", "value3");
    try memtable.put("4", "value4");

    var iter1_4 = memtable.scan("1", "4");
    const val1_4_1 = try iter1_4.next();
    try std.testing.expectEqualStrings("value1", val1_4_1.?.val);
    const val1_4_3 = try iter1_4.next();
    try std.testing.expectEqualStrings("value3", val1_4_3.?.val);
    try std.testing.expectEqual(null, try iter1_4.next());

    var iter2_5 = memtable.scan("2", "5");
    const val2_5_3 = try iter2_5.next();
    try std.testing.expectEqualStrings("value3", val2_5_3.?.val);
    const val2_5_4 = try iter2_5.next();
    try std.testing.expectEqualStrings("value4", val2_5_4.?.val);
    try std.testing.expectEqual(null, try iter2_5.next());

    var iter2_3 = memtable.scan("2", "3");
    try std.testing.expectEqual(null, try iter2_3.next());
}
