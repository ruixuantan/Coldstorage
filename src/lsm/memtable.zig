const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const DefaultSkiplist = @import("skiplist.zig").DefaultSkiplist;
const SsTableBuilder = @import("table.zig").SsTableBuilder;
const Wal = @import("wal.zig").Wal;

pub const Memtable = struct {
    pub const MemtableIterator = struct {
        curr: *DefaultSkiplist.Node,
        head: *DefaultSkiplist.Node,
        tail: *DefaultSkiplist.Node,

        pub fn key(self: MemtableIterator) []const u8 {
            return self.curr.key;
        }

        pub fn val(self: MemtableIterator) []const u8 {
            return self.curr.val;
        }

        pub fn is_valid(self: *MemtableIterator) bool {
            return self.curr != self.tail and self.curr != self.head.prev;
        }

        pub fn next(self: *MemtableIterator) void {
            self.curr = self.curr.next[0];
        }

        pub fn prev(self: *MemtableIterator) void {
            self.curr = self.curr.prev;
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
        _ = upper;
        const start_node = self.skiplist.get_greater_or_equal_to(lower);
        return MemtableIterator{
            .curr = start_node,
            .head = self.skiplist.head,
            .tail = self.skiplist.tail,
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
    try std.testing.expect(iter1_4.is_valid());
    try std.testing.expectEqualStrings("value1", iter1_4.val());
    iter1_4.next();
    try std.testing.expect(iter1_4.is_valid());
    try std.testing.expectEqualStrings("value3", iter1_4.val());
    iter1_4.next();
    try std.testing.expect(iter1_4.is_valid());
    iter1_4.next();
    try std.testing.expect(!iter1_4.is_valid());

    iter1_4.prev();
    try std.testing.expect(iter1_4.is_valid());
    try std.testing.expectEqualStrings("value4", iter1_4.val());
    iter1_4.prev();
    try std.testing.expect(iter1_4.is_valid());
    try std.testing.expectEqualStrings("value3", iter1_4.val());
    iter1_4.prev();
    try std.testing.expect(iter1_4.is_valid());
    try std.testing.expectEqualStrings("value1", iter1_4.val());
    iter1_4.prev();
    try std.testing.expect(iter1_4.is_valid());
    iter1_4.prev();
    try std.testing.expect(!iter1_4.is_valid());

    var iter2_5 = memtable.scan("2", "5");
    try std.testing.expect(iter2_5.is_valid());
    try std.testing.expectEqualStrings("value3", iter2_5.val());
    iter2_5.next();
    try std.testing.expect(iter2_5.is_valid());
    try std.testing.expectEqualStrings("value4", iter2_5.val());

    var iter2_3 = memtable.scan("2", "3");
    try std.testing.expect(iter2_3.is_valid());
}
