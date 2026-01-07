const std = @import("std");
const SsTable = @import("table.zig").SsTable;
const Block = @import("../block.zig").block.Block;
const BlockIterator = @import("../block.zig").iterator.BlockIterator;
const Kv = @import("../kv.zig").Kv;

pub const SsTableIterator = struct {
    table: *SsTable,
    block_iterator: BlockIterator,
    block_index: usize,
    block: *Block,

    pub fn deinit(self: SsTableIterator) void {
        self.block.deinit();
        self.table.gpa.destroy(self.block);
    }

    pub fn create_and_seek_to_first(table: *SsTable) !SsTableIterator {
        const block = try table.gpa.create(Block);
        var itr = SsTableIterator{
            .table = table,
            .block_iterator = undefined,
            .block = block,
            .block_index = 0,
        };
        try itr.seek_to_first();
        return itr;
    }

    pub fn seek_to_first(self: *SsTableIterator) !void {
        self.block_index = 0;
        self.block.* = try self.table.read_block(self.block_index);
        self.block_iterator = try BlockIterator.init_and_seek_to_first(self.block);
    }

    pub fn create_and_seek_to_key(table: *SsTable, key: []const u8) !SsTableIterator {
        const block = try table.gpa.create(Block);
        var itr = SsTableIterator{
            .table = table,
            .block_iterator = undefined,
            .block_index = undefined,
            .block = block,
        };
        try itr.seek_to_key(key);
        return itr;
    }

    pub fn seek_to_key(self: *SsTableIterator, key: []const u8) !void {
        var block_index = self.table.find_block_index(key);
        var itr: BlockIterator = undefined;
        if (block_index >= self.table.block_metas.len) {
            const meta = self.table.block_metas[self.table.block_metas.len - 1];
            self.block.* = try self.table.read_block(self.table.block_metas.len - 1);
            itr = try BlockIterator.init_and_seek_to_key(self.block, meta.last_key);
            _ = try itr.next();
            block_index = self.table.block_metas.len - 1;
            return;
        } else {
            self.block.* = try self.table.read_block(block_index);
            itr = try BlockIterator.init_and_seek_to_key(self.block, key);
        }
        self.block_iterator = itr;
        self.block_index = block_index;
    }

    pub fn next(self: *SsTableIterator) !?Kv {
        const next_tuple = try self.block_iterator.next();
        if (next_tuple) |tuple| {
            return Kv.init(tuple.key, tuple.val);
        } else {
            if (self.block_index < self.table.block_metas.len - 1) {
                self.block_index += 1;
                self.block.deinit();
                const new_block = try self.table.read_block(self.block_index);
                self.block.* = new_block;
                self.block_iterator = try BlockIterator.init_and_seek_to_first(self.block);
                return try self.next();
            } else {
                return null;
            }
        }
    }
};

test "SsTableIterator: create_and_seek_to_first, next" {
    const test_gpa = std.testing.allocator;
    const test_sstable = @import("test_table.zig");
    var sst = try test_sstable.create_sst_test("sst_table_iterator_test.sst", test_gpa);
    defer test_sstable.close_sst_test(&sst, "sst_table_iterator_test.sst");
    var itr = try SsTableIterator.create_and_seek_to_first(&sst);
    defer itr.deinit();
    const first = try itr.next();
    try std.testing.expectEqualStrings("key1", first.?.key);
    try std.testing.expectEqualStrings("val1", first.?.val);
    const second = try itr.next();
    try std.testing.expectEqualStrings("key2", second.?.key);
    try std.testing.expectEqualStrings("val2", second.?.val);
    const third = try itr.next();
    try std.testing.expectEqualStrings("key3", third.?.key);
    try std.testing.expectEqualStrings("val3", third.?.val);
    const fourth = try itr.next();
    try std.testing.expectEqualStrings("key4", fourth.?.key);
    try std.testing.expectEqualStrings("val4", fourth.?.val);
    const fifth = try itr.next();
    try std.testing.expectEqualStrings("key5", fifth.?.key);
    try std.testing.expectEqualStrings("val5", fifth.?.val);
    try std.testing.expectEqual(null, itr.next());
}

test "SsTableIterator: create_and_seek_to_key, next" {
    const test_gpa = std.testing.allocator;
    const test_sstable = @import("test_table.zig");
    var sst = try test_sstable.create_sst_test("sst_table_iterator_test.sst", test_gpa);
    defer test_sstable.close_sst_test(&sst, "sst_table_iterator_test.sst");
    var itr = try SsTableIterator.create_and_seek_to_key(&sst, "key3");
    defer itr.deinit();
    const third = try itr.next();
    try std.testing.expectEqualStrings("key3", third.?.key);
    try std.testing.expectEqualStrings("val3", third.?.val);
    const fourth = try itr.next();
    try std.testing.expectEqualStrings("key4", fourth.?.key);
    try std.testing.expectEqualStrings("val4", fourth.?.val);
    const fifth = try itr.next();
    try std.testing.expectEqualStrings("key5", fifth.?.key);
    try std.testing.expectEqualStrings("val5", fifth.?.val);
    try std.testing.expectEqual(null, itr.next());
}
