const std = @import("std");
const SsTable = @import("table.zig").SsTable;
const SsTableError = SsTable.SsTableError;
const Block = @import("../block.zig").block.Block;
const BlockIterator = @import("../block.zig").iterator.BlockIterator;
const Kv = @import("../kv.zig").Kv;

pub const SsTableIterator = struct {
    table: *SsTable,
    block_iterator: BlockIterator,
    block_index: i64,
    block: *Block,

    pub fn deinit(self: SsTableIterator) void {
        self.block.deinit();
        self.table.gpa.destroy(self.block);
    }

    pub fn create_and_seek_to_first(table: *SsTable) SsTableError!SsTableIterator {
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

    pub fn seek_to_first(self: *SsTableIterator) SsTableError!void {
        self.block_index = 0;
        self.block.* = try self.table.read_block(@intCast(self.block_index));
        self.block_iterator = BlockIterator.init_and_seek_to_first(self.block);
    }

    pub fn create_and_seek_to_key(table: *SsTable, k: []const u8) SsTableError!SsTableIterator {
        const block = try table.gpa.create(Block);
        var itr = SsTableIterator{
            .table = table,
            .block_iterator = undefined,
            .block_index = undefined,
            .block = block,
        };
        try itr.seek_to_key(k);
        return itr;
    }

    pub fn seek_to_key(self: *SsTableIterator, k: []const u8) SsTableError!void {
        var block_index = self.table.find_block_index(k);
        var itr: BlockIterator = undefined;
        if (block_index >= self.table.block_metas.len) {
            const meta = self.table.block_metas[self.table.block_metas.len - 1];
            self.block.* = try self.table.read_block(self.table.block_metas.len - 1);
            itr = BlockIterator.init_and_seek_to_key(self.block, meta.last_key);
            itr.next();
            block_index = self.table.block_metas.len - 1;
            return;
        } else {
            self.block.* = try self.table.read_block(block_index);
            itr = BlockIterator.init_and_seek_to_key(self.block, k);
        }
        self.block_iterator = itr;
        self.block_index = @intCast(block_index);
    }

    pub fn key(self: SsTableIterator) []const u8 {
        return self.block_iterator.key();
    }

    pub fn val(self: SsTableIterator) []const u8 {
        return self.block_iterator.val();
    }

    pub fn is_valid(self: SsTableIterator) bool {
        if (self.block_index < self.table.block_metas.len - 1 and self.block_index > 0) {
            return true;
        } else {
            return self.block_iterator.is_valid();
        }
    }

    pub fn next(self: *SsTableIterator) SsTableError!void {
        self.block_iterator.next();
        if (self.block_iterator.is_valid()) return;

        if (self.block_index >= self.table.block_metas.len - 1) return;
        self.block_index += 1;
        self.block.deinit();
        const new_block = try self.table.read_block(@intCast(self.block_index));
        self.block.* = new_block;
        self.block_iterator = BlockIterator.init_and_seek_to_first(self.block);
    }

    pub fn prev(self: *SsTableIterator) SsTableError!void {
        self.block_iterator.prev();
        if (self.block_iterator.is_valid()) return;

        if (self.block_index == 0) return;
        self.block_index -= 1;
        self.block.deinit();
        const new_block = try self.table.read_block(@intCast(self.block_index));
        self.block.* = new_block;
        self.block_iterator = BlockIterator.init_and_seek_to_last(self.block);
    }
};

test "SsTableIterator: create_and_seek_to_first, next, prev" {
    const test_gpa = std.testing.allocator;
    const test_sstable = @import("test_table.zig");
    var sst = try test_sstable.create_sst_test("sst_table_iterator_test.sst", test_gpa);
    defer test_sstable.close_sst_test(&sst, "sst_table_iterator_test.sst");
    var itr = try SsTableIterator.create_and_seek_to_first(&sst);
    defer itr.deinit();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key1", itr.key());
    try std.testing.expectEqualStrings("val1", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key2", itr.key());
    try std.testing.expectEqualStrings("val2", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key3", itr.key());
    try std.testing.expectEqualStrings("val3", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key4", itr.key());
    try std.testing.expectEqualStrings("val4", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key5", itr.key());
    try std.testing.expectEqualStrings("val5", itr.val());
    try itr.next();
    try std.testing.expect(!itr.is_valid());

    try itr.prev();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key5", itr.key());
    try std.testing.expectEqualStrings("val5", itr.val());
    try itr.prev();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key4", itr.key());
    try std.testing.expectEqualStrings("val4", itr.val());
    try itr.prev();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key3", itr.key());
    try std.testing.expectEqualStrings("val3", itr.val());
    try itr.prev();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key2", itr.key());
    try std.testing.expectEqualStrings("val2", itr.val());
    try itr.prev();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key1", itr.key());
    try std.testing.expectEqualStrings("val1", itr.val());
    try itr.prev();
    try std.testing.expect(!itr.is_valid());
}

test "SsTableIterator: create_and_seek_to_key, next" {
    const test_gpa = std.testing.allocator;
    const test_sstable = @import("test_table.zig");
    var sst = try test_sstable.create_sst_test("sst_table_iterator_test.sst", test_gpa);
    defer test_sstable.close_sst_test(&sst, "sst_table_iterator_test.sst");
    var itr = try SsTableIterator.create_and_seek_to_key(&sst, "key3");
    defer itr.deinit();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key3", itr.key());
    try std.testing.expectEqualStrings("val3", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key4", itr.key());
    try std.testing.expectEqualStrings("val4", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("key5", itr.key());
    try std.testing.expectEqualStrings("val5", itr.val());
    try itr.next();
    try std.testing.expect(!itr.is_valid());
}
