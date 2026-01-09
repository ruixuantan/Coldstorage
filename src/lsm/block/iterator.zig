const std = @import("std");
const Block = @import("block.zig").Block;
const Kv = @import("../kv.zig").Kv;

pub const BlockIterator = struct {
    block: *const Block,
    key_start_offset: usize,
    key_end_offset: usize,
    val_start_offset: usize,
    val_end_offset: usize,
    idx: i64, // index of the current entry, not byte offset

    fn init(block: *const Block) BlockIterator {
        return BlockIterator{
            .block = block,
            .key_start_offset = 0,
            .key_end_offset = 0,
            .val_start_offset = 0,
            .val_end_offset = 0,
            .idx = 0,
        };
    }

    fn seek_to_idx(self: *BlockIterator, target_idx: usize) void {
        if (target_idx >= self.block.offsets.len) return;

        const offset: usize = @intCast(self.block.offsets[target_idx]);
        const key_len: usize = @intCast(
            std.mem.bytesAsValue(u16, self.block.data[offset .. offset + 2]).*,
        );
        const val_offset = key_len + 2 + offset;
        const val_len: usize = @intCast(
            std.mem.bytesAsValue(u16, self.block.data[val_offset .. val_offset + 2]).*,
        );

        self.key_start_offset = offset + 2;
        self.key_end_offset = self.key_start_offset + key_len;
        self.val_start_offset = val_offset + 2;
        self.val_end_offset = self.val_start_offset + val_len;
        self.idx = @intCast(target_idx);
    }

    pub fn init_and_seek_to_first(block: *const Block) BlockIterator {
        var it = BlockIterator.init(block);
        it.seek_to_idx(0);
        return it;
    }

    pub fn init_and_seek_to_last(block: *const Block) BlockIterator {
        var it = BlockIterator.init(block);
        it.seek_to_idx(block.offsets.len - 1);
        return it;
    }

    // will seek to self.key >= target_key
    pub fn init_and_seek_to_key(block: *const Block, target_key: []const u8) BlockIterator {
        var it = BlockIterator.init(block);
        var lo: usize = 0;
        var hi: usize = block.offsets.len;

        while (lo < hi) {
            const mid: usize = lo + @divFloor((hi - lo), 2);
            it.seek_to_idx(mid);
            const ord = std.mem.order(
                u8,
                it.block.data[it.key_start_offset..it.key_end_offset],
                target_key,
            );
            switch (ord) {
                .eq => return it,
                .lt => lo = mid + 1,
                .gt => hi = mid,
            }
        }
        it.seek_to_idx(lo);
        return it;
    }

    pub fn key(self: BlockIterator) []const u8 {
        return self.block.data[self.key_start_offset..self.key_end_offset];
    }

    pub fn val(self: BlockIterator) []const u8 {
        return self.block.data[self.val_start_offset..self.val_end_offset];
    }

    pub fn is_valid(self: BlockIterator) bool {
        return self.idx < self.block.offsets.len and self.idx >= 0;
    }

    pub fn next(self: *BlockIterator) void {
        self.idx += 1;
        self.seek_to_idx(@intCast(self.idx));
    }

    pub fn prev(self: *BlockIterator) void {
        self.idx -= 1;
        if (self.idx < 0) return;
        self.seek_to_idx(@intCast(self.idx));
    }
};

test "BlockIterator: init_and_seek_to_first, next" {
    const test_gpa = std.testing.allocator;
    const BlockBuilder = @import("builder.zig").BlockBuilder;
    var builder = try BlockBuilder.init(64, test_gpa);
    _ = try builder.add("key1", "val1");
    _ = try builder.add("k2", "v2");
    const block = try builder.build();
    defer block.deinit();

    var it = BlockIterator.init_and_seek_to_first(&block);
    try std.testing.expect(it.is_valid());
    try std.testing.expectEqualStrings("key1", it.key());
    try std.testing.expectEqualStrings("val1", it.val());
    it.next();
    try std.testing.expect(it.is_valid());
    try std.testing.expectEqualStrings("k2", it.key());
    try std.testing.expectEqualStrings("v2", it.val());
    it.next();
    try std.testing.expect(!it.is_valid());

    it.prev();
    try std.testing.expect(it.is_valid());
    try std.testing.expectEqualStrings("k2", it.key());
    try std.testing.expectEqualStrings("v2", it.val());

    it.prev();
    try std.testing.expect(it.is_valid());
    try std.testing.expectEqualStrings("key1", it.key());
    try std.testing.expectEqualStrings("val1", it.val());
    it.prev();
    try std.testing.expect(!it.is_valid());
}

test "BlockIterator: init_and_seek_to_key, next" {
    const test_gpa = std.testing.allocator;
    const BlockBuilder = @import("builder.zig").BlockBuilder;
    var builder = try BlockBuilder.init(64, test_gpa);
    _ = try builder.add("a", "1");
    _ = try builder.add("c", "3");
    _ = try builder.add("d", "4");
    const block = try builder.build();
    defer block.deinit();

    var it_exact = BlockIterator.init_and_seek_to_key(&block, "c");
    try std.testing.expect(it_exact.is_valid());
    try std.testing.expectEqualStrings("c", it_exact.key());
    try std.testing.expectEqualStrings("3", it_exact.val());
    it_exact.next();
    try std.testing.expect(it_exact.is_valid());
    try std.testing.expectEqualStrings("d", it_exact.key());
    try std.testing.expectEqualStrings("4", it_exact.val());
    it_exact.next();
    try std.testing.expect(!it_exact.is_valid());

    var it_off = BlockIterator.init_and_seek_to_key(&block, "b");
    try std.testing.expect(it_off.is_valid());
    try std.testing.expectEqualStrings("c", it_off.key());
    try std.testing.expectEqualStrings("3", it_off.val());
    it_off.next();
    try std.testing.expect(it_off.is_valid());
    try std.testing.expectEqualStrings("d", it_off.key());
    try std.testing.expectEqualStrings("4", it_off.val());
    it_off.next();
    try std.testing.expect(!it_off.is_valid());
}
