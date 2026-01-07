const std = @import("std");
const Block = @import("block.zig").Block;
const Kv = @import("../kv.zig").Kv;

pub const BlockIterator = struct {
    block: *const Block,
    key_start_offset: usize,
    key_end_offset: usize,
    val_start_offset: usize,
    val_end_offset: usize,
    idx: usize, // index of the current entry, not byte offset

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
        std.debug.assert(target_idx < self.block.offsets.len);

        const offset: usize = @intCast(self.block.offsets[target_idx]);
        const key_len: usize = @intCast(std.mem.bytesAsValue(u16, self.block.data[offset .. offset + 2]).*);
        const val_offset = key_len + 2 + offset;
        const val_len: usize = @intCast(std.mem.bytesAsValue(u16, self.block.data[val_offset .. val_offset + 2]).*);

        self.key_start_offset = offset + 2;
        self.key_end_offset = self.key_start_offset + key_len;
        self.val_start_offset = val_offset + 2;
        self.val_end_offset = self.val_start_offset + val_len;
        self.idx = target_idx;
    }

    pub fn init_and_seek_to_first(block: *const Block) !BlockIterator {
        var it = BlockIterator.init(block);
        it.seek_to_idx(0);
        return it;
    }

    // will seek to self.key >= target_key
    pub fn init_and_seek_to_key(block: *const Block, target_key: []const u8) !BlockIterator {
        var it = BlockIterator.init(block);
        var lo: usize = 0;
        var hi: usize = block.offsets.len;

        while (lo < hi) {
            const mid: usize = lo + @divFloor((hi - lo), 2);
            it.seek_to_idx(mid);
            const ord = std.mem.order(u8, it.block.data[it.key_start_offset..it.key_end_offset], target_key);
            switch (ord) {
                .eq => return it,
                .lt => lo = mid + 1,
                .gt => hi = mid,
            }
        }
        it.seek_to_idx(lo);
        return it;
    }

    pub fn next(self: *BlockIterator) !?Kv {
        if (self.idx >= self.block.offsets.len) {
            return null;
        }
        self.seek_to_idx(self.idx);
        const key = self.block.data[self.key_start_offset..self.key_end_offset];
        const val = self.block.data[self.val_start_offset..self.val_end_offset];
        self.idx += 1;
        return Kv.init(key, val);
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

    var it = try BlockIterator.init_and_seek_to_first(&block);
    const first = try it.next();
    try std.testing.expectEqualStrings("key1", first.?.key);
    try std.testing.expectEqualStrings("val1", first.?.val);

    const second = try it.next();
    try std.testing.expectEqualStrings("k2", second.?.key);
    try std.testing.expectEqualStrings("v2", second.?.val);

    try std.testing.expectEqual(null, try it.next());
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

    var it_exact = try BlockIterator.init_and_seek_to_key(&block, "c");
    const c = try it_exact.next();
    try std.testing.expectEqualStrings("c", c.?.key);
    try std.testing.expectEqualStrings("3", c.?.val);
    const d = try it_exact.next();
    try std.testing.expectEqualStrings("d", d.?.key);
    try std.testing.expectEqualStrings("4", d.?.val);
    try std.testing.expectEqual(null, try it_exact.next());

    var it_off = try BlockIterator.init_and_seek_to_key(&block, "b");
    const c_off = try it_off.next();
    try std.testing.expectEqualStrings("c", c_off.?.key);
    try std.testing.expectEqualStrings("3", c_off.?.val);
    const d_off = try it_off.next();
    try std.testing.expectEqualStrings("d", d_off.?.key);
    try std.testing.expectEqualStrings("4", d_off.?.val);
    try std.testing.expectEqual(null, it_off.next());
}
