const std = @import("std");
const Block = @import("block.zig").Block;

pub const BlockBuilder = struct {
    data: std.ArrayList(u8),
    offsets: std.ArrayList(u16),
    block_size: usize,
    first_key: []const u8 = undefined,
    gpa: std.mem.Allocator,

    pub fn init(block_size: usize, gpa: std.mem.Allocator) !BlockBuilder {
        return BlockBuilder{
            .data = try std.ArrayList(u8).initCapacity(gpa, 1),
            .offsets = try std.ArrayList(u16).initCapacity(gpa, 1),
            .block_size = block_size,
            .gpa = gpa,
        };
    }

    // no need to call if build() is called
    pub fn deinit(self: *BlockBuilder) void {
        self.data.deinit(self.gpa);
        self.offsets.deinit(self.gpa);
    }

    // in bytes
    fn size(self: BlockBuilder) usize {
        return self.data.items.len + (self.offsets.items.len) * 2 + 2;
    }

    pub fn add(self: *BlockBuilder, key: []const u8, val: []const u8) !bool {
        if (self.size() + key.len + val.len + 6 > self.block_size) { // 6 = 2 (key_len) + 2 (val_len) + 2 (offset entry)
            return false;
        }
        if (self.offsets.items.len == 0) {
            self.first_key = key;
        }
        try self.offsets.append(self.gpa, @as(u16, @intCast(self.data.items.len)));
        const key_len: []const u8 = @ptrCast(&@as(u16, @intCast(key.len)));
        try self.data.appendSlice(self.gpa, key_len);
        try self.data.appendSlice(self.gpa, key);
        const val_len: []const u8 = @ptrCast(&@as(u16, @intCast(val.len)));
        try self.data.appendSlice(self.gpa, val_len);
        try self.data.appendSlice(self.gpa, val);
        return true;
    }

    pub fn is_empty(self: BlockBuilder) bool {
        return self.offsets.items.len == 0;
    }

    pub fn build(self: *BlockBuilder) !Block {
        std.debug.assert(!self.is_empty());
        return .{ .data = try self.data.toOwnedSlice(self.gpa), .offsets = try self.offsets.toOwnedSlice(self.gpa), .gpa = self.gpa };
    }
};

test "BlockBuilder: add, build" {
    const test_gpa = std.testing.allocator;
    var builder = try BlockBuilder.init(64, test_gpa);
    try std.testing.expect(try builder.add("key1", "val1"));
    try std.testing.expect(try builder.add("k2", "v2"));
    const block = try builder.build();
    defer block.deinit();

    const len_4: []const u8 = @ptrCast(&@as(u16, 4));
    const len_2: []const u8 = @ptrCast(&@as(u16, 2));
    const block_data_slice: []const u8 = len_4 ++ "key1" ++ len_4 ++ "val1" ++ len_2 ++ "k2" ++ len_2 ++ "v2";
    try std.testing.expectEqual(block_data_slice.len, block.data.len);
    try std.testing.expectEqualSlices(u8, block.data, block_data_slice);

    const bytes_0: []const u16 = @ptrCast(&@as(u16, 0));
    const bytes_12: []const u16 = @ptrCast(&@as(u16, 12));
    const offsets_slice: []const u16 = bytes_0 ++ bytes_12;
    try std.testing.expectEqual(offsets_slice.len, block.offsets.len);
    try std.testing.expectEqualSlices(u16, offsets_slice, block.offsets);
}
