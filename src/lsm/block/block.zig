const std = @import("std");
const mem = std.mem;

pub const Block = struct {
    data: []const u8,
    offsets: []const u16,
    gpa: mem.Allocator,

    pub fn deinit(self: Block) void {
        self.gpa.free(self.data);
        self.gpa.free(self.offsets);
    }

    pub fn encode(self: Block, gpa: mem.Allocator) ![]const u8 {
        const capacity = self.data.len + self.offsets.len * 2 + 2;
        var data_builder = try std.ArrayList(u8).initCapacity(gpa, capacity);
        data_builder.appendSliceAssumeCapacity(self.data);
        data_builder.appendSliceAssumeCapacity(mem.sliceAsBytes(self.offsets));
        const num_elements_slice: []const u8 = @ptrCast(&@as(u16, @intCast(self.offsets.len)));
        data_builder.appendSliceAssumeCapacity(num_elements_slice);
        return try data_builder.toOwnedSlice(gpa);
    }

    pub fn decode(data: []const u8, gpa: mem.Allocator) !Block {
        const num_elements: u16 = mem.bytesAsValue(u16, data[data.len - 2 ..]).*;
        const offsets_start = data.len - 2 - (@as(usize, @intCast(num_elements)) * 2);
        const data_ls = try gpa.alloc(u8, data[0..offsets_start].len);
        @memcpy(data_ls, data[0..offsets_start]);
        const offsets = try gpa.alloc(u16, @divExact(data[offsets_start .. data.len - 2].len, 2));
        @memcpy(mem.sliceAsBytes(offsets), data[offsets_start .. data.len - 2]);
        return Block{
            .data = data_ls,
            .offsets = offsets,
            .gpa = gpa,
        };
    }
};

test "Block: encode" {
    const test_gpa = std.testing.allocator;
    const BlockBuilder = @import("builder.zig").BlockBuilder;
    var builder = try BlockBuilder.init(64, test_gpa);
    _ = try builder.add("key1", "val1");
    _ = try builder.add("k2", "v2");
    const block = try builder.build();
    defer block.deinit();

    const encoded = try block.encode(test_gpa);
    defer test_gpa.free(encoded);

    const len_4: []const u8 = @ptrCast(&@as(u16, 4));
    const len_2: []const u8 = @ptrCast(&@as(u16, 2));
    const offset_0: []const u8 = @ptrCast(&@as(u16, 0));
    const offset_12: []const u8 = @ptrCast(&@as(u16, 12));
    const block_data_slice: []const u8 = len_4 ++ "key1" ++ len_4 ++ "val1" ++ len_2 ++ "k2" ++ len_2 ++ "v2" ++ offset_0 ++ offset_12 ++ len_2;
    try std.testing.expectEqualSlices(u8, block_data_slice, encoded);
}

test "Block: decode" {
    const test_gpa = std.testing.allocator;
    const len_4: []const u8 = @ptrCast(&@as(u16, 4));
    const len_2: []const u8 = @ptrCast(&@as(u16, 2));
    const offset_0: []const u8 = @ptrCast(&@as(u16, 0));
    const offset_12: []const u8 = @ptrCast(&@as(u16, 12));
    const data: []const u8 = len_4 ++ "key1" ++ len_4 ++ "val1" ++ len_2 ++ "k2" ++ len_2 ++ "v2" ++ offset_0 ++ offset_12 ++ len_2;

    const block = try Block.decode(data, test_gpa);
    defer block.deinit();
    try std.testing.expectEqualSlices(u8, data[0 .. data.len - 6], block.data);
    try std.testing.expectEqualSlices(u16, &.{ 0, 12 }, block.offsets);
}
