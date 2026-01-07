const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const table = @import("table.zig");
const BlockMeta = table.BlockMeta;
const SstTable = table.SsTable;
const BlockBuilder = @import("../block.zig").builder.BlockBuilder;
const Bloom = @import("bloom.zig").Bloom;

pub const SsTableBuilder = struct {
    builder: BlockBuilder,
    first_key: []const u8,
    last_key: []const u8,
    data: std.ArrayList(u8),
    meta: std.ArrayList(BlockMeta),
    block_size: usize,
    bloom_keys: std.ArrayList([]const u8),
    gpa: mem.Allocator,

    pub fn init(block_size: usize, gpa: mem.Allocator) !SsTableBuilder {
        return SsTableBuilder{
            .builder = try BlockBuilder.init(block_size, gpa),
            .first_key = try gpa.alloc(u8, 0),
            .last_key = try gpa.alloc(u8, 0),
            .data = try std.ArrayList(u8).initCapacity(gpa, 1),
            .meta = try std.ArrayList(BlockMeta).initCapacity(gpa, 1),
            .block_size = block_size,
            .bloom_keys = try std.ArrayList([]const u8).initCapacity(gpa, 1),
            .gpa = gpa,
        };
    }

    pub fn deinit(self: *SsTableBuilder) void {
        self.builder.deinit();
        self.data.deinit(self.gpa);
        self.meta.deinit(self.gpa);
        self.bloom_keys.deinit(self.gpa);
        self.gpa.free(self.first_key);
        self.gpa.free(self.last_key);
    }

    pub fn add(self: *SsTableBuilder, key: []const u8, val: []const u8) !void {
        if (self.meta.items.len == 0 and self.builder.is_empty()) {
            self.gpa.free(self.first_key);
            self.first_key = try self.gpa.dupe(u8, key);
        }
        const add_res = try self.builder.add(key, val);
        try self.bloom_keys.append(self.gpa, key);
        if (add_res) {
            self.gpa.free(self.last_key);
            self.last_key = try self.gpa.dupe(u8, key);
            return;
        }

        try self.build_block();
        const add_new_res = try self.builder.add(key, val);
        std.debug.assert(add_new_res);
        self.gpa.free(self.first_key);
        self.first_key = try self.gpa.dupe(u8, key);
        self.gpa.free(self.last_key);
        self.last_key = try self.gpa.dupe(u8, key);
    }

    pub fn estimated_size(self: SsTableBuilder) usize {
        var meta_size: usize = 0;
        for (self.meta.items) |m| {
            meta_size += 4 + 2 + 2 + m.first_key.len + m.last_key.len;
        }
        return self.data.items.len + meta_size + 4; // 4 bytes for num_blocks
    }

    fn build_block(self: *SsTableBuilder) !void {
        const block = try self.builder.build();
        defer block.deinit();
        self.builder = try BlockBuilder.init(self.block_size, self.gpa);
        const block_meta = try BlockMeta.init(
            self.data.items.len,
            self.first_key,
            self.last_key,
            self.gpa,
        );
        try self.meta.append(self.gpa, block_meta);
        const encoded_block = try block.encode(self.gpa);
        defer self.gpa.free(encoded_block);
        try self.data.appendSlice(self.gpa, encoded_block);

        const checksum = std.hash.Crc32.hash(encoded_block);
        const checksum_slice: []const u8 = @ptrCast(&@as(u32, @intCast(checksum)));
        try self.data.appendSlice(self.gpa, checksum_slice);
    }

    pub fn build(self: *SsTableBuilder, id: usize, file: fs.File) !SstTable {
        defer self.deinit();
        try self.build_block();
        const block_meta_offset = self.data.items.len;

        const encoded_meta = try BlockMeta.encode_block_meta(self.meta.items, self.gpa);
        defer self.gpa.free(encoded_meta);
        try self.data.appendSlice(self.gpa, encoded_meta);

        const block_meta_bytes: []const u8 = @ptrCast(&@as(u32, @intCast(block_meta_offset)));
        try self.data.appendSlice(self.gpa, block_meta_bytes);

        const bloom_offset = self.data.items.len;
        const bits_per_key = Bloom.bloom_bits_per_key(0.01);
        const bloom = try Bloom.build_from_keys(self.bloom_keys.items, bits_per_key, self.gpa);
        const encoded_bloom = try bloom.encode(self.gpa);
        defer self.gpa.free(encoded_bloom);
        try self.data.appendSlice(self.gpa, encoded_bloom);

        const bloom_checksum = std.hash.Crc32.hash(encoded_bloom);
        const bloom_checksum_slice: []const u8 = @ptrCast(&@as(u32, @intCast(bloom_checksum)));
        try self.data.appendSlice(self.gpa, bloom_checksum_slice);

        const bloom_offset_bytes: []const u8 = @ptrCast(&@as(u32, @intCast(bloom_offset)));
        try self.data.appendSlice(self.gpa, bloom_offset_bytes);
        const data_bytes = try self.data.toOwnedSlice(self.gpa);

        var file_writer = file.writer(&.{});
        try file_writer.interface.writeAll(data_bytes);
        self.gpa.free(data_bytes);

        return SstTable.init(
            id,
            try self.meta.toOwnedSlice(self.gpa),
            block_meta_offset,
            file,
            bloom,
            self.gpa,
        );
    }
};

test "SstTableBuilder: add, build, estimated_size" {
    const test_gpa = std.testing.allocator;
    var builder = try SsTableBuilder.init(64, test_gpa);
    try builder.add("key1", "val1");
    try builder.add("key2", "val2");
    try builder.add("key3", "val3");

    const file = try fs.cwd().createFile("sst_table_builder_test.sst", .{});
    defer fs.cwd().deleteTree("sst_table_builder_test.sst") catch {};
    var sst_table = try builder.build(0, file);
    defer sst_table.close();
    try std.testing.expectEqualStrings("key1", sst_table.first_key);
    try std.testing.expectEqualStrings("key3", sst_table.last_key);
    try std.testing.expectEqual(48, sst_table.block_meta_offset);
    try std.testing.expectEqual(1, sst_table.block_metas.len);
}
