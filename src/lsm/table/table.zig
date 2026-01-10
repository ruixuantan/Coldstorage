const std = @import("std");
const mem = std.mem;
const fs = std.fs;
const Block = @import("../block.zig").block.Block;
const SsTableIterator = @import("iterator.zig").SsTableIterator;
const Bloom = @import("bloom.zig").Bloom;
const Cache = @import("cache.zig").Cache;

// BlockMeta layout:
// --------------------------------------------------------------
// |   num_blocks: u32   | Block #1 | Block #2 | ... | Block #N |
// --------------------------------------------------------------
//

// ---------------------------------------------------------------------------------------------------------
// | Entry #1                                                                             | Entry #2 | ... |
// ---------------------------------------------------------------------------------------------------------
// | offset: u32 | key_len: u16 | first_key (keylen) | key_len: u16 | second_key (varlen) |  ...     | ... |
// ---------------------------------------------------------------------------------------------------------
pub const BlockMeta = struct {
    offset: usize,
    first_key: []const u8,
    last_key: []const u8,
    gpa: mem.Allocator,

    pub fn init(offset: usize, first_key: []const u8, last_key: []const u8, gpa: mem.Allocator) !BlockMeta {
        return .{
            .offset = offset,
            .first_key = try gpa.dupe(u8, first_key),
            .last_key = try gpa.dupe(u8, last_key),
            .gpa = gpa,
        };
    }

    pub fn deinit(self: BlockMeta) void {
        self.gpa.free(self.first_key);
        self.gpa.free(self.last_key);
    }

    pub fn encode_block_meta(block_metas: []const BlockMeta, gpa: mem.Allocator) ![]const u8 {
        var estimated_size: usize = @sizeOf(u32); // num_blocks
        for (block_metas) |meta| {
            estimated_size += @sizeOf(u32); // buffer offset is u32
            estimated_size += @sizeOf(u16) * 2; // first_key len and last_key len
            estimated_size += meta.first_key.len;
            estimated_size += meta.last_key.len;
        }
        var buffer = try std.ArrayListUnmanaged(u8).initCapacity(gpa, estimated_size);

        const num_blocks_bytes: []const u8 = @ptrCast(&@as(u32, @intCast(block_metas.len)));
        buffer.appendSliceAssumeCapacity(num_blocks_bytes);
        for (block_metas) |meta| {
            const offset_bytes: []const u8 = @ptrCast(&@as(u32, @intCast(meta.offset)));
            buffer.appendSliceAssumeCapacity(offset_bytes);

            const first_key_len_bytes: []const u8 = @ptrCast(&@as(u16, @intCast(meta.first_key.len)));
            buffer.appendSliceAssumeCapacity(first_key_len_bytes);
            buffer.appendSliceAssumeCapacity(meta.first_key);

            const last_key_len_bytes: []const u8 = @ptrCast(&@as(u16, @intCast(meta.last_key.len)));
            buffer.appendSliceAssumeCapacity(last_key_len_bytes);
            buffer.appendSliceAssumeCapacity(meta.last_key);
        }
        return try buffer.toOwnedSlice(gpa);
    }

    pub fn decode_block_meta(data: []const u8, gpa: mem.Allocator) ![]BlockMeta {
        const num_blocks: u32 = mem.bytesAsValue(u32, data[0..4]).*;
        var blocks = try std.ArrayList(BlockMeta).initCapacity(gpa, num_blocks);
        var pos: usize = 4;
        for (0..num_blocks) |_| {
            const offset: usize = @intCast(mem.bytesAsValue(u32, data[pos .. pos + 4]).*);
            pos += 4;

            const first_key_len: usize = @intCast(mem.bytesAsValue(u16, data[pos .. pos + 2]).*);
            pos += 2;
            const first_key: []const u8 = data[pos .. pos + first_key_len];
            pos += first_key_len;

            const last_key_len: usize = @intCast(mem.bytesAsValue(u16, data[pos .. pos + 2]).*);
            pos += 2;
            const last_key: []const u8 = data[pos .. pos + last_key_len];
            pos += last_key_len;

            try blocks.append(gpa, try BlockMeta.init(@as(usize, offset), first_key, last_key, gpa));
        }

        return try blocks.toOwnedSlice(gpa);
    }
};

pub const FileReader = struct {
    pub const buffer_size = 4096 * 2; // assume block metas can fit in here

    reader: fs.File.Reader,
    buffer: [buffer_size]u8,

    pub fn init(file: fs.File) FileReader {
        var buffer: [buffer_size]u8 = undefined;
        const file_reader = file.reader(&buffer);
        return .{ .reader = file_reader, .buffer = buffer };
    }

    pub fn seek_and_read(self: *FileReader, offset: usize, len: usize) []const u8 {
        self.reader.seekTo(offset) catch @panic("Offset out of range");
        return self.reader.interface.take(len) catch @panic("Failed to read");
    }
};

// -------------------------------------------------------
// |                   Block Section                     |
// -------------------------------------------------------
// | data block | checksum | ... | data block | checksum |
// |   varlen   |    u32   |     |   varlen   |    u32   |
// -------------------------------------------------------
// -------------------------------------------------------------------------------------------
// |                                     Meta Section                                        |
// -------------------------------------------------------------------------------------------
// | metadata | checksum | meta block offset | bloom filter | checksum | bloom filter offset |
// |  varlen  |    u32   |        u32        |    varlen    |    u32   |        u32          |
// -------------------------------------------------------------------------------------------

pub const SsTable = struct {
    pub const SsTableError = error{ DiskCorruptedBlock, DiskCorruptedBloom } || error{OutOfMemory};

    block_metas: []const BlockMeta,
    block_meta_offset: usize,
    id: usize,
    first_key: []const u8,
    last_key: []const u8,
    file: fs.File,
    file_reader: FileReader,
    sst_cache: *Cache,
    bloom: Bloom,
    gpa: mem.Allocator,

    pub fn get_sst_filename(id: usize, gpa: mem.Allocator) ![]const u8 {
        return try std.fmt.allocPrint(gpa, "sst_{d}.sst", .{id});
    }

    pub fn init(
        id: usize,
        block_metas: []const BlockMeta,
        block_meta_offset: usize,
        file: fs.File,
        bloom: Bloom,
        gpa: mem.Allocator,
    ) !SsTable {
        const cache = try gpa.create(Cache);
        cache.* = Cache.init(gpa, 64);
        return SsTable{
            .block_metas = block_metas,
            .block_meta_offset = block_meta_offset,
            .id = id,
            .first_key = block_metas[0].first_key,
            .last_key = block_metas[block_metas.len - 1].last_key,
            .file = file,
            .file_reader = FileReader.init(file),
            .sst_cache = cache,
            .bloom = bloom,
            .gpa = gpa,
        };
    }

    pub fn open(id: usize, file: fs.File, gpa: mem.Allocator) !SsTable {
        const end = try file.getEndPos();
        var file_reader = FileReader.init(file);
        const bloom_offset: usize = @intCast(
            mem.bytesAsValue(u32, file_reader.seek_and_read(end - 4, 4)).*,
        );
        const bloom_data = file_reader.seek_and_read(bloom_offset, end - 4 - bloom_offset - 4);
        const bloom_checksum = std.hash.Crc32.hash(bloom_data);
        const stored_bloom_checksum_slice = file_reader.seek_and_read(end - 4 - 4, 4);
        const stored_bloom_checksum: u32 = mem.bytesAsValue(u32, stored_bloom_checksum_slice).*;
        if (bloom_checksum != stored_bloom_checksum) return SsTableError.DiskCorruptedBloom;

        const bloom = try Bloom.decode(bloom_data, gpa);

        const block_meta_offset: usize = @intCast(
            mem.bytesAsValue(u32, file_reader.seek_and_read(bloom_offset - 4, 4)).*,
        );
        const block_metas = try BlockMeta.decode_block_meta(
            file_reader.seek_and_read(block_meta_offset, bloom_offset - 4 - block_meta_offset),
            gpa,
        );
        const cache = try gpa.create(Cache);
        cache.* = Cache.init(gpa, 64);
        return .{
            .id = id,
            .file = file,
            .file_reader = file_reader,
            .sst_cache = cache,
            .bloom = bloom,
            .gpa = gpa,
            .block_metas = block_metas,
            .block_meta_offset = block_meta_offset,
            .first_key = block_metas[0].first_key,
            .last_key = block_metas[block_metas.len - 1].last_key,
        };
    }

    pub fn close(self: *SsTable) void {
        for (self.block_metas) |bm| {
            bm.deinit();
        }
        self.gpa.free(self.block_metas);
        self.sst_cache.deinit();
        self.gpa.destroy(self.sst_cache);
        self.bloom.deinit();
        self.file.close();
    }

    pub fn num_blocks(self: SsTable) usize {
        return self.block_metas.len;
    }

    fn read_block(self: *SsTable, block_index: usize) SsTableError!Block {
        const offset = self.block_metas[block_index].offset;
        var end_offset = self.block_meta_offset;
        if (block_index + 1 < self.num_blocks()) {
            end_offset = self.block_metas[block_index + 1].offset;
        }
        const block_data = self.file_reader.seek_and_read(offset, end_offset - offset - 4);
        const checksum = std.hash.Crc32.hash(block_data);

        const stored_checksum_slice = self.file_reader.seek_and_read(end_offset - 4, 4);
        const stored_checksum: u32 = mem.bytesAsValue(u32, stored_checksum_slice).*;
        if (checksum != stored_checksum) return SsTableError.DiskCorruptedBlock;

        return try Block.decode(block_data, self.gpa);
    }

    pub fn read_block_cached(self: *SsTable, block_index: usize) SsTableError!Block {
        const cache_key = Cache.CacheKey{ .sst_id = self.id, .block_index = block_index };
        if (self.sst_cache.get(cache_key)) |cached_block| {
            return cached_block;
        } else {
            const block = try self.read_block(block_index);
            try self.sst_cache.put(cache_key, block);
            return block;
        }
    }

    pub fn find_block_index(self: *SsTable, key: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = self.num_blocks();

        while (lo < hi) {
            const mid: usize = lo + @divFloor((hi - lo), 2);
            const ord = mem.order(u8, self.block_metas[mid].first_key, key);
            switch (ord) {
                .eq => return mid,
                .lt => {
                    if (mem.order(u8, self.block_metas[mid].last_key, key) != .lt) {
                        return mid;
                    }
                    lo = mid + 1;
                },
                .gt => hi = mid,
            }
        }
        return lo;
    }

    pub fn scan(self: *SsTable, lower: []const u8, upper: []const u8) !?SsTableIterator {
        if (mem.order(u8, self.last_key, lower) == .lt or
            mem.order(u8, self.first_key, upper) == .gt)
        {
            return null;
        }
        return try SsTableIterator.create_and_seek_to_key(self, lower);
    }
};

test "BlockMeta: encode" {
    const test_gpa = std.testing.allocator;
    var block_metas = try std.ArrayList(BlockMeta).initCapacity(test_gpa, 2);
    defer block_metas.deinit(test_gpa);
    try block_metas.append(test_gpa, try BlockMeta.init(0, "apple", "banana", test_gpa));
    try block_metas.append(test_gpa, try BlockMeta.init(1234, "carrot", "date", test_gpa));
    const encoded = try BlockMeta.encode_block_meta(block_metas.items, test_gpa);
    defer {
        for (block_metas.items) |bm| {
            bm.deinit();
        }
        test_gpa.free(encoded);
    }

    const num_blocks: []const u8 = @ptrCast(&@as(u32, 2));
    const offset1: []const u8 = @ptrCast(&@as(u32, 0));
    const offset2: []const u8 = @ptrCast(&@as(u32, 1234));
    const apple_len: []const u8 = @ptrCast(&@as(u16, 5));
    const banana_len: []const u8 = @ptrCast(&@as(u16, 6));
    const carrot_len: []const u8 = @ptrCast(&@as(u16, 6));
    const date_len: []const u8 = @ptrCast(&@as(u16, 4));
    try std.testing.expectEqualSlices(u8, num_blocks, encoded[0..4]);
    try std.testing.expectEqualSlices(u8, offset1, encoded[4..8]);
    try std.testing.expectEqualSlices(u8, apple_len, encoded[8..10]);
    try std.testing.expectEqualSlices(u8, "apple", encoded[10..15]);
    try std.testing.expectEqualSlices(u8, banana_len, encoded[15..17]);
    try std.testing.expectEqualSlices(u8, "banana", encoded[17..23]);
    try std.testing.expectEqualSlices(u8, offset2, encoded[23..27]);
    try std.testing.expectEqualSlices(u8, carrot_len, encoded[27..29]);
    try std.testing.expectEqualSlices(u8, "carrot", encoded[29..35]);
    try std.testing.expectEqualSlices(u8, date_len, encoded[35..37]);
    try std.testing.expectEqualSlices(u8, "date", encoded[37..41]);
}

test "BlockMeta: decode" {
    const test_gpa = std.testing.allocator;
    const num_blocks: []const u8 = @ptrCast(&@as(u32, 2));
    const offset1: []const u8 = @ptrCast(&@as(u32, 0));
    const offset2: []const u8 = @ptrCast(&@as(u32, 1234));
    const apple_len: []const u8 = @ptrCast(&@as(u16, 5));
    const banana_len: []const u8 = @ptrCast(&@as(u16, 6));
    const carrot_len: []const u8 = @ptrCast(&@as(u16, 6));
    const date_len: []const u8 = @ptrCast(&@as(u16, 4));
    const data: []const u8 = num_blocks ++
        offset1 ++ apple_len ++ "apple" ++ banana_len ++ "banana" ++
        offset2 ++ carrot_len ++ "carrot" ++ date_len ++ "date";

    const decoded = try BlockMeta.decode_block_meta(data, test_gpa);
    defer {
        for (decoded) |bm| {
            bm.deinit();
        }
        test_gpa.free(decoded);
    }

    try std.testing.expectEqual(decoded.len, 2);
    try std.testing.expectEqual(decoded[0].offset, 0);
    try std.testing.expectEqualSlices(u8, "apple", decoded[0].first_key);
    try std.testing.expectEqualSlices(u8, "banana", decoded[0].last_key);
    try std.testing.expectEqual(decoded[1].offset, 1234);
    try std.testing.expectEqualSlices(u8, "carrot", decoded[1].first_key);
    try std.testing.expectEqualSlices(u8, "date", decoded[1].last_key);
}
