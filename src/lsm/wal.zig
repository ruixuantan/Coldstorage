const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const Skiplist = @import("skiplist.zig").DefaultSkiplist;

// layout: key_len(u16) | key | value_len(u16) | value

pub const Wal = struct {
    const file_prefix = "wal_";

    file: fs.File,
    writer: fs.File.Writer,

    pub fn get_wal_filename(id: usize, gpa: mem.Allocator) ![]const u8 {
        return try std.fmt.allocPrint(gpa, "wal_{d}.wal", .{id});
    }

    pub fn list_wal_file_ids(dir: *fs.Dir, gpa: mem.Allocator) ![]usize {
        var file_itr = dir.iterate();
        var wal_files = try std.ArrayList(usize).initCapacity(gpa, 1);
        while (try file_itr.next()) |entry| {
            if (!std.mem.eql(u8, entry.name[0..file_prefix.len], file_prefix)) continue;
            var end_idx: usize = file_prefix.len + 1;
            while (entry.name[end_idx] != '.') : (end_idx += 1) {}
            try wal_files.append(
                gpa,
                try std.fmt.parseInt(usize, entry.name[file_prefix.len..end_idx], 10),
            );
        }
        return try wal_files.toOwnedSlice(gpa);
    }

    pub fn create(dir: *fs.Dir, id: usize, gpa: mem.Allocator) !Wal {
        const filename = try get_wal_filename(id, gpa);
        defer gpa.free(filename);
        var file: fs.File = undefined;
        if (dir.openFile(filename, .{ .mode = .read_write })) |opened_file| {
            file = opened_file;
        } else |err| {
            if (err != error.FileNotFound) @panic("Failed to open wal file");
            file = dir.createFile(filename, .{}) catch @panic("Failed to create wal file");
        }
        var buffer: [4096]u8 = undefined;
        const writer = file.writer(&buffer);
        return Wal{ .file = file, .writer = writer };
    }

    pub fn recover(dir: *fs.Dir, id: usize, gpa: mem.Allocator, skiplist: *Skiplist) !Wal {
        const wal = try Wal.create(dir, id, gpa);
        var read_buffer: [4096]u8 = undefined;
        var reader = wal.file.reader(&read_buffer);

        var bytes = reader.interface.take(2);
        while (bytes) |key_len_bytes| {
            const key_len: u16 = mem.bytesAsValue(u16, key_len_bytes).*;
            const key = try reader.interface.take(@as(usize, @intCast(key_len)));
            const value_len_bytes = try reader.interface.take(2);
            const value_len: u16 = mem.bytesAsValue(u16, value_len_bytes).*;
            const value = try reader.interface.take(@as(usize, @intCast(value_len)));
            try skiplist.put(key, value);
            bytes = reader.interface.take(2);
        } else |err| {
            if (err != error.EndOfStream) return err;
        }
        return wal;
    }

    pub fn deinit(self: Wal) void {
        self.file.close();
    }

    pub fn put(self: *Wal, key: []const u8, value: []const u8) !void {
        const key_size: []const u8 = @ptrCast(&@as(u16, @intCast(key.len)));
        try self.writer.interface.writeAll(key_size);
        try self.writer.interface.writeAll(key);
        const value_size: []const u8 = @ptrCast(&@as(u16, @intCast(value.len)));
        try self.writer.interface.writeAll(value_size);
        try self.writer.interface.writeAll(value);
        try self.writer.interface.flush();
    }
};

test "Wal: create, put, recover" {
    const test_gpa = std.testing.allocator;
    try fs.cwd().makeDir("test_wal");
    var dir = try fs.cwd().openDir("test_wal", .{});
    defer fs.cwd().deleteTree("test_wal") catch {};

    const wal_id: usize = 1;
    var wal = try Wal.create(&dir, wal_id, test_gpa);
    try wal.put("key1", "value1");
    try wal.put("key2", "value2");
    try wal.file.sync();
    wal.deinit();
    dir.close();

    var skiplist = try Skiplist.init(test_gpa);
    defer skiplist.deinit();

    var new_dir = try fs.cwd().openDir("test_wal", .{});
    const recovered_wal = try Wal.recover(&new_dir, wal_id, test_gpa, &skiplist);
    defer recovered_wal.deinit();

    const val1 = skiplist.get("key1");
    try std.testing.expectEqualSlices(u8, "value1", val1.?);

    const val2 = skiplist.get("key2");
    try std.testing.expectEqualSlices(u8, "value2", val2.?);
}
