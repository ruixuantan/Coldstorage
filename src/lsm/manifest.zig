const std = @import("std");
const fs = std.fs;

// encoding:
// <level><sst_id><level><sst_id> ...

pub const ManifestRecord = struct {
    level: u8,
    sst_id: u32,

    pub const record_size = 1 + 4; // level + sst_id
};

pub const Manifest = struct {
    pub const filename = "MANIFEST";

    file: fs.File,
    writer: fs.File.Writer,

    pub fn create(dir: *fs.Dir) Manifest {
        var file: fs.File = undefined;
        if (dir.openFile(filename, .{ .mode = .write_only })) |opened_file| {
            file = opened_file;
        } else |err| {
            if (err != error.FileNotFound) @panic("Failed to open manifest file");
            file = dir.createFile(filename, .{}) catch @panic("Failed to create manifest file");
        }
        return Manifest{ .file = file, .writer = file.writer(&.{}) };
    }

    pub fn recover(dir: *fs.Dir, gpa: std.mem.Allocator) ![]const ManifestRecord {
        var records = try std.ArrayList(ManifestRecord).initCapacity(gpa, 1);
        const file = dir.openFile(filename, .{ .mode = .read_only }) catch |err| {
            switch (err) {
                error.FileNotFound => return try records.toOwnedSlice(gpa),
                else => return err,
            }
        };
        defer file.close();
        var read_buffer: [4096]u8 = undefined;
        var reader = file.reader(&read_buffer);
        while (reader.interface.take(ManifestRecord.record_size)) |bytes| {
            const level = bytes[0];
            const sst_id: u32 = std.mem.bytesAsValue(u32, bytes[1..5]).*;
            try records.append(gpa, .{ .level = level, .sst_id = sst_id });
        } else |err| {
            if (err != error.EndOfStream) return err;
        }
        return try records.toOwnedSlice(gpa);
    }

    pub fn deinit(self: Manifest) void {
        self.file.close();
    }

    pub fn append_record(self: *Manifest, record: ManifestRecord) !void {
        try self.writer.interface.writeByte(record.level);
        const sst_id_slice: []const u8 = @ptrCast(&@as(u32, @intCast(record.sst_id)));
        try self.writer.interface.writeAll(sst_id_slice);
        try self.writer.interface.flush();
        try self.file.sync();
    }
};

test "Manifest: create, appendRecord, recover" {
    const test_gpa = std.testing.allocator;
    try fs.cwd().makeDir("test_manifest");
    var dir = try fs.cwd().openDir("test_manifest", .{});
    defer {
        dir.close();
        fs.cwd().deleteTree("test_manifest") catch {};
    }

    var manifest = Manifest.create(&dir);
    defer manifest.deinit();
    try manifest.append_record(.{ .level = 0, .sst_id = 42 });
    try manifest.append_record(.{ .level = 1, .sst_id = 84 });
    const records = try Manifest.recover(&dir, test_gpa);
    defer test_gpa.free(records);

    try std.testing.expectEqual(records.len, 2);
    try std.testing.expectEqual(records[0].level, 0);
    try std.testing.expectEqual(records[0].sst_id, 42);
    try std.testing.expectEqual(records[1].level, 1);
    try std.testing.expectEqual(records[1].sst_id, 84);
}
