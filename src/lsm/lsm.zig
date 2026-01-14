const std = @import("std");
const mem = std.mem;
const fs = std.fs;

const memtable = @import("memtable.zig");
const Memtable = memtable.Memtable;
const SsTable = @import("table.zig").SsTable;
const SsTableBuilder = @import("table/builder.zig").SsTableBuilder;

const MemtableIterator = memtable.Memtable.MemtableIterator;
const Iterator = @import("iterator.zig").Iterator;
const MergeIterator = @import("iterator.zig").MergeIterator;
const TwoMergeIterator = @import("iterator.zig").TwoMergeIterator;
const ConcatIterator = @import("iterator.zig").ConcatIterator;
const LsmIterator = @import("lsm_iterator.zig").LsmIterator;
const SsTableIterator = @import("table.zig").SsTableIterator;

const CompactionTask = @import("compact.zig").CompactionTask;
const CompactionOptions = @import("compact.zig").CompactionOptions;
const CompactionController = @import("compact.zig").CompactionController;
const DummyCompactionController = @import("compact.zig").DummyCompactionController;

const Manifest = @import("manifest.zig").Manifest;
const ManifestRecord = @import("manifest.zig").ManifestRecord;
const Wal = @import("wal.zig").Wal;

pub const LsmStorageOptions = struct {
    block_size: usize = 1024 * 1024 * 4, // 4MB
    target_sst_size: usize = 1024 * 1024 * 128, // 128 MB
    num_memtable_limit: usize = 2,
    compaction_options: CompactionOptions = .{ .simple_leveled = .{
        .size_ratio_percent = 800,
        .l0_file_num_compaction_trigger = 8,
        .max_lvls = 8,
    } },
    enable_wal: bool = false,
};

pub const LsmStorageState = struct {
    memtable: Memtable,
    imm_memtables: std.ArrayListUnmanaged(Memtable),
    l0: std.ArrayList(*SsTable),
    levels: std.ArrayList(std.ArrayList(*SsTable)),
    path: []const u8,
    next_sst_id: usize = 0,
    dir: fs.Dir,
    manifest: Manifest,
    gpa: std.mem.Allocator,

    pub fn init(
        options: LsmStorageOptions,
        path: []const u8,
        gpa: mem.Allocator,
    ) !LsmStorageState {
        var imm_memtables = try std.ArrayListUnmanaged(Memtable).initCapacity(
            gpa,
            options.num_memtable_limit,
        );
        var l0 = try std.ArrayList(*SsTable).initCapacity(gpa, 1);
        var levels = try std.ArrayList(std.ArrayList(*SsTable)).initCapacity(gpa, 1);
        const l1 = try std.ArrayList(*SsTable).initCapacity(gpa, 1);
        try levels.append(gpa, l1);
        fs.cwd().makePath(path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        var dir = try fs.cwd().openDir(path, .{});
        var manifest: Manifest = Manifest.create(&dir);
        var next_sst_id: usize = 0;
        var mt = try Memtable.init(undefined, options.target_sst_size, gpa);
        try recover(&dir, &mt, &l0, &levels, &next_sst_id, &manifest, gpa);
        if (options.enable_wal) try recover_wal(&options, &dir, &mt, &imm_memtables, gpa);

        return .{
            .memtable = mt,
            .imm_memtables = imm_memtables,
            .l0 = l0,
            .levels = levels,
            .path = path,
            .dir = dir,
            .manifest = manifest,
            .next_sst_id = next_sst_id + 1,
            .gpa = gpa,
        };
    }

    fn recover(
        dir: *fs.Dir,
        mt: *Memtable,
        l0: *std.ArrayList(*SsTable),
        levels: *std.ArrayList(std.ArrayList(*SsTable)),
        next_sst_id: *usize,
        manifest: *Manifest,
        gpa: mem.Allocator,
    ) !void {
        const manifest_records = try Manifest.recover(dir, gpa);
        defer gpa.free(manifest_records);

        for (manifest_records) |record| {
            const filename = try std.fmt.allocPrint(gpa, "sst_{d}.sst", .{record.sst_id});
            defer gpa.free(filename);
            dir.access(filename, .{ .mode = .read_only }) catch |err| {
                if (err != error.FileNotFound) return err;
                continue;
            };

            const file = try dir.openFile(filename, .{ .mode = .read_only });
            const sst = try gpa.create(SsTable);
            sst.* = try SsTable.open(record.sst_id, file, gpa);
            switch (record.level) {
                0 => try l0.append(gpa, sst),
                else => {
                    const level_index: usize = @intCast(record.level);
                    std.debug.assert(level_index >= 1);
                    for (0..level_index - levels.items.len) |_| {
                        const new_level = try std.ArrayList(*SsTable).initCapacity(gpa, 1);
                        try levels.append(gpa, new_level);
                    }
                    try levels.items[level_index - 1].append(gpa, sst);
                },
            }
            try manifest.append_record(record);
            next_sst_id.* = @max(next_sst_id.*, record.sst_id + 1);
        }
        try manifest.file.sync();
        mt.id = next_sst_id.*;
    }

    fn recover_wal(
        options: *const LsmStorageOptions,
        dir: *fs.Dir,
        mt: *Memtable,
        imm_memtables: *std.ArrayListUnmanaged(Memtable),
        gpa: mem.Allocator,
    ) !void {
        const wal_files = try Wal.list_wal_file_ids(dir, gpa);
        defer gpa.free(wal_files);
        std.mem.sort(usize, wal_files, {}, comptime std.sort.desc(usize));

        if (wal_files.len > 1) {
            for (wal_files[1..]) |index| {
                var imm_mt = try Memtable.init(index, options.target_sst_size, gpa);
                imm_mt.wal = try Wal.recover(dir, index, gpa, &imm_mt.skiplist);
                imm_memtables.appendAssumeCapacity(imm_mt);
            }
        }
        if (wal_files.len > 0) {
            mt.wal = try Wal.recover(dir, wal_files[0], gpa, &mt.skiplist);
        } else {
            // no wal files detected
            mt.wal = try Wal.create(dir, 0, gpa);
        }
    }

    pub fn deinit(self: *LsmStorageState) void {
        self.memtable.deinit();
        for (self.imm_memtables.items) |m| {
            m.deinit();
        }
        self.imm_memtables.deinit(self.gpa);
        for (self.l0.items) |sst| {
            sst.close();
            self.gpa.destroy(sst);
        }
        for (self.levels.items) |*level| {
            for (level.items) |sst| {
                sst.close();
                self.gpa.destroy(sst);
            }
            level.deinit(self.gpa);
        }
        self.levels.deinit(self.gpa);
        self.l0.deinit(self.gpa);
        self.manifest.deinit();
        self.dir.close();
    }

    pub fn get_next_sst_id(self: *LsmStorageState) usize {
        const id = self.next_sst_id;
        self.next_sst_id += 1;
        return id;
    }

    pub fn delete_sst_file(self: *LsmStorageState, sst: *SsTable) !void {
        const id = sst.id;
        sst.close();
        const filename = try SsTable.get_sst_filename(id, self.gpa);
        defer self.gpa.free(filename);
        try self.dir.deleteFile(filename);
    }

    pub fn append_empty_level(self: *LsmStorageState) !void {
        const new_level = try std.ArrayList(*SsTable).initCapacity(self.gpa, 1);
        try self.levels.append(self.gpa, new_level);
    }
};

pub const LsmStorageInner = struct {
    state: LsmStorageState,
    options: LsmStorageOptions,
    compaction_controller: CompactionController,
    gpa: mem.Allocator,

    pub fn open(path: []const u8, options: LsmStorageOptions, gpa: mem.Allocator) !LsmStorageInner {
        const state = try LsmStorageState.init(options, path, gpa);
        return LsmStorageInner{
            .options = options,
            .state = state,
            .compaction_controller = CompactionController.from_options(options.compaction_options),
            .gpa = gpa,
        };
    }

    pub fn close(self: *LsmStorageInner) !void {
        if (self.state.memtable.size() > 0) {
            try self.freeze_memtable();
        }
        for (self.state.imm_memtables.items) |_| {
            try self.force_flush_next_imm_memtable();
        }
        try self.sync();
        self.state.deinit();
    }

    fn sync(self: *LsmStorageInner) !void {
        try self.state.manifest.file.sync();
        for (self.state.l0.items) |sst| {
            try sst.file.sync();
        }
        for (self.state.levels.items) |*level| {
            for (level.items) |sst| {
                try sst.file.sync();
            }
        }

        if (self.options.enable_wal) {
            if (self.state.memtable.size() > 0) {
                try self.state.memtable.wal.?.file.sync();
            }
            for (self.state.imm_memtables.items) |*mt| {
                try mt.wal.?.file.sync();
            }
        }
    }

    fn buf_print(val: []const u8, buf: []u8) []const u8 {
        @memcpy(buf[0..val.len], val);
        return buf[0..val.len];
    }

    pub fn get(self: LsmStorageInner, key: []const u8, buf: []u8) !?[]const u8 {
        if (self.state.memtable.get(key)) |val| {
            return if (val.len == 0) null else buf_print(val, buf);
        }
        for (self.state.imm_memtables.items) |m| {
            if (m.get(key)) |val| {
                return if (val.len == 0) null else buf_print(val, buf);
            }
        }

        var l0_itrs = try std.ArrayList(Iterator).initCapacity(self.gpa, self.state.l0.items.len);
        for (self.state.l0.items) |sst| {
            if (!sst.bloom.may_contain(key)) continue;
            const itr = try sst.scan(key, key);
            if (itr == null) continue;
            const itr_ptr = try self.gpa.create(SsTableIterator);
            itr_ptr.* = itr.?;
            l0_itrs.appendAssumeCapacity(Iterator{ .ss_table_iterator = itr_ptr });
        }
        var l0_merge_itr = try MergeIterator.init(self.gpa, try l0_itrs.toOwnedSlice(self.gpa));
        defer l0_merge_itr.deinit();
        if (l0_merge_itr.is_valid()) {
            const k = l0_merge_itr.key();
            const v = l0_merge_itr.val();
            if (std.mem.eql(u8, k, key)) {
                return if (v.len == 0) null else buf_print(v, buf);
            }
        }

        std.debug.assert(self.state.levels.items.len >= 1);
        for (self.state.levels.items) |level| {
            var concat_itr = try ConcatIterator.create_and_seek_to_key(level.items, key);
            if (concat_itr.is_valid()) {
                const k = concat_itr.key();
                const v = concat_itr.val();
                if (std.mem.eql(u8, k, key)) {
                    return if (v.len == 0) null else buf_print(v, buf);
                }
            }
        }
        return null;
    }

    pub fn put(self: *LsmStorageInner, key: []const u8, val: []const u8) !void {
        if (self.state.memtable.willBeFull(key, val)) {
            try self.freeze_memtable();
        }
        try self.state.memtable.put(key, val);

        if (self.state.imm_memtables.items.len >= self.options.num_memtable_limit - 1) {
            try self.force_flush_next_imm_memtable();
        }
    }

    pub fn del(self: *LsmStorageInner, key: []const u8) !void {
        try self.put(key, "");
    }

    // [lower, upper)
    pub fn scan(self: LsmStorageInner, lower: []const u8, upper: []const u8) !LsmIterator {
        var mem_itrs = try std.ArrayList(Iterator).initCapacity(
            self.gpa,
            1 + self.state.imm_memtables.items.len,
        );
        const mem_itr_ptr = try self.gpa.create(MemtableIterator);
        mem_itr_ptr.* = self.state.memtable.scan(lower, upper);
        mem_itrs.appendAssumeCapacity(Iterator{ .memtable_iterator = mem_itr_ptr });
        for (self.state.imm_memtables.items) |m| {
            const frozen_mem_itr_ptr = try self.gpa.create(MemtableIterator);
            frozen_mem_itr_ptr.* = m.scan(lower, upper);
            mem_itrs.appendAssumeCapacity(Iterator{ .memtable_iterator = frozen_mem_itr_ptr });
        }
        const mem_merge_iter_ptr = try self.gpa.create(MergeIterator);
        mem_merge_iter_ptr.* = try MergeIterator.init(
            self.gpa,
            try mem_itrs.toOwnedSlice(self.gpa),
        );

        var l0_itrs = try std.ArrayList(Iterator).initCapacity(self.gpa, self.state.l0.items.len);
        for (self.state.l0.items) |sst| {
            if (try sst.scan(lower, upper)) |itr| {
                const itr_ptr = try self.gpa.create(SsTableIterator);
                itr_ptr.* = itr;
                l0_itrs.appendAssumeCapacity(Iterator{ .ss_table_iterator = itr_ptr });
            }
        }
        const l0_merge_itr_ptr = try self.gpa.create(MergeIterator);
        l0_merge_itr_ptr.* = try MergeIterator.init(self.gpa, try l0_itrs.toOwnedSlice(self.gpa));

        const mem_l0_itr = try self.gpa.create(TwoMergeIterator);
        mem_l0_itr.* = try TwoMergeIterator.init(
            Iterator{ .merge_iterator = mem_merge_iter_ptr },
            Iterator{ .merge_iterator = l0_merge_itr_ptr },
            self.gpa,
        );

        std.debug.assert(self.state.levels.items.len >= 1);
        var ln_merge_itrs = try std.ArrayList(*ConcatIterator).initCapacity(self.gpa, self.state.levels.items.len);
        defer ln_merge_itrs.deinit(self.gpa);
        for (self.state.levels.items) |level| {
            const level_concat_itr_ptr = try self.gpa.create(ConcatIterator);
            level_concat_itr_ptr.* = try ConcatIterator.create_and_seek_to_key(level.items, lower);
            ln_merge_itrs.appendAssumeCapacity(level_concat_itr_ptr);
        }

        var main_itr = try TwoMergeIterator.init(
            Iterator{ .two_merge_iterator = mem_l0_itr },
            Iterator{ .concat_iterator = ln_merge_itrs.items[0] },
            self.gpa,
        );
        for (ln_merge_itrs.items[1..]) |level_itr_ptr| {
            const main_itr_ptr = try self.gpa.create(TwoMergeIterator);
            main_itr_ptr.* = main_itr;
            const new_main_itr = try TwoMergeIterator.init(
                Iterator{ .two_merge_iterator = main_itr_ptr },
                Iterator{ .concat_iterator = level_itr_ptr },
                self.gpa,
            );
            main_itr = new_main_itr;
        }
        return LsmIterator.init(main_itr, upper);
    }

    pub fn freeze_memtable(self: *LsmStorageInner) !void {
        std.debug.assert(self.state.memtable.size() > 0);
        const old_memtable = self.state.memtable;
        try self.state.imm_memtables.insert(self.gpa, 0, old_memtable);
        const next_sst_id = self.state.get_next_sst_id();
        self.state.memtable = try Memtable.init(
            next_sst_id,
            self.options.target_sst_size,
            self.state.gpa,
        );
        if (self.options.enable_wal) {
            self.state.memtable.wal = try Wal.create(&self.state.dir, next_sst_id, self.state.gpa);
        }
    }

    fn build_sst(self: *LsmStorageInner, builder: *SsTableBuilder, id: usize) !SsTable {
        const filename = try SsTable.get_sst_filename(id, self.gpa);
        defer self.gpa.free(filename);

        const file = try self.state.dir.createFile(filename, .{ .read = true });
        const sst = try builder.build(id, file);
        try sst.file.sync();
        return sst;
    }

    fn force_flush_next_imm_memtable(self: *LsmStorageInner) !void {
        std.debug.assert(self.state.imm_memtables.items.len > 0);
        var to_flush = self.state.imm_memtables.pop().?;
        defer to_flush.deinit();
        var sst_builder = try SsTableBuilder.init(self.options.block_size, self.gpa);
        try to_flush.flush(&sst_builder);
        const sst = try self.gpa.create(SsTable);
        sst.* = try self.build_sst(&sst_builder, to_flush.id);
        if (self.options.enable_wal) {
            try to_flush.delete_wal(&self.state.dir);
        }
        try self.state.l0.append(self.gpa, sst);
        try self.state.manifest.append_record(.{ .level = 0, .sst_id = @intCast(sst.id) });
        try self.sync();

        try self.trigger_compaction();
    }

    fn trigger_compaction(self: *LsmStorageInner) !void {
        var task = try self.compaction_controller.init_compaction_task(&self.state) orelse return;
        defer task.deinit(self.gpa);
        const compact_results = try self.compact(task);
        defer self.gpa.free(compact_results);

        try self.compaction_controller.apply_compaction_result(
            &self.state,
            task,
            compact_results,
        );
        try self.sync();
    }

    fn compact_from_iter(self: *LsmStorageInner, itr: *Iterator) ![]const *SsTable {
        defer {
            itr.deinit(self.gpa);
            self.gpa.destroy(itr);
        }
        var new_ssts = try std.ArrayList(*SsTable).initCapacity(self.gpa, 1);
        var sst_builder = try SsTableBuilder.init(self.options.block_size, self.gpa);

        while (itr.is_valid()) {
            const k = itr.key();
            const v = itr.val();
            if (sst_builder.estimated_size() >= self.options.target_sst_size) {
                const next_sst_id = self.state.get_next_sst_id();
                const sst = try self.gpa.create(SsTable);
                sst.* = try self.build_sst(&sst_builder, next_sst_id);
                try new_ssts.append(self.gpa, sst);
                sst_builder = try SsTableBuilder.init(self.options.block_size, self.gpa);
            }
            try sst_builder.add(k, v);
            try itr.next();
        }
        const next_sst_id = self.state.get_next_sst_id();
        const sst = try self.gpa.create(SsTable);
        sst.* = try self.build_sst(&sst_builder, next_sst_id);
        try new_ssts.append(self.gpa, sst);
        return try new_ssts.toOwnedSlice(self.gpa);
    }

    fn compact(self: *LsmStorageInner, task: CompactionTask) ![]const *SsTable {
        switch (task) {
            .simple_leveled => |t| {
                var itrs = try std.ArrayList(Iterator).initCapacity(
                    self.gpa,
                    t.upper_level_ssts.len,
                );
                for (t.upper_level_ssts) |sst| {
                    const itr = try self.gpa.create(SsTableIterator);
                    itr.* = try SsTableIterator.create_and_seek_to_first(sst);
                    itrs.appendAssumeCapacity(Iterator{ .ss_table_iterator = itr });
                }
                const merge_itr = try self.gpa.create(MergeIterator);
                merge_itr.* = try MergeIterator.init(self.gpa, try itrs.toOwnedSlice(self.gpa));
                const base_itr = try self.gpa.create(Iterator);
                base_itr.* = .{ .merge_iterator = merge_itr };
                return try self.compact_from_iter(base_itr);
            },
        }
        unreachable;
    }
};

test "LsmStorageInner: put, get, del" {
    const test_gpa = std.testing.allocator;
    const test_options = LsmStorageOptions{
        .block_size = 4096,
        .target_sst_size = 8,
        .num_memtable_limit = 4,
        .compaction_options = .{ .simple_leveled = .{
            .size_ratio_percent = 200,
            .l0_file_num_compaction_trigger = 2,
            .max_lvls = 3,
        } },
    };
    var lsm = try LsmStorageInner.open("testdb/data", test_options, test_gpa);
    defer {
        lsm.close() catch @panic("Failed to close LSM");
        fs.cwd().deleteTree("testdb") catch {};
    }

    try lsm.put("key1", "val1");
    try lsm.put("key2", "val2");
    try lsm.put("key3", "val3");

    var buf: [4]u8 = undefined;
    const kv1 = try lsm.get("key1", &buf);
    try std.testing.expectEqualStrings("val1", kv1.?);
    const kv2 = try lsm.get("key2", &buf);
    try std.testing.expectEqualStrings("val2", kv2.?);
    const kv3 = try lsm.get("key3", &buf);
    try std.testing.expectEqualStrings("val3", kv3.?);
    const kvnull = try lsm.get("key4", &buf);
    try std.testing.expectEqual(null, kvnull);
    try lsm.put("key2", "v2up");
    const kv2up = try lsm.get("key2", &buf);
    try std.testing.expectEqualStrings("v2up", kv2up.?);

    try lsm.del("key2");
    try std.testing.expectEqual(null, try lsm.get("key2", &buf));

    try std.testing.expectEqual(2, lsm.state.imm_memtables.items.len);
}

test "LsmStorageInner: scan" {
    const test_gpa = std.testing.allocator;
    const test_options = LsmStorageOptions{
        .block_size = 4096,
        .target_sst_size = 8,
        .num_memtable_limit = 4,
    };
    var lsm = try LsmStorageInner.open("testdb", test_options, test_gpa);
    defer {
        lsm.close() catch @panic("Failed to close LSM");
        fs.cwd().deleteTree("testdb") catch {};
    }

    try lsm.put("a", "1");
    try lsm.put("b", "2");
    try lsm.put("c", "3");
    try lsm.put("d", "4");
    try lsm.put("e", "5");
    try lsm.del("b");
    try lsm.put("d", "7");

    var itr = try lsm.scan("b", "e");
    defer itr.inner.deinit();

    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("3", itr.val());
    try itr.next();
    try std.testing.expect(itr.is_valid());
    try std.testing.expectEqualStrings("7", itr.val());
    try itr.next();
    try std.testing.expect(!itr.is_valid());
}

test "LsmStorageInner: recover" {
    const test_gpa = std.testing.allocator;
    const test_options = LsmStorageOptions{
        .block_size = 4096,
        .target_sst_size = 8,
        .num_memtable_limit = 4,
    };
    var lsm = try LsmStorageInner.open("recover_testdb", test_options, test_gpa);
    defer fs.cwd().deleteTree("recover_testdb") catch {};

    for (0..5) |i| {
        const key = try std.fmt.allocPrint(test_gpa, "key{d}", .{i});
        defer test_gpa.free(key);
        const val = try std.fmt.allocPrint(test_gpa, "val{d}", .{i});
        defer test_gpa.free(val);
        try lsm.put(key, val);
    }
    try lsm.close();

    var recovered_lsm = try LsmStorageInner.open("recover_testdb", test_options, test_gpa);
    defer recovered_lsm.close() catch {};
    try std.testing.expectEqual(5, recovered_lsm.state.l0.items.len);
    try std.testing.expectEqual(0, recovered_lsm.state.levels.items[0].items.len);

    try recovered_lsm.put("key6", "val6");

    var buf: [4]u8 = undefined;
    const kv0 = try recovered_lsm.get("key0", &buf);
    try std.testing.expectEqualStrings("val0", kv0.?);
    const kv1 = try recovered_lsm.get("key1", &buf);
    try std.testing.expectEqualStrings("val1", kv1.?);
    const kv2 = try recovered_lsm.get("key2", &buf);
    try std.testing.expectEqualStrings("val2", kv2.?);
    const kv3 = try recovered_lsm.get("key3", &buf);
    try std.testing.expectEqualStrings("val3", kv3.?);
    const kv4 = try recovered_lsm.get("key4", &buf);
    try std.testing.expectEqualStrings("val4", kv4.?);
    const kv5 = try recovered_lsm.get("key5", &buf);
    try std.testing.expectEqual(null, kv5);
    const kv6 = try recovered_lsm.get("key6", &buf);
    try std.testing.expectEqualStrings("val6", kv6.?);
}

test "LsmStorageInner: recover with wal" {
    const test_gpa = std.testing.allocator;
    const test_options = LsmStorageOptions{
        .block_size = 4096,
        .target_sst_size = 16,
        .num_memtable_limit = 2,
        .enable_wal = true,
    };
    var lsm = try LsmStorageInner.open("recover_wal_testdb", test_options, test_gpa);
    defer fs.cwd().deleteTree("recover_wal_testdb") catch {};

    try lsm.put("key1", "val1");
    try lsm.put("key2", "val2");
    try lsm.put("key3", "val3");
    try lsm.put("key4", "val4");
    try lsm.put("key5", "val5");
    try lsm.close();

    var recovered_lsm = try LsmStorageInner.open("recover_wal_testdb", test_options, test_gpa);
    defer recovered_lsm.close() catch {};

    try recovered_lsm.put("key6", "val6");

    var buf: [4]u8 = undefined;
    const kv1 = try recovered_lsm.get("key1", &buf);
    try std.testing.expectEqualStrings("val1", kv1.?);
    const kv2 = try recovered_lsm.get("key2", &buf);
    try std.testing.expectEqualStrings("val2", kv2.?);
    const kv3 = try recovered_lsm.get("key3", &buf);
    try std.testing.expectEqualStrings("val3", kv3.?);
    const kv4 = try recovered_lsm.get("key4", &buf);
    try std.testing.expectEqualStrings("val4", kv4.?);
    const kv5 = try recovered_lsm.get("key5", &buf);
    try std.testing.expectEqualStrings("val5", kv5.?);
    const kv6 = try recovered_lsm.get("key6", &buf);
    try std.testing.expectEqualStrings("val6", kv6.?);
}
