const std = @import("std");
const SsTable = @import("../table.zig").SsTable;
const LsmStorageState = @import("../lsm.zig").LsmStorageState;

// NOT THREAD SAFE

pub const DummyCompactionOptions = struct {
    max_l0_files: usize,
    max_l1_files: usize,
};

pub const DummyCompactionTask = struct {
    level: usize,
    tables: []SsTable,
};

pub const DummyCompactionController = struct {
    options: DummyCompactionOptions,

    pub fn init_compaction_task(
        self: DummyCompactionController,
        state: *LsmStorageState,
    ) ?DummyCompactionTask {
        if (state.l0.items.len >= self.options.max_l0_files) {
            return DummyCompactionTask{ .level = 0, .tables = state.l0.items };
        }
        if (state.levels.items[0].items.len >= self.options.max_l1_files) {
            return DummyCompactionTask{ .level = 1, .tables = state.levels.items[0].items };
        }
        return null;
    }

    pub fn apply_compaction_result(
        self: DummyCompactionController,
        state: *LsmStorageState,
        task: DummyCompactionTask,
        output: []const SsTable,
    ) !void {
        _ = self;
        std.debug.assert(task.tables.len > 0);
        switch (task.level) {
            0 => {
                for (output) |new_sst| {
                    var old_sst = state.l0.pop();
                    try state.delete_sst_file(&old_sst.?);
                    try state.levels.items[0].append(state.gpa, new_sst);
                    try state.manifest.append_record(.{ .level = 1, .sst_id = @intCast(new_sst.id) });
                }
            },
            1 => {
                for (output) |_| {
                    var old_sst = state.levels.items[0].pop();
                    try state.delete_sst_file(&old_sst.?);
                }
                if (state.levels.items.len == 1) {
                    try state.append_empty_level();
                }
                for (output) |new_sst| {
                    try state.levels.items[1].append(state.gpa, new_sst);
                    try state.manifest.append_record(.{ .level = 2, .sst_id = @intCast(new_sst.id) });
                }
            },
            else => unreachable,
        }
    }
};
