const std = @import("std");
const mem = std.mem;
const SsTable = @import("../table.zig").SsTable;
const LsmStorageState = @import("../lsm.zig").LsmStorageState;

pub const SimpleLeveledCompactionOptions = struct {
    size_ratio_percent: usize = 200,
    l0_file_num_compaction_trigger: usize = 8,
    max_lvls: usize = 8,
};

pub const SimpleLeveledCompactionTask = struct {
    upper_level: ?usize,
    upper_level_ssts: []*SsTable,
    lower_level: usize,
    lower_level_ssts: []*SsTable,
    is_lower_level_bottom: bool,

    pub fn deinit(self: SimpleLeveledCompactionTask, gpa: mem.Allocator) void {
        gpa.free(self.upper_level_ssts);
        gpa.free(self.lower_level_ssts);
    }
};

pub const SimpleLeveledCompactionController = struct {
    options: SimpleLeveledCompactionOptions,

    pub fn init_compaction_task(
        self: SimpleLeveledCompactionController,
        state: *LsmStorageState,
    ) !?SimpleLeveledCompactionTask {
        if (self.options.max_lvls == 0) return null;

        var upper_level_ssts = try std.ArrayList(*SsTable).initCapacity(state.gpa, 1);
        defer upper_level_ssts.deinit(state.gpa);
        var lower_level_ssts = try std.ArrayList(*SsTable).initCapacity(state.gpa, 1);
        defer lower_level_ssts.deinit(state.gpa);

        if (state.l0.items.len >= self.options.l0_file_num_compaction_trigger) {
            for (state.l0.items) |sst| {
                try upper_level_ssts.append(state.gpa, sst);
            }
            for (state.levels.items[0].items) |sst| {
                try lower_level_ssts.append(state.gpa, sst);
            }
            return .{
                .upper_level = null,
                .upper_level_ssts = try upper_level_ssts.toOwnedSlice(state.gpa),
                .lower_level = 1,
                .lower_level_ssts = try lower_level_ssts.toOwnedSlice(state.gpa),
                .is_lower_level_bottom = false,
            };
        }
        return null;
    }

    pub fn apply_compaction_result(
        self: SimpleLeveledCompactionController,
        state: *LsmStorageState,
        task: SimpleLeveledCompactionTask,
        output: []const *SsTable,
    ) !void {
        _ = self;
        for (0..state.l0.items.len) |_| {
            const sst = state.l0.pop().?;
            try state.delete_sst_file(sst);
            state.gpa.destroy(sst);
        }
        const level = task.lower_level;
        if (state.levels.items.len <= level) {
            try state.append_empty_level();
        }
        for (output) |sst| {
            try state.levels.items[level - 1].append(state.gpa, sst);
            try state.manifest.append_record(.{
                .level = @intCast(level),
                .sst_id = @intCast(sst.id),
            });
        }
    }
};
