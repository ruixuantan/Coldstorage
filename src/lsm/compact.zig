const std = @import("std");
const mem = std.mem;
const SsTable = @import("table.zig").SsTable;
const LsmStorageState = @import("lsm.zig").LsmStorageState;
const simple_leveled = @import("compact/simple_leveled.zig");

const SimpleLeveledCompactionTask = simple_leveled.SimpleLeveledCompactionTask;
const SimpleLeveledCompactionController = simple_leveled.SimpleLeveledCompactionController;
const SimpleLeveledCompactionOptions = simple_leveled.SimpleLeveledCompactionOptions;

pub const CompactionTask = union(enum) {
    simple_leveled: SimpleLeveledCompactionTask,

    pub fn deinit(self: *CompactionTask, gpa: mem.Allocator) void {
        switch (self.*) {
            .simple_leveled => |t| {
                t.deinit(gpa);
            },
        }
    }
};

pub const CompactionOptions = union(enum) {
    simple_leveled: SimpleLeveledCompactionOptions,
};

pub const CompactionController = union(enum) {
    simple_leveled: SimpleLeveledCompactionController,

    pub fn from_options(
        options: CompactionOptions,
    ) CompactionController {
        return switch (options) {
            .simple_leveled => |opts| CompactionController{
                .simple_leveled = .{ .options = opts },
            },
        };
    }

    pub fn init_compaction_task(
        self: CompactionController,
        state: *LsmStorageState,
    ) !?CompactionTask {
        return switch (self) {
            .simple_leveled => |ctrl| {
                if (try ctrl.init_compaction_task(state)) |t| {
                    return CompactionTask{ .simple_leveled = t };
                } else {
                    return null;
                }
            },
        };
    }

    pub fn apply_compaction_result(
        self: CompactionController,
        state: *LsmStorageState,
        task: CompactionTask,
        output: []const *SsTable,
    ) !void {
        switch (self) {
            .simple_leveled => |ctrl| {
                try ctrl.apply_compaction_result(state, task.simple_leveled, output);
            },
        }
    }
};
