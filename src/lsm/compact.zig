const std = @import("std");
const SsTable = @import("table.zig").SsTable;
const LsmStorageState = @import("lsm.zig").LsmStorageState;
const dummy = @import("compact/dummy.zig");

const DummyCompactionTask = dummy.DummyCompactionTask;
const DummyCompactionController = dummy.DummyCompactionController;
const DummyCompactionOptions = dummy.DummyCompactionOptions;

pub const CompactionTask = union(enum) {
    dummy: DummyCompactionTask,
};

pub const CompactionOptions = union(enum) {
    dummy: DummyCompactionOptions,
};

pub const CompactionController = union(enum) {
    dummy: DummyCompactionController,

    pub fn from_options(
        options: CompactionOptions,
    ) CompactionController {
        return switch (options) {
            .dummy => |opts| CompactionController{
                .dummy = DummyCompactionController{ .options = opts },
            },
        };
    }

    pub fn init_compaction_task(
        self: CompactionController,
        state: *LsmStorageState,
    ) ?CompactionTask {
        return switch (self) {
            .dummy => |ctrl| {
                const task = ctrl.init_compaction_task(state);
                if (task) |t| {
                    return CompactionTask{ .dummy = t };
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
        output: []const SsTable,
    ) !void {
        switch (self) {
            .dummy => |ctrl| {
                try ctrl.apply_compaction_result(state, task.dummy, output);
            },
        }
    }
};
