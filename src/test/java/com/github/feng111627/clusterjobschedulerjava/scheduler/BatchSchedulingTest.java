package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.Job;
import com.github.feng111627.clusterjobschedulerjava.model.JobState;
import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import com.github.feng111627.clusterjobschedulerjava.model.TaskPlacement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：整批调度 —— 优先级/排队顺序稳定可解释；多任务作业要么整批落位、要么一个都不占；
 * 任何节点剩余资源不得为负。
 */
class BatchSchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(BatchSchedulingTest.class);

    private ClusterScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new ClusterScheduler();
        scheduler.registerNode("node-a", new Resource(8, 16384));
        scheduler.registerNode("node-b", new Resource(8, 16384));
        scheduler.registerNode("node-c", new Resource(8, 16384));
    }

    private void report(String business, String useCase, boolean passed, String detail) {
        log.info("【业务】{} | 【用例】{} | 【结果】{} | {}",
                business, useCase, passed ? "通过" : "失败", detail);
        assertTrue(passed, () -> business + " / " + useCase + " : " + detail);
    }

    private boolean allNodesNonNegative() {
        return scheduler.snapshot().nodes().stream().allMatch(n ->
                n.cpuAvailable() >= 0 && n.memoryAvailableMb() >= 0
                        && n.cpuUsed() >= 0 && n.memoryUsedMb() >= 0);
    }

    @Test
    @DisplayName("优先级高的先调度；同优先级先提交的先调度")
    void priorityThenFifoOrdering() {
        String business = "优先级与排队顺序";

        // 先提交低优先级 big-low（3 任务 x 6CPU = 18CPU），再提交高优先级 big-high（18CPU），
        // 集群共 24CPU，二者不能同时跑。
        scheduler.submitJob("big-low", 1, 3, new Resource(6, 1024));
        scheduler.submitJob("big-high", 9, 3, new Resource(6, 1024));

        ScheduleResult r = scheduler.schedulePendingJobs();
        boolean case1 = r.scheduledJobIds().equals(List.of("big-high"))
                && r.queuedJobIds().equals(List.of("big-low"));
        report(business, "高优先级 big-high 先落位，big-low 留队列", case1,
                "scheduled=" + r.scheduledJobIds() + ", queued=" + r.queuedJobIds());

        // 同优先级：先提交者先调度。释放后两个同优先级作业各需独占集群（3 任务 x 8CPU）。
        scheduler.completeJob("big-high");
        scheduler.submitJob("same-a", 5, 3, new Resource(8, 1024)); // 24CPU，独占集群
        scheduler.submitJob("same-b", 5, 3, new Resource(8, 1024));
        ScheduleResult r2 = scheduler.schedulePendingJobs();
        // same-a 必须在 same-b 之前；big-low 优先级 1 最低，排在最后
        boolean order = r2.scheduledJobIds().equals(List.of("same-a"))
                && r2.queuedJobIds().indexOf("same-b") < r2.queuedJobIds().indexOf("big-low");
        report(business, "同优先级按提交先后：same-a 先于 same-b，big-low 最后", order,
                "scheduled=" + r2.scheduledJobIds() + ", queued=" + r2.queuedJobIds());
    }

    @Test
    @DisplayName("相同初始状态下调度结论一致；达到不动点后连续调用不再抖动")
    void schedulingIsStableAcrossRounds() {
        String business = "调度顺序稳定性";

        // 用例1：两个提交序列完全相同的独立集群，各自调度一轮，结论必须逐位一致
        ClusterScheduler other = new ClusterScheduler();
        other.registerNode("node-a", new Resource(8, 16384));
        other.registerNode("node-b", new Resource(8, 16384));
        other.registerNode("node-c", new Resource(8, 16384));
        for (ClusterScheduler s : List.of(scheduler, other)) {
            s.submitJob("j1", 3, 3, new Resource(6, 1024));
            s.submitJob("j2", 3, 3, new Resource(6, 1024));
            s.submitJob("j3", 7, 3, new Resource(6, 1024));
        }
        ScheduleResult first = scheduler.schedulePendingJobs();
        ScheduleResult mirror = other.schedulePendingJobs();
        boolean deterministic = first.scheduledJobIds().equals(mirror.scheduledJobIds())
                && first.queuedJobIds().equals(mirror.queuedJobIds())
                && first.scheduledJobIds().equals(List.of("j3"))
                && first.queuedJobIds().equals(List.of("j1", "j2"));
        report(business, "相同状态的两个集群调度结论逐位一致（j3 落位，j1/j2 按序排队）",
                deterministic, "a=" + first.scheduledJobIds() + "/" + first.queuedJobIds()
                        + ", b=" + mirror.scheduledJobIds() + "/" + mirror.queuedJobIds());

        // 用例2：状态不再变化时继续调度，到达不动点（无作业落位、队列不变）
        ScheduleResult second = scheduler.schedulePendingJobs();
        ScheduleResult third = scheduler.schedulePendingJobs();
        boolean fixedPoint = second.scheduledJobIds().isEmpty()
                && second.queuedJobIds().equals(List.of("j1", "j2"))
                && second.queuedJobIds().equals(third.queuedJobIds())
                && third.scheduledJobIds().isEmpty();
        report(business, "无可调度作业后连续两轮均为同一不动点，不抖动", fixedPoint,
                "second.queued=" + second.queuedJobIds() + ", third.queued=" + third.queuedJobIds());
    }

    @Test
    @DisplayName("整批原子：放不下全部任务时一个任务都不占用")
    void allOrNothingPlacement() {
        String business = "多任务整批原子调度";

        // 先占掉 node-a 4CPU，node-b/c 各满（8/8），使全集群只剩 node-a 的 4CPU 连续可用。
        scheduler.submitJob("filler-a", 1, 1, new Resource(4, 1024));
        scheduler.submitJob("filler-b", 1, 1, new Resource(8, 1024));
        scheduler.submitJob("filler-c", 1, 1, new Resource(8, 1024));
        scheduler.schedulePendingJobs();

        long usedBefore = totalUsedCpu();
        // hungry: 3 个任务各 4CPU，需要 3 个能放下 4CPU 的节点；此刻只有 node-a 剩 4CPU
        scheduler.submitJob("hungry", 5, 3, new Resource(4, 1024));
        ScheduleResult r = scheduler.schedulePendingJobs();
        boolean queued = r.queuedJobIds().contains("hungry")
                && !r.scheduledJobIds().contains("hungry");
        boolean nothingAllocated = totalUsedCpu() == usedBefore;
        report(business, "3x4CPU 任务只能放下 1 个时，整批留在队列", queued,
                "scheduled=" + r.scheduledJobIds() + ", queued=" + r.queuedJobIds());
        report(business, "整批失败时不允许部分占用资源", nothingAllocated,
                "调度前已用CPU=" + usedBefore + ", 调度后已用CPU=" + totalUsedCpu());

        // 释放两个 filler 后，node-a 4 + node-b 8 + node-c 8 足够 3 个 4CPU，整批成功
        scheduler.completeJob("filler-b");
        scheduler.completeJob("filler-c");
        ScheduleResult r2 = scheduler.schedulePendingJobs();
        Job hungry = scheduler.getJob("hungry");
        boolean allPlaced = r2.scheduledJobIds().equals(List.of("hungry"))
                && hungry.getState() == JobState.RUNNING
                && hungry.getPlacements().size() == 3;
        report(business, "腾出空间后 3 个任务整批落位且作业变 RUNNING", allPlaced,
                "落点=" + hungry.getPlacements().stream().map(TaskPlacement::nodeId).toList());

        boolean noNegative = allNodesNonNegative();
        report(business, "全过程所有节点剩余资源非负", noNegative,
                "节点账目=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + ": used=" + n.cpuUsed() + ", avail=" + n.cpuAvailable())
                        .toList());
    }

    @Test
    @DisplayName("任务可跨节点分散放置，装箱后账目守恒")
    void tasksSpreadAcrossNodes() {
        String business = "跨节点装箱与账目守恒";
        // 12 个 2CPU 任务 = 24CPU，恰好铺满 3 节点
        scheduler.submitJob("spread", 1, 12, new Resource(2, 1024));
        ScheduleResult r = scheduler.schedulePendingJobs();
        boolean placed = r.scheduledJobIds().equals(List.of("spread"))
                && scheduler.getJob("spread").getPlacements().size() == 12;
        report(business, "12x2CPU 整批铺满 3 节点", placed, "落点数="
                + scheduler.getJob("spread").getPlacements().size());

        boolean fullyUsed = scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuUsed() == 8 && n.cpuAvailable() == 0);
        report(business, "每节点恰好 8CPU 用满、剩余 0", fullyUsed,
                "节点=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + " avail=" + n.cpuAvailable()).toList());

        // 此时任何新作业都放不下
        scheduler.submitJob("late", 9, 1, new Resource(1, 1024));
        ScheduleResult r2 = scheduler.schedulePendingJobs();
        report(business, "集群用满时高优先级新作业也只能排队",
                r2.queuedJobIds().equals(List.of("late")), "queued=" + r2.queuedJobIds());

        scheduler.completeJob("spread");
        boolean returned = scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuUsed() == 0 && n.cpuAvailable() == 8);
        report(business, "完成后全部 24CPU 原样归还", returned,
                "节点可用=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + ":" + n.cpuAvailable()).toList());
        assertEquals(0, 0);
    }

    private long totalUsedCpu() {
        return scheduler.snapshot().nodes().stream().mapToLong(ClusterSnapshot.NodeView::cpuUsed).sum();
    }
}
