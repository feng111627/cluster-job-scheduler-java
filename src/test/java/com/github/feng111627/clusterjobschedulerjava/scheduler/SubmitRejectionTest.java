package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.JobState;
import com.github.feng111627.clusterjobschedulerjava.model.RejectReason;
import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：作业提交与排队 —— 资源需求超过物理容量必须拒绝，且不同拒绝原因可区分。
 */
class SubmitRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(SubmitRejectionTest.class);

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

    @Test
    @DisplayName("拒绝原因区分：单任务超最大节点 vs 总量超集群")
    void rejectWithDistinctReasons() {
        String business = "提交拒绝原因区分";

        // 用例1：单任务需要 9 CPU，超过任何单节点（8 CPU）
        SubmitResult r1 = scheduler.submitJob("job-big-task", 1, 1, new Resource(9, 1024));
        boolean case1 = !r1.accepted()
                && r1.rejectReason() == RejectReason.TASK_EXCEEDS_LARGEST_NODE
                && scheduler.getJob("job-big-task").getState() == JobState.REJECTED;
        report(business, "单任务 9CPU 超过最大单节点 8CPU", case1, r1.message());

        // 用例2：单任务 20000MB 内存，超过最大单节点 16384MB（CPU 不超，只卡内存维度）
        SubmitResult r2 = scheduler.submitJob("job-big-mem", 1, 1, new Resource(1, 20000));
        boolean case2 = !r2.accepted()
                && r2.rejectReason() == RejectReason.TASK_EXCEEDS_LARGEST_NODE;
        report(business, "单任务 20000MB 超过最大单节点 16384MB", case2, r2.message());

        // 用例3：每任务 1CPU 但要 25 个任务，总量 25CPU 超过集群总量 24CPU
        SubmitResult r3 = scheduler.submitJob("job-too-many", 1, 25, new Resource(1, 1024));
        boolean case3 = !r3.accepted()
                && r3.rejectReason() == RejectReason.TOTAL_EXCEEDS_CLUSTER_CAPACITY;
        report(business, "25 个 1CPU 任务总量超集群 24CPU", case3, r3.message());

        // 用例4：边界值——单任务 8CPU 恰好等于节点容量，必须接受
        SubmitResult r4 = scheduler.submitJob("job-fit-node", 1, 1, new Resource(8, 16384));
        report(business, "单任务恰好 8CPU/16384MB 等于节点容量", r4.accepted(), r4.message());

        // 用例5：边界值——24 个 1CPU 任务总量恰好等于集群总量，必须接受
        SubmitResult r5 = scheduler.submitJob("job-fit-cluster", 1, 24, new Resource(1, 1024));
        report(business, "24 个 1CPU 任务恰好等于集群总量", r5.accepted(), r5.message());

        // 汇总：被拒绝作业不应进入等待队列
        ClusterSnapshot snapshot = scheduler.snapshot();
        boolean queueOnlyContainsAccepted = snapshot.queuedJobIds().size() == 2
                && !snapshot.queuedJobIds().contains("job-big-task")
                && !snapshot.queuedJobIds().contains("job-too-many");
        report(business, "被拒绝作业不进入等待队列", queueOnlyContainsAccepted,
                "队列内容=" + snapshot.queuedJobIds());
    }

    @Test
    @DisplayName("被拒绝作业不占资源，后续作业仍可正常排队")
    void rejectedJobDoesNotOccupyResource() {
        String business = "提交拒绝不占用资源";
        scheduler.submitJob("job-x", 1, 100, new Resource(5, 1024)); // 500CPU，必拒
        SubmitResult normal = scheduler.submitJob("job-normal", 1, 2, new Resource(2, 1024));
        boolean ok = normal.accepted()
                && scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuUsed() == 0 && n.memoryUsedMb() == 0);
        report(business, "拒绝超大作业后，正常作业入队且节点零占用", ok,
                "队列=" + scheduler.snapshot().queuedJobIds());
        assertFalse(normal.job().getPlacements().iterator().hasNext());
        assertEquals(JobState.PENDING, normal.job().getState());
    }
}
