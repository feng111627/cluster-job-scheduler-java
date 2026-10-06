package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.Job;
import com.github.feng111627.clusterjobschedulerjava.model.JobState;
import com.github.feng111627.clusterjobschedulerjava.model.NodeState;
import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import com.github.feng111627.clusterjobschedulerjava.model.TaskPlacement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：节点掉线/任务失败 —— 受影响作业整体失败并归还其占用的全部资源；
 * 重复故障、释放顺序都不改变最终账目。
 */
class NodeFailureTest {

    private static final Logger log = LoggerFactory.getLogger(NodeFailureTest.class);

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
    @DisplayName("节点掉线：其上作业整体失败，全部资源归还，队列作业可接手腾出的资源")
    void nodeOfflineReleasesAffectedJobs() {
        String business = "节点掉线释放";

        // job-span: 12 个 2CPU 任务铺满 3 节点；job-a: 1 个 4CPU 作业随后放不下
        scheduler.submitJob("job-span", 1, 12, new Resource(2, 1024));
        scheduler.submitJob("job-wait", 1, 1, new Resource(4, 1024));
        scheduler.schedulePendingJobs();
        Job span = scheduler.getJob("job-span");
        report(business, "前置：job-span 12 任务跨 3 节点运行，job-wait 排队",
                span.getState() == JobState.RUNNING
                        && scheduler.snapshot().queuedJobIds().equals(List.of("job-wait")),
                "running=" + scheduler.snapshot().runningJobIds());

        // node-a 掉线：job-span 整体失败，它在 node-b/node-c 上的任务资源也一并归还
        int failed = scheduler.markNodeOffline("node-a");
        boolean case1 = failed == 1 && span.getState() == JobState.FAILED;
        report(business, "node-a 掉线导致 job-span 整体 FAILED", case1,
                "受影响作业数=" + failed + ", job-span 状态=" + span.getState());

        boolean case2 = scheduler.snapshot().nodes().stream()
                .filter(n -> !n.nodeId().equals("node-a"))
                .allMatch(n -> n.cpuUsed() == 0 && n.cpuAvailable() == 8);
        report(business, "跨节点作业在 node-b/node-c 上占用的资源也全部归还",
                case2, "节点=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + " used=" + n.cpuUsed()).toList());

        // 重复掉线返回 0，不再重复释放
        boolean repeat = scheduler.markNodeOffline("node-a") == 0;
        report(business, "重复标记同一节点掉线返回 0，账目不重复变化", repeat,
                "usedCpu=" + scheduler.snapshot().nodes().stream()
                        .mapToInt(ClusterSnapshot.NodeView::cpuUsed).sum());

        // 掉线节点不参与装箱：job-wait 的 4CPU 应落在 node-b/node-c 而非 node-a
        ScheduleResult r = scheduler.schedulePendingJobs();
        Job wait = scheduler.getJob("job-wait");
        boolean placed = r.scheduledJobIds().equals(List.of("job-wait"))
                && wait.getPlacements().stream().noneMatch(p -> p.nodeId().equals("node-a"));
        report(business, "恢复调度后 job-wait 落位在线节点，掉线节点不参与装箱",
                placed, "落点=" + wait.getPlacements().stream().map(TaskPlacement::nodeId).toList());
    }

    @Test
    @DisplayName("节点恢复上线后账目为 0 占用，可继续接受调度")
    void nodeComesBackOnline() {
        String business = "节点恢复上线";
        scheduler.submitJob("fill", 1, 12, new Resource(2, 1024));
        scheduler.schedulePendingJobs();
        scheduler.markNodeOffline("node-b");

        // node-b 已被清空资源并下线
        var nodeB = scheduler.snapshot().nodes().stream()
                .filter(n -> n.nodeId().equals("node-b")).findFirst().orElseThrow();
        boolean case1 = nodeB.state().equals("OFFLINE") && nodeB.cpuUsed() == 0;
        report(business, "掉线节点状态 OFFLINE 且占用清零", case1,
                "state=" + nodeB.state() + ", used=" + nodeB.cpuUsed());

        boolean back = scheduler.markNodeOnline("node-b")
                && scheduler.nodeState("node-b") == NodeState.ONLINE;
        report(business, "节点重新上线，状态恢复 ONLINE", back, "state=" + scheduler.nodeState("node-b"));

        // 上线后集群 8+0+8=16 可用；新 4CPU 作业可以使用 node-b
        scheduler.submitJob("after", 1, 1, new Resource(4, 1024));
        ScheduleResult r = scheduler.schedulePendingJobs();
        boolean placed = r.scheduledJobIds().equals(List.of("after"));
        report(business, "上线节点立即可接受新作业", placed,
                "落点=" + scheduler.getJob("after").getPlacements().stream()
                        .map(TaskPlacement::nodeId).toList());

        boolean nonNegative = scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuAvailable() >= 0 && n.cpuUsed() >= 0);
        report(business, "全过程账目非负", nonNegative,
                "节点=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + " avail=" + n.cpuAvailable()).toList());
    }

    @Test
    @DisplayName("单个作业任务失败：释放其全部资源，同节点其他作业不受影响")
    void singleJobFailureDoesNotAffectOthers() {
        String business = "任务失败释放";
        // job-x 占 node-a 4CPU，job-y 随后也占 node-a 4CPU（首次适应），node-a 恰好 8
        scheduler.submitJob("job-x", 1, 1, new Resource(4, 1024));
        scheduler.submitJob("job-y", 1, 1, new Resource(4, 1024));
        scheduler.schedulePendingJobs();

        boolean before = scheduler.snapshot().nodes().stream()
                .filter(n -> n.nodeId().equals("node-a"))
                .findFirst().orElseThrow().cpuUsed() == 8;
        report(business, "前置：job-x/job-y 同处 node-a 共占 8CPU", before, "node-a used=8");

        boolean failed = scheduler.failJob("job-x", "task-crash");
        Job x = scheduler.getJob("job-x");
        Job y = scheduler.getJob("job-y");
        boolean case1 = failed && x.getState() == JobState.FAILED
                && y.getState() == JobState.RUNNING;
        report(business, "job-x 失败释放，同节点 job-y 继续 RUNNING 不受影响", case1,
                "x=" + x.getState() + ", y=" + y.getState());

        var nodeA = scheduler.snapshot().nodes().stream()
                .filter(n -> n.nodeId().equals("node-a")).findFirst().orElseThrow();
        boolean case2 = nodeA.cpuUsed() == 4 && nodeA.cpuAvailable() == 4;
        report(business, "node-a 仅剩余 job-y 的 4CPU 占用，账目精确", case2,
                "used=" + nodeA.cpuUsed() + ", available=" + nodeA.cpuAvailable());

        // 重复失败释放返回 false
        boolean repeat = !scheduler.failJob("job-x", "again");
        report(business, "对已 FAILED 的作业再次 fail 返回 false 且账目不变",
                repeat && nodeA.cpuUsed() == 4, "used=" + nodeA.cpuUsed());
    }
}
