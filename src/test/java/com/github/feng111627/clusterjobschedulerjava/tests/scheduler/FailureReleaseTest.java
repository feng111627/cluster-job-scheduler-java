package com.github.feng111627.clusterjobschedulerjava.tests.scheduler;

import com.github.feng111627.clusterjobschedulerjava.scheduler.JobState;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Resources;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SchedulerService;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Task;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 业务四：节点掉线 / 任务失败时的资源释放。
 *
 * <p>验证：
 * <ul>
 *   <li>掉线节点上的作业整体 FAILED，占用全部归还，队列里的作业自动补位；</li>
 *   <li>重复掉线、重复释放、重复失败上报都是幂等的，可用资源不会越算越多；</li>
 *   <li>无论以什么顺序释放，最终节点账目一致（顺序无关）；</li>
 *   <li>任何节点任何一维的剩余资源都不会是负数。</li>
 * </ul>
 */
public class FailureReleaseTest {

    private SchedulerService newCluster() {
        SchedulerService scheduler = new SchedulerService();
        scheduler.registerNode("n1", new Resources(4, 4, 0));
        scheduler.registerNode("n2", new Resources(4, 4, 0));
        return scheduler;
    }

    private Task task(String id, int cpu, int mem) {
        return new Task(id, new Resources(cpu, mem, 0));
    }

    @TestFactory
    Stream<DynamicTest> cases() {
        BusinessCaseReporter reporter = new BusinessCaseReporter("节点故障与任务失败释放");

        reporter.add("01", "节点掉线：跨节点作业整体 FAILED、两台节点的份额都归还，排队作业自动补位", () -> {
            SchedulerService scheduler = newCluster();
            // j1 独占 n1 的 3C；j2 跨两台节点（每台占 1C，同生共死）
            scheduler.submitJob("j1", List.of(task("a", 3, 3)), 0);
            scheduler.submitJob("j2", List.of(task("e", 1, 1), task("f", 1, 1)), 0);
            // j3 排队：需要 3C；只有 n1 或 n2 上空出 3C 才能跑
            //   n1 剩 0C（j1 占 3 + j2 占 1），n2 剩 3C 但 j2 占了 1C -> 只剩 3C... 恰好？
            //   4-1=3C 可用，所以 j3 当场就能上 n2。改为 4C 需求使其排队：
            scheduler.submitJob("j3", List.of(task("g", 4, 4)), 0);
            BusinessCaseReporter.check(scheduler.getJobState("j2") == JobState.RUNNING,
                    () -> "j2 应 RUNNING（e->n1, f->n2）");
            BusinessCaseReporter.check(scheduler.getJobState("j3") == JobState.PENDING,
                    () -> "j3 应 PENDING");

            scheduler.nodeOffline("n1");

            // j1、j2 都在 n1 上有任务，二者都应 FAILED；
            // j2 的 f 任务在 n2 上也必须被一并释放（同生共死）
            BusinessCaseReporter.check(scheduler.getJobState("j1") == JobState.FAILED,
                    () -> "n1 掉线后 j1 应 FAILED，实际：" + scheduler.getJobState("j1"));
            BusinessCaseReporter.check(scheduler.getJobState("j2") == JobState.FAILED,
                    () -> "n1 掉线后 j2 应 FAILED，实际：" + scheduler.getJobState("j2"));
            BusinessCaseReporter.check(scheduler.getTaskAssignments("j2").get("f") == null,
                    () -> "j2 在 n2 上的 f 也应解除分配，实际："
                            + scheduler.getTaskAssignments("j2"));
            // n2 完整 4C 空出，j3 自动补位
            BusinessCaseReporter.check(scheduler.getJobState("j3") == JobState.RUNNING,
                    () -> "资源释放后 j3 应自动补位 RUNNING，实际："
                            + scheduler.getJobState("j3"));
            BusinessCaseReporter.check("n2".equals(scheduler.getTaskAssignments("j3").get("g")),
                    () -> "g 应在仅存的 n2 上，实际："
                            + scheduler.getTaskAssignments("j3"));
            System.out.println("  掉线后节点账目：" + scheduler.nodeSnapshot());
            BusinessCaseReporter.check(
                    scheduler.nodeSnapshot().get("n2").contains("allocated=(cpu=4, mem=4, disk=0)"),
                    () -> "n2 账目应为 j3 的 4C/4G，实际："
                            + scheduler.nodeSnapshot().get("n2"));
        });

        reporter.add("02", "重复掉线幂等：账目不变，可用资源不会虚增", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j1", List.of(task("a", 3, 3)), 0);
            scheduler.nodeOffline("n1");
            String afterFirst = scheduler.nodeSnapshot().toString();
            scheduler.nodeOffline("n1");
            scheduler.nodeOffline("n1");
            String afterThird = scheduler.nodeSnapshot().toString();
            BusinessCaseReporter.check(afterFirst.equals(afterThird),
                    () -> "重复掉线后账目发生变化：" + afterFirst + " != " + afterThird);
            System.out.println("  三次掉线后账目保持：" + afterThird);
        });

        reporter.add("03", "任务失败：整个作业 FAILED 并归还全部资源，重复上报幂等", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j1", List.of(task("a", 3, 3), task("b", 3, 3)), 0);
            scheduler.submitJob("j2", List.of(task("c", 4, 4)), 0);
            BusinessCaseReporter.check(scheduler.getJobState("j2") == JobState.PENDING,
                    () -> "j2 初始应排队");

            scheduler.taskFailed("a");
            BusinessCaseReporter.check(scheduler.getJobState("j1") == JobState.FAILED,
                    () -> "j1 应 FAILED");
            BusinessCaseReporter.check(scheduler.getJobState("j2") == JobState.RUNNING,
                    () -> "j1 释放后 j2 应补位 RUNNING");
            String snapshotAfterFail = scheduler.nodeSnapshot().toString();

            // 重复上报同一任务失败 / 释放已结束作业，都不应改变账目
            scheduler.taskFailed("a");
            scheduler.taskFailed("b");
            scheduler.releaseJob("j1");
            String snapshotAfterRepeats = scheduler.nodeSnapshot().toString();
            BusinessCaseReporter.check(snapshotAfterFail.equals(snapshotAfterRepeats),
                    () -> "重复失败上报后账目变化：" + snapshotAfterFail + " != " + snapshotAfterRepeats);
            System.out.println("  重复上报后账目保持：" + snapshotAfterRepeats);
        });

        reporter.add("04", "释放顺序无关：无排队作业时，乱序释放的最终账目完全一致且归零", () -> {
            // 用 3 台 4C 节点，保证 3 个作业提交即全部 RUNNING（队列里没有等待者）
            SchedulerService orderA = new SchedulerService();
            SchedulerService orderB = new SchedulerService();
            for (SchedulerService s : List.of(orderA, orderB)) {
                s.registerNode("n1", new Resources(4, 4, 0));
                s.registerNode("n2", new Resources(4, 4, 0));
                s.registerNode("n3", new Resources(4, 4, 0));
                s.submitJob("j1", List.of(task("a", 4, 4)), 0);
                s.submitJob("j2", List.of(task("b", 4, 4)), 0);
                s.submitJob("j3", List.of(task("c", 4, 4)), 0);
            }
            BusinessCaseReporter.check(orderA.queuedJobIds().isEmpty() && orderB.queuedJobIds().isEmpty(),
                    () -> "前置条件：两边队列都应为空");
            // 顺序 A：j1 -> j2 -> j3
            orderA.releaseJob("j1");
            orderA.releaseJob("j2");
            orderA.releaseJob("j3");
            // 顺序 B：j3 -> j1 -> j2（乱序）
            orderB.releaseJob("j3");
            orderB.releaseJob("j1");
            orderB.releaseJob("j2");
            String a = orderA.nodeSnapshot().toString();
            String b = orderB.nodeSnapshot().toString();
            System.out.println("  顺序A 账目：" + a);
            System.out.println("  顺序B 账目：" + b);
            BusinessCaseReporter.check(a.equals(b), () -> "两种释放顺序的最终账目不同");
            BusinessCaseReporter.check(a.contains("allocated=(cpu=0, mem=0, disk=0)"),
                    () -> "全部释放后所有节点账目应归零");
        });

        reporter.add("05", "账目守恒：并发释放 + 掉线混合操作后，没有节点出现负资源", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j1", List.of(task("a", 2, 2), task("b", 2, 2)), 0);
            scheduler.submitJob("j2", List.of(task("e", 2, 2), task("f", 2, 2)), 0);
            scheduler.submitJob("j3", List.of(task("g", 4, 4)), 0);

            // 各操作从不同线程几乎同时触发
            Thread t1 = new Thread(() -> scheduler.releaseJob("j1"));
            Thread t2 = new Thread(() -> scheduler.taskFailed("e"));
            Thread t3 = new Thread(() -> scheduler.nodeOffline("n2"));
            t1.start();
            t2.start();
            t3.start();
            t1.join();
            t2.join();
            t3.join();

            // 把所有还在运行的作业显式释放掉，最终在线节点 n1 账目必须归零且不为负
            scheduler.releaseJob("j3");
            Map<String, String> snapshot = scheduler.nodeSnapshot();
            System.out.println("  混合故障后最终账目：" + snapshot);
            BusinessCaseReporter.check(
                    snapshot.get("n1").contains("allocated=(cpu=0, mem=0, disk=0)"),
                    () -> "n1 最终账目应归零：" + snapshot.get("n1"));
            BusinessCaseReporter.check(snapshot.get("n1").contains("available=(cpu=4, mem=4, disk=0)"),
                    () -> "n1 可用资源应恢复满配且不为负：" + snapshot.get("n1"));
            BusinessCaseReporter.check(snapshot.get("n2").contains("online=false"),
                    () -> "n2 应处于离线：" + snapshot.get("n2"));
        });

        return reporter.tests().stream();
    }
}
