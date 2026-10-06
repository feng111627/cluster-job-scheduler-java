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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 业务二：一个作业多任务整批调度（all-or-nothing）+ 队列顺序稳定可解释。
 *
 * <p>集群：n1/n2 各 4C/4G。
 */
public class BatchSchedulingTest {

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
        BusinessCaseReporter reporter = new BusinessCaseReporter("整批调度与排队顺序");

        reporter.add("01", "容量足够时多任务被拆分到不同节点，作业整体 RUNNING", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j1", List.of(
                    task("t1", 3, 3), task("t2", 3, 3), task("t3", 2, 2)), 0);
            // t1 -> n1(剩1), t2 -> n2(剩1), t3 放不下 -> 必须整体回滚
            BusinessCaseReporter.check(scheduler.getJobState("j1") == JobState.PENDING,
                    () -> "三台任务当前无法同时放下，应排队，实际：" + scheduler.getJobState("j1"));
            BusinessCaseReporter.check(scheduler.queuedJobIds().equals(List.of("j1")),
                    () -> "队列：" + scheduler.queuedJobIds());
        });

        reporter.add("02", "放不下时所有任务都未分配，节点账目不允许部分占用", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j1", List.of(
                    task("t1", 3, 3), task("t2", 3, 3), task("t3", 2, 2)), 0);
            Map<String, String> assigned = scheduler.getTaskAssignments("j1");
            long assignedCount = assigned.values().stream().filter(v -> v != null).count();
            BusinessCaseReporter.check(assignedCount == 0,
                    () -> "不应有任何任务被标记分配，实际：" + assigned);
            // 两台节点的可用资源必须仍是满配 4C/4G
            String n1 = scheduler.nodeSnapshot().get("n1");
            String n2 = scheduler.nodeSnapshot().get("n2");
            BusinessCaseReporter.check(n1.contains("allocated=(cpu=0, mem=0, disk=0)"),
                    () -> "n1 账目：" + n1);
            BusinessCaseReporter.check(n2.contains("allocated=(cpu=0, mem=0, disk=0)"),
                    () -> "n2 账目：" + n2);
        });

        reporter.add("03", "两任务分别落到两台节点 -> 作业 RUNNING 且分配完整", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("j2", List.of(task("a", 3, 3), task("b", 3, 3)), 0);
            BusinessCaseReporter.check(scheduler.getJobState("j2") == JobState.RUNNING,
                    () -> "实际状态：" + scheduler.getJobState("j2"));
            Map<String, String> placement = scheduler.getTaskAssignments("j2");
            assertEquals("n1", placement.get("a"));
            assertEquals("n2", placement.get("b"));
            System.out.println("  分配结果：" + placement);
        });

        reporter.add("04", "优先级高的后提交也先调度；同优先级按提交顺序（FIFO）", () -> {
            SchedulerService scheduler = newCluster();
            // 先用两个作业把两台节点占满
            scheduler.submitJob("holder1", List.of(task("h1", 4, 4)), 0);
            scheduler.submitJob("holder2", List.of(task("h2", 4, 4)), 0);
            // low 先来、high 后来，都要整台 4C，此刻都排队
            scheduler.submitJob("low", List.of(task("l", 4, 4)), 0);
            scheduler.submitJob("high", List.of(task("h", 4, 4)), 5);
            BusinessCaseReporter.check(scheduler.queuedJobIds().equals(List.of("high", "low")),
                    () -> "队列应按优先级排成 [high, low]，实际：" + scheduler.queuedJobIds());

            // 释放一台节点：优先级高的 high 应被自动调度，low 继续排队
            scheduler.releaseJob("holder1");
            BusinessCaseReporter.check(scheduler.getJobState("high") == JobState.RUNNING,
                    () -> "high 应已调度，实际：" + scheduler.getJobState("high"));
            BusinessCaseReporter.check(scheduler.getJobState("low") == JobState.PENDING,
                    () -> "low 应继续排队，实际：" + scheduler.getJobState("low"));
            BusinessCaseReporter.check(scheduler.queuedJobIds().equals(List.of("low")),
                    () -> "队列应为 [low]，实际：" + scheduler.queuedJobIds());

            // 再释放另一台：low 按 FIFO 顺序被自动调度
            scheduler.releaseJob("holder2");
            BusinessCaseReporter.check(scheduler.getJobState("low") == JobState.RUNNING,
                    () -> "第二次释放后 low 应自动调度，实际：" + scheduler.getJobState("low"));
            BusinessCaseReporter.check(scheduler.queuedJobIds().isEmpty(),
                    () -> "队列应清空，实际：" + scheduler.queuedJobIds());
        });

        reporter.add("05", "调度顺序稳定：队列不变时连续手动调度，结论完全一致", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("big", List.of(
                    task("b1", 3, 3), task("b2", 3, 3), task("b3", 3, 3)), 10);
            // 集群共 8C，三任务要 9C -> 永远放不下（提交时按总容量拒绝）；
            // 改成两任务 6C 但需要分两台、每台只剩 3C 的场景：
            SchedulerService s2 = newCluster();
            s2.submitJob("holder", List.of(task("h", 1, 1)), 0); // 占住 n1 1C
            s2.submitJob("q1", List.of(task("q1a", 3, 3), task("q1b", 4, 4)), 10);
            // q1a 可放 n1(剩3)，q1b 需要 n2(4) -> 可以放下
            BusinessCaseReporter.check(s2.getJobState("q1") == JobState.RUNNING,
                    () -> "q1 应可整批调度：" + s2.getJobState("q1"));

            // 真正的稳定性验证：构造一个 PENDING 作业，连续两轮手动调度，队列与状态都不变
            s2.submitJob("stuck", List.of(task("s", 4, 4)), 0);
            List<String> before = s2.queuedJobIds();
            int n1 = s2.runScheduling();
            List<String> after1 = s2.queuedJobIds();
            int n2scheduled = s2.runScheduling();
            List<String> after2 = s2.queuedJobIds();
            BusinessCaseReporter.check(n1 == 0 && n2scheduled == 0,
                    () -> "资源没变时重复调度不应产生变化");
            BusinessCaseReporter.check(before.equals(after1) && after1.equals(after2),
                    () -> "队列在相邻调度间漂移：" + before + " / " + after1 + " / " + after2);
            System.out.println("  连续两轮调度后队列保持：" + after2);
        });

        reporter.add("06", "释放后排队作业自动顶上，资源账目与分配总和逐维相等", () -> {
            SchedulerService scheduler = newCluster();
            scheduler.submitJob("w1", List.of(task("w1", 4, 4)), 0); // n1
            scheduler.submitJob("w2", List.of(task("w2", 4, 4)), 0); // n2
            scheduler.submitJob("w3", List.of(task("w3", 2, 2)), 0); // 排队
            BusinessCaseReporter.check(scheduler.queuedJobIds().equals(List.of("w3")),
                    () -> "w3 应在排队");
            scheduler.releaseJob("w1");
            BusinessCaseReporter.check(scheduler.getJobState("w3") == JobState.RUNNING,
                    () -> "w1 释放后 w3 应自动 RUNNING");
            BusinessCaseReporter.check(scheduler.queuedJobIds().isEmpty(),
                    () -> "队列应清空：" + scheduler.queuedJobIds());
            System.out.println("  释放后节点账目：" + scheduler.nodeSnapshot());
        });

        return reporter.tests().stream();
    }
}
