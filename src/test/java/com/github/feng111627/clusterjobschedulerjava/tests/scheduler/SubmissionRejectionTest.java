package com.github.feng111627.clusterjobschedulerjava.tests.scheduler;

import com.github.feng111627.clusterjobschedulerjava.scheduler.JobState;
import com.github.feng111627.clusterjobschedulerjava.scheduler.RejectReason;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Resources;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SchedulerService;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SubmissionResult;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Task;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

/**
 * 业务一：作业提交与排队 —— 各种拒绝路径必须被区分，且被拒绝的作业不进队列、不占资源。
 *
 * <p>集群固定为两台 4C/4G 的节点：集群总容量 8C/8G，单节点上限 4C/4G。
 */
public class SubmissionRejectionTest {

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
        BusinessCaseReporter reporter = new BusinessCaseReporter("作业提交：拒绝原因可区分");
        SchedulerService scheduler = newCluster();

        reporter.add("01", "单任务超过最大单节点容量 -> TASK_EXCEEDS_LARGEST_NODE", () -> {
            SubmissionResult result = scheduler.submitJob("j-too-big",
                    List.of(task("t1", 6, 1)), 0);
            BusinessCaseReporter.check(!result.accepted(), () -> "应被拒绝");
            BusinessCaseReporter.check(result.rejectReason() == RejectReason.TASK_EXCEEDS_LARGEST_NODE,
                    () -> "实际原因：" + result.rejectReason());
            BusinessCaseReporter.check(scheduler.getJobState("j-too-big") == null,
                    () -> "被拒绝的作业不应进入系统");
        });

        reporter.add("02", "多任务中某一个超过单节点容量 -> 同样按单节点原因拒绝", () -> {
            SubmissionResult result = scheduler.submitJob("j-one-bad",
                    List.of(task("t1", 2, 2), task("t2", 5, 1)), 0);
            BusinessCaseReporter.check(!result.accepted() && result.rejectReason()
                    == RejectReason.TASK_EXCEEDS_LARGEST_NODE, () -> "实际结果：" + result);
        });

        reporter.add("03", "每个任务单节点放得下，但总和超过集群总容量 -> JOB_EXCEEDS_CLUSTER_CAPACITY", () -> {
            // 三个 4C 任务：单台都放得下，但 12C > 集群总容量 8C
            SubmissionResult result = scheduler.submitJob("j-cluster-full",
                    List.of(task("t1", 4, 1), task("t2", 4, 1), task("t3", 4, 1)), 0);
            BusinessCaseReporter.check(!result.accepted(), () -> "应被拒绝");
            BusinessCaseReporter.check(result.rejectReason() == RejectReason.JOB_EXCEEDS_CLUSTER_CAPACITY,
                    () -> "实际原因：" + result.rejectReason());
        });

        reporter.add("04", "内存维度总量超集群容量也能识别（逐维比较，不只看 CPU）", () -> {
            // 每个任务 4G 内存，单台节点放得下；三个共 12G > 集群总内存 8G
            SubmissionResult result = scheduler.submitJob("j-mem",
                    List.of(task("t1", 1, 4), task("t2", 1, 4), task("t3", 1, 4)), 0);
            BusinessCaseReporter.check(!result.accepted()
                            && result.rejectReason() == RejectReason.JOB_EXCEEDS_CLUSTER_CAPACITY,
                    () -> "12G 内存超过集群 8G，实际结果：" + result);
        });

        reporter.add("05", "空任务列表 -> INVALID_REQUEST", () -> {
            SubmissionResult result = scheduler.submitJob("j-empty", List.of(), 0);
            BusinessCaseReporter.check(!result.accepted()
                            && result.rejectReason() == RejectReason.INVALID_REQUEST,
                    () -> "实际结果：" + result);
        });

        reporter.add("06", "重复的作业 id -> INVALID_REQUEST", () -> {
            SubmissionResult first = scheduler.submitJob("j-dup",
                    List.of(task("t-dup-1", 1, 1)), 0);
            SubmissionResult second = scheduler.submitJob("j-dup",
                    List.of(task("t-dup-2", 1, 1)), 0);
            BusinessCaseReporter.check(first.accepted(), () -> "第一次提交应接受");
            BusinessCaseReporter.check(!second.accepted()
                            && second.rejectReason() == RejectReason.INVALID_REQUEST,
                    () -> "重复 id 应拒绝，实际：" + second);
        });

        reporter.add("07", "被拒绝的作业不占用任何资源，后续正常作业照常调度", () -> {
            scheduler.submitJob("j-bad", List.of(task("t-x", 99, 1)), 0);
            SubmissionResult ok = scheduler.submitJob("j-ok",
                    List.of(task("t-ok", 3, 3)), 0);
            BusinessCaseReporter.check(ok.accepted(), () -> "正常作业应被接受");
            BusinessCaseReporter.check(scheduler.getJobState("j-ok") == JobState.RUNNING,
                    () -> "资源空闲，应立即 RUNNING，实际：" + scheduler.getJobState("j-ok"));
        });

        return reporter.tests().stream();
    }
}
