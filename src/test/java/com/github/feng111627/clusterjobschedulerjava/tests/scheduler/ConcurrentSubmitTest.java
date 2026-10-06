package com.github.feng111627.clusterjobschedulerjava.tests.scheduler;

import com.github.feng111627.clusterjobschedulerjava.scheduler.JobState;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Resources;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SchedulerService;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Task;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * 业务三：并发提交与并发释放。
 *
 * <p>验证同一份资源不会被重复分配：同一时刻运行中的作业所占用资源总和
 * 不能超过节点容量；并发提交后"运行数 + 排队数"必须恰好等于接受数。
 */
public class ConcurrentSubmitTest {

    private SchedulerService newCluster() {
        SchedulerService scheduler = new SchedulerService();
        // 4 台节点，每台 4C：集群共 16C
        scheduler.registerNode("n1", new Resources(4, 0, 0));
        scheduler.registerNode("n2", new Resources(4, 0, 0));
        scheduler.registerNode("n3", new Resources(4, 0, 0));
        scheduler.registerNode("n4", new Resources(4, 0, 0));
        return scheduler;
    }

    /** 按作业当前分配重新汇总各节点已分配 CPU（独立于服务内部账目交叉核对）。 */
    private Map<String, Integer> cpuByNodeFromAssignments(SchedulerService scheduler,
                                                          List<String> jobIds,
                                                          JobState expectedState) {
        Map<String, Integer> used = new java.util.HashMap<>();
        for (String jobId : jobIds) {
            if (scheduler.getJobState(jobId) != expectedState) {
                continue;
            }
            for (String nodeId : scheduler.getTaskAssignments(jobId).values()) {
                if (nodeId != null) {
                    used.merge(nodeId, 4, Integer::sum);
                }
            }
        }
        return used;
    }

    @TestFactory
    Stream<DynamicTest> cases() {
        BusinessCaseReporter reporter = new BusinessCaseReporter("并发提交与并发释放");

        reporter.add("01", "20 个作业并发提交（每作业独占 4C）：接受 20、运行 4、排队 16", () -> {
            SchedulerService scheduler = newCluster();
            int threads = 20;
            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicInteger accepted = new AtomicInteger();
            List<String> jobIds = java.util.Collections.synchronizedList(new ArrayList<>());

            for (int i = 0; i < threads; i++) {
                String jobId = "job-" + i;
                jobIds.add(jobId);
                pool.submit(() -> {
                    try {
                        start.await();
                        boolean ok = scheduler.submitJob(jobId,
                                List.of(new Task("task-" + jobId, new Resources(4, 0, 0))), 0).accepted();
                        if (ok) {
                            accepted.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            BusinessCaseReporter.check(done.await(10, TimeUnit.SECONDS), () -> "并发提交应在 10s 内完成");
            pool.shutdown();

            BusinessCaseReporter.check(accepted.get() == 20,
                    () -> "单任务 4C 不超过单节点容量 4C，也未要求作业总量小于在线总容量"
                            + "（总量门槛只拒绝无法成立的作业，不拒绝排队），故 20 个应全部接受，实际："
                            + accepted.get());
            long running = jobIds.stream().filter(j -> scheduler.getJobState(j) == JobState.RUNNING).count();
            long pending = jobIds.stream().filter(j -> scheduler.getJobState(j) == JobState.PENDING).count();
            System.out.println("  运行中=" + running + "，排队中=" + pending + "，队列长度="
                    + scheduler.queuedJobIds().size());
            BusinessCaseReporter.check(running == 4, () -> "集群 16C / 每作业 4C，恰好 4 个运行，实际：" + running);
            BusinessCaseReporter.check(pending == 16, () -> "其余 16 个应排队，实际：" + pending);
            BusinessCaseReporter.check(scheduler.queuedJobIds().size() == 16,
                    () -> "队列长度应为 16，实际：" + scheduler.queuedJobIds().size());

            Map<String, Integer> used = cpuByNodeFromAssignments(scheduler, jobIds, JobState.RUNNING);
            used.forEach((node, cpu) -> BusinessCaseReporter.check(cpu <= 4,
                    () -> node + " 超卖：" + cpu + "C > 4C"));
            int totalInUse = used.values().stream().mapToInt(Integer::intValue).sum();
            BusinessCaseReporter.check(totalInUse == 16,
                    () -> "四个节点应恰好占满 16C，实际：" + totalInUse);
            System.out.println("  并发提交后各节点占用（按分配交叉核对）：" + used);
        });

        reporter.add("02", "4 个运行中作业并发释放 + 4 个排队作业并发再提交，账目始终不超卖", () -> {
            SchedulerService scheduler = newCluster();
            List<String> initial = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                String id = "init-" + i;
                initial.add(id);
                scheduler.submitJob(id, List.of(new Task("t-" + id, new Resources(4, 0, 0))), 0);
            }
            // 4 运行 + 4 排队
            BusinessCaseReporter.check(scheduler.queuedJobIds().size() == 4,
                    () -> "初始队列应为 4，实际：" + scheduler.queuedJobIds().size());

            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(8);
            // 前 4 个线程释放运行中的作业，后 4 个线程再提交新作业
            for (int i = 0; i < 8; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        if (idx < 4) {
                            scheduler.releaseJob("init-" + idx);
                        } else {
                            String id = "more-" + idx;
                            scheduler.submitJob(id,
                                    List.of(new Task("t-" + id, new Resources(4, 0, 0))), 0);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            BusinessCaseReporter.check(done.await(10, TimeUnit.SECONDS), () -> "并发操作应在 10s 内完成");
            pool.shutdown();

            List<String> all = new ArrayList<>(initial);
            for (int i = 4; i < 8; i++) {
                all.add("more-" + i);
            }
            long running = all.stream().filter(j -> scheduler.getJobState(j) == JobState.RUNNING).count();
            long pending = all.stream().filter(j -> scheduler.getJobState(j) == JobState.PENDING).count();
            System.out.println("  混合并发后：运行中=" + running + "，排队中=" + pending);
            BusinessCaseReporter.check(running == 4, () -> "集群容量不变，运行中应恒为 4，实际：" + running);
            BusinessCaseReporter.check(running + pending == 8,
                    () -> "运行+排队应恒为 8（无丢失、无重复分配），实际：" + (running + pending));

            Map<String, Integer> used = cpuByNodeFromAssignments(scheduler, all, JobState.RUNNING);
            used.forEach((node, cpu) -> BusinessCaseReporter.check(cpu <= 4,
                    () -> node + " 超卖：" + cpu + "C"));
            int totalInUse = used.values().stream().mapToInt(Integer::intValue).sum();
            BusinessCaseReporter.check(totalInUse == 16,
                    () -> "资源应恰好占满 16C，实际：" + totalInUse);
            System.out.println("  混合并发后节点账目快照：" + scheduler.nodeSnapshot());
        });

        return reporter.tests().stream();
    }
}
