package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务：并发提交/调度/释放 —— 同一份资源不能被重复分配，账目在任何交错顺序下保持一致。
 */
class ConcurrentAccessTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentAccessTest.class);

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

    private int totalUsedCpu() {
        return scheduler.snapshot().nodes().stream()
                .mapToInt(ClusterSnapshot.NodeView::cpuUsed).sum();
    }

    private boolean accountsNonNegative() {
        return scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuUsed() >= 0 && n.cpuAvailable() >= 0
                        && n.memoryUsedMb() >= 0 && n.memoryAvailableMb() >= 0);
    }

    @Test
    @DisplayName("并发提交+调度：同一资源不会被分配两次")
    void concurrentSubmitAndSchedule() throws InterruptedException {
        String business = "并发提交与调度的互斥";
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();

        // 每个线程提交一个独占 8CPU 的作业（集群共 24CPU），并立即尝试调度。
        // 交错执行下最多只能有 3 个作业真正落位；重复分配会让账目超过 24。
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitResult r = scheduler.submitJob("job-" + idx, 1, 1, new Resource(8, 1024));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    }
                    scheduler.schedulePendingJobs();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        // 收尾：队列里剩余作业再统一调度一轮
        scheduler.schedulePendingJobs();

        long running = scheduler.snapshot().runningJobIds().size();
        int usedCpu = totalUsedCpu();
        boolean case1 = running == 3;
        report(business, "16 个 8CPU 作业并发提交+调度，恰好 3 个运行、其余排队",
                case1, "running=" + running + ", accepted=" + accepted.get());
        boolean case2 = usedCpu == 24;
        report(business, "已用 CPU 恰好等于集群总量 24，没有资源被重复分配",
                case2, "usedCpu=" + usedCpu);
        boolean case3 = accountsNonNegative();
        report(business, "并发交错过程结束后所有节点账目非负", case3,
                "节点=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + " used=" + n.cpuUsed()).toList());
        boolean case4 = scheduler.snapshot().queuedJobIds().size() == 13;
        report(business, "未落位的 13 个作业稳定停留在队列中", case4,
                "queued=" + scheduler.snapshot().queuedJobIds().size());
    }

    @Test
    @DisplayName("并发完成：释放幂等，重复/乱序释放不改变最终账目")
    void concurrentReleaseIsIdempotent() throws InterruptedException {
        String business = "并发释放幂等与顺序无关";

        // 准备 3 个各占 8CPU 的运行中作业
        scheduler.submitJob("j0", 1, 1, new Resource(8, 1024));
        scheduler.submitJob("j1", 1, 1, new Resource(8, 1024));
        scheduler.submitJob("j2", 1, 1, new Resource(8, 1024));
        scheduler.schedulePendingJobs();
        report(business, "前置：3 个作业整批落位、24CPU 全占用",
                totalUsedCpu() == 24, "usedCpu=" + totalUsedCpu());

        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger trueReleases = new AtomicInteger();

        // 每个作业被 4 个线程同时释放（complete/fail 混合），只有一次应真正生效
        for (int t = 0; t < threads; t++) {
            final String jobId = "j" + (t % 3);
            final boolean fail = t % 2 == 0;
            pool.submit(() -> {
                try {
                    start.await();
                    boolean changed = fail
                            ? scheduler.failJob(jobId, "concurrent-failure")
                            : scheduler.completeJob(jobId);
                    if (changed) {
                        trueReleases.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        boolean case1 = trueReleases.get() == 3;
        report(business, "12 个线程混合 complete/fail，只有 3 次真正释放",
                case1, "真正生效次数=" + trueReleases.get());
        boolean case2 = totalUsedCpu() == 0;
        report(business, "重复释放没有把可用资源越算越多：已用 CPU 归 0、可用 24",
                case2, "usedCpu=" + totalUsedCpu());
        boolean case3 = scheduler.snapshot().nodes().stream()
                .allMatch(n -> n.cpuAvailable() == 8);
        report(business, "每个节点可用资源恢复为 8，与释放先后顺序无关", case3,
                "available=" + scheduler.snapshot().nodes().stream()
                        .map(n -> n.nodeId() + ":" + n.cpuAvailable()).toList());

        // 终态作业再次释放应全部返回 false，账目保持不变
        boolean repeatReturnsFalse = !scheduler.completeJob("j0")
                && !scheduler.failJob("j1", "again") && totalUsedCpu() == 0;
        report(business, "终态作业重复释放返回 false 且账目不变", repeatReturnsFalse,
                "usedCpu=" + totalUsedCpu());
    }

    @Test
    @DisplayName("提交/调度/完成持续交错时，账目恒等式始终成立")
    void interleavedOperationsKeepInvariant() throws InterruptedException {
        String business = "交错操作账目守恒";
        int submitters = 6;
        ExecutorService pool = Executors.newFixedThreadPool(submitters + 1);
        CountDownLatch start = new CountDownLatch(1);
        List<String> violations = new ArrayList<>();

        // 提交者：不断提交 1~4 CPU 的小作业
        for (int i = 0; i < submitters; i++) {
            final int base = i * 100;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < 30; k++) {
                        String id = "w" + base + "-" + k;
                        scheduler.submitJob(id, k % 5 + 1, 1, new Resource((k % 4) + 1, 1024));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        // 调度/回收线程：反复调度，并把运行中的作业完成掉
        pool.submit(() -> {
            try {
                start.await();
                for (int round = 0; round < 200; round++) {
                    scheduler.schedulePendingJobs();
                    List<String> running = new ArrayList<>(scheduler.snapshot().runningJobIds());
                    // 隔一个完成一个，制造并发交错
                    for (int i = round % 2; i < running.size(); i += 2) {
                        scheduler.completeJob(running.get(i));
                    }
                    int used = scheduler.snapshot().nodes().stream()
                            .mapToInt(ClusterSnapshot.NodeView::cpuUsed).sum();
                    if (used < 0 || used > 24 || !accountsNonNegative()) {
                        violations.add("round " + round + " used=" + used);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        start.countDown();
        pool.shutdown();
        boolean finished = pool.awaitTermination(30, TimeUnit.SECONDS);
        // 清场：全部完成
        scheduler.schedulePendingJobs();
        scheduler.snapshot().runningJobIds().forEach(scheduler::completeJob);
        scheduler.schedulePendingJobs();

        boolean case1 = finished && violations.isEmpty();
        report(business, "交错 200 轮中账目恒满足 0<=已用CPU<=24 且无负值",
                case1, "violations=" + violations);
        boolean case2 = totalUsedCpu() <= 24 && accountsNonNegative();
        report(business, "交错结束后账目仍守恒、非负", case2, "usedCpu=" + totalUsedCpu());
    }
}
