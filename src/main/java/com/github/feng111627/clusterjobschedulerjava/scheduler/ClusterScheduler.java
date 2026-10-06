package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.Job;
import com.github.feng111627.clusterjobschedulerjava.model.JobState;
import com.github.feng111627.clusterjobschedulerjava.model.NodeState;
import com.github.feng111627.clusterjobschedulerjava.model.RejectReason;
import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import com.github.feng111627.clusterjobschedulerjava.model.TaskPlacement;
import com.github.feng111627.clusterjobschedulerjava.model.WorkerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存集群调度器：管理节点、等待队列、整批调度与资源释放。
 *
 * <p>所有公共方法都在同一把锁 {@code lock} 下串行执行，因此：
 * <ul>
 *   <li>并发提交/调度/释放不会把同一份资源分配两次；</li>
 *   <li>“整批任务要么全部落位、要么一个都不占”的决策与提交在同一个临界区完成；</li>
 *   <li>释放动作幂等：作业只有处于 RUNNING 时才会真正归还资源，重复释放不生效。</li>
 * </ul>
 */
public class ClusterScheduler {

    private static final Logger log = LoggerFactory.getLogger(ClusterScheduler.class);

    private final Object lock = new Object();

    /** 节点表，使用 LinkedHashMap 保持注册顺序，作为装箱时稳定的节点遍历顺序。 */
    private final Map<String, WorkerNode> nodes = new LinkedHashMap<>();
    /** 全部作业（含已结束/被拒绝的），便于追溯。 */
    private final Map<String, Job> jobs = new HashMap<>();
    /** 等待队列，保持提交顺序；真正的调度顺序在每轮调度时按规则排序得到。 */
    private final List<Job> pendingQueue = new ArrayList<>();

    private long sequenceGenerator = 0;

    public ClusterScheduler() {
    }

    /** 注册一个节点。重复 id 会被拒绝。 */
    public void registerNode(String nodeId, Resource capacity) {
        synchronized (lock) {
            if (nodes.containsKey(nodeId)) {
                throw new IllegalArgumentException("节点已存在: " + nodeId);
            }
            nodes.put(nodeId, new WorkerNode(nodeId, capacity));
            log.info("注册节点 {} 容量{}", nodeId, capacity);
        }
    }

    /**
     * 提交作业。做两道结构性容量校验（与当前占用无关，只取决于集群拓扑）：
     * <ol>
     *   <li>单任务需求超过最大节点容量 → {@link RejectReason#TASK_EXCEEDS_LARGEST_NODE}；</li>
     *   <li>全部任务总需求超过集群总容量 → {@link RejectReason#TOTAL_EXCEEDS_CLUSTER_CAPACITY}。</li>
     * </ol>
     * 通过后作业以 PENDING 进入等待队列尾部，本方法不触发调度。
     */
    public SubmitResult submitJob(String jobId, int priority, int taskCount, Resource taskResource) {
        if (taskCount <= 0) {
            throw new IllegalArgumentException("任务数必须为正: " + taskCount);
        }
        if (taskResource == null || taskResource.isZero()) {
            throw new IllegalArgumentException("单任务资源需求必须为正数");
        }
        synchronized (lock) {
            if (jobs.containsKey(jobId)) {
                throw new IllegalArgumentException("作业已存在: " + jobId);
            }

            Resource clusterCapacity = Resource.ZERO;
            Resource largestNode = Resource.ZERO;
            for (WorkerNode node : nodes.values()) {
                clusterCapacity = clusterCapacity.add(node.getCapacity());
                if (node.getCapacity().getCpu() > largestNode.getCpu()
                        || (node.getCapacity().getCpu() == largestNode.getCpu()
                        && node.getCapacity().getMemoryMb() > largestNode.getMemoryMb())) {
                    largestNode = node.getCapacity();
                }
            }

            if (!largestNode.canFit(taskResource)) {
                String msg = "单任务需求" + taskResource + "超过集群最大节点容量" + largestNode;
                log.info("拒绝作业 {}: {}", jobId, msg);
                Job rejected = new Job(jobId, priority, taskCount, taskResource, -1L);
                rejected.setState(JobState.REJECTED);
                rejected.setRejectReason(RejectReason.TASK_EXCEEDS_LARGEST_NODE);
                jobs.put(jobId, rejected);
                return SubmitResult.rejected(RejectReason.TASK_EXCEEDS_LARGEST_NODE, msg);
            }

            Resource total = new Resource(
                    taskResource.getCpu() * taskCount,
                    taskResource.getMemoryMb() * taskCount);
            if (!clusterCapacity.canFit(total)) {
                String msg = "总需求" + total + "超过集群总容量" + clusterCapacity;
                log.info("拒绝作业 {}: {}", jobId, msg);
                Job rejected = new Job(jobId, priority, taskCount, taskResource, -1L);
                rejected.setState(JobState.REJECTED);
                rejected.setRejectReason(RejectReason.TOTAL_EXCEEDS_CLUSTER_CAPACITY);
                jobs.put(jobId, rejected);
                return SubmitResult.rejected(RejectReason.TOTAL_EXCEEDS_CLUSTER_CAPACITY, msg);
            }

            long seq = ++sequenceGenerator;
            Job job = new Job(jobId, priority, taskCount, taskResource, seq);
            jobs.put(jobId, job);
            pendingQueue.add(job);
            log.info("作业 {} 已入队: priority={}, tasks={}, 单任务{}, 总需求{}, 队列序号={}",
                    jobId, priority, taskCount, taskResource, total, seq);
            return SubmitResult.accepted(job);
        }
    }

    /**
     * 执行一轮调度。
     *
     * <p>排序规则（稳定、可解释）：优先级数值大者优先；优先级相同时，按提交序号（即提交先后）
     * 排在前面。节点遍历顺序固定为节点注册顺序，装箱采用首次适应，因此在相同集群状态下，
     * 相邻两轮调度的结论必然一致，不会出现莫名抖动。
     *
     * <p>每个作业先在“影子账目”上为其全部任务试摆：所有任务都能放下才真正提交分配；
     * 任意一个任务放不下则整批放弃，本作业一个资源都不会占用。
     */
    public ScheduleResult schedulePendingJobs() {
        synchronized (lock) {
            List<Job> ordered = new ArrayList<>(pendingQueue);
            ordered.sort(Comparator.comparingInt(Job::getPriority).reversed()
                    .thenComparingLong(Job::getSubmitSequence));

            List<String> scheduledIds = new ArrayList<>();
            List<String> queuedIds = new ArrayList<>();

            for (Job job : ordered) {
                List<TaskPlacement> plan = tryPlaceAllTasks(job);
                if (plan == null) {
                    queuedIds.add(job.getId());
                    log.info("调度轮次: 作业 {} 暂放不下，留在队列", job.getId());
                    continue;
                }
                commitPlacement(job, plan);
                scheduledIds.add(job.getId());
                pendingQueue.remove(job);
                log.info("调度轮次: 作业 {} 整批调度成功, {} 个任务落位: {}",
                        job.getId(), job.getTaskCount(), describePlan(plan));
            }
            return new ScheduleResult(scheduledIds, queuedIds);
        }
    }

    /**
     * 在影子账目上为作业的全部任务试摆。全部成功返回落点计划；任一任务放不下返回 null，
     * 此时没有任何节点的真实账目被修改。
     */
    private List<TaskPlacement> tryPlaceAllTasks(Job job) {
        Map<String, Resource> simulatedUsed = new HashMap<>();
        for (WorkerNode node : nodes.values()) {
            simulatedUsed.put(node.getId(), node.getUsed());
        }

        List<TaskPlacement> plan = new ArrayList<>(job.getTaskCount());
        for (int i = 0; i < job.getTaskCount(); i++) {
            String chosen = null;
            for (WorkerNode node : nodes.values()) {
                if (!node.isOnline()) {
                    continue;
                }
                Resource projected = simulatedUsed.get(node.getId()).add(job.getTaskResource());
                if (node.getCapacity().canFit(projected)) {
                    chosen = node.getId();
                    simulatedUsed.put(chosen, projected);
                    break;
                }
            }
            if (chosen == null) {
                return null;
            }
            plan.add(new TaskPlacement(job.getId(), i, chosen, job.getTaskResource()));
        }
        return plan;
    }

    /** 将试摆计划真正写入节点账目并把作业置为 RUNNING。 */
    private void commitPlacement(Job job, List<TaskPlacement> plan) {
        for (TaskPlacement p : plan) {
            WorkerNode node = nodes.get(p.nodeId());
            node.allocate(p.resource());
        }
        job.setPlacements(List.copyOf(plan));
        job.setState(JobState.RUNNING);
    }

    /**
     * 正常完成作业：归还其全部任务占用的资源。
     * 只有 RUNNING 作业会真正释放，重复调用返回 false 且不改动账目（幂等）。
     */
    public boolean completeJob(String jobId) {
        synchronized (lock) {
            Job job = jobs.get(jobId);
            if (job == null || job.getState() != JobState.RUNNING) {
                return false;
            }
            releaseAllTasks(job, JobState.COMPLETED);
            log.info("作业 {} 已完成，资源全部归还", jobId);
            return true;
        }
    }

    /**
     * 任务失败：语义与完成一致——归还该作业占用的全部资源，作业标记为 FAILED。
     * 同样幂等。
     */
    public boolean failJob(String jobId, String reason) {
        synchronized (lock) {
            Job job = jobs.get(jobId);
            if (job == null || job.getState() != JobState.RUNNING) {
                return false;
            }
            releaseAllTasks(job, JobState.FAILED);
            log.info("作业 {} 失败（{}），资源全部归还", jobId, reason);
            return true;
        }
    }

    /**
     * 节点掉线：节点置为 OFFLINE，其上所有作业整体失败并释放这些作业占用的
     * <em>全部</em>资源（含分布在其他节点上的任务），符合“整批作业”的原子语义。
     * 返回受影响（被置为 FAILED）的作业数。
     */
    public int markNodeOffline(String nodeId) {
        synchronized (lock) {
            WorkerNode node = nodes.get(nodeId);
            if (node == null || !node.isOnline()) {
                return 0;
            }
            node.setState(NodeState.OFFLINE);

            List<Job> affected = new ArrayList<>();
            for (Job job : jobs.values()) {
                if (job.getState() != JobState.RUNNING) {
                    continue;
                }
                for (TaskPlacement p : job.getPlacements()) {
                    if (p.nodeId().equals(nodeId)) {
                        affected.add(job);
                        break;
                    }
                }
            }
            for (Job job : affected) {
                releaseAllTasks(job, JobState.FAILED);
            }
            log.info("节点 {} 掉线，受影响失败作业 {} 个: {}", nodeId, affected.size(),
                    affected.stream().map(Job::getId).toList());
            return affected.size();
        }
    }

    /** 节点重新上线。此前掉线时其资源已全部归还，因此上线时账目自然为 0 占用。 */
    public boolean markNodeOnline(String nodeId) {
        synchronized (lock) {
            WorkerNode node = nodes.get(nodeId);
            if (node == null || node.isOnline()) {
                return false;
            }
            node.setState(NodeState.ONLINE);
            log.info("节点 {} 重新上线，当前已用资源{}", nodeId, node.getUsed());
            return true;
        }
    }

    public NodeState nodeState(String nodeId) {
        synchronized (lock) {
            WorkerNode node = nodes.get(nodeId);
            return node == null ? null : node.getState();
        }
    }

    /** 只读查看作业（返回的是内部引用，调用方不应修改）。 */
    public Job getJob(String jobId) {
        synchronized (lock) {
            return jobs.get(jobId);
        }
    }

    /**
     * 归还作业全部任务的资源并把作业迁移到终态。
     *
     * <p>释放顺序不影响最终账目：每个任务只从它所在节点扣减它自己当初申请的那一份资源；
     * 加法满足交换律，且 {@link WorkerNode#release} 不允许扣成负数，
     * 因此先放后放、重复释放（终态作业不会进入本方法）都得到同一个结果。
     */
    private void releaseAllTasks(Job job, JobState terminalState) {
        for (TaskPlacement p : job.getPlacements()) {
            WorkerNode node = nodes.get(p.nodeId());
            if (node != null) {
                node.release(p.resource());
            }
        }
        job.setPlacements(List.of());
        job.setState(terminalState);
    }

    private String describePlan(List<TaskPlacement> plan) {
        StringBuilder sb = new StringBuilder();
        for (TaskPlacement p : plan) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append("task#").append(p.taskIndex()).append("->").append(p.nodeId());
        }
        return sb.toString();
    }

    /** 生成集群与队列的只读快照。 */
    public ClusterSnapshot snapshot() {
        synchronized (lock) {
            List<String> queued = pendingQueue.stream().map(Job::getId).toList();
            List<String> running = jobs.values().stream()
                    .filter(j -> j.getState() == JobState.RUNNING)
                    .map(Job::getId)
                    .sorted()
                    .toList();
            List<ClusterSnapshot.NodeView> nodeViews = new ArrayList<>();
            for (WorkerNode node : nodes.values()) {
                Resource available = node.getAvailable();
                nodeViews.add(new ClusterSnapshot.NodeView(
                        node.getId(),
                        node.getState().name(),
                        node.getCapacity().getCpu(),
                        node.getUsed().getCpu(),
                        available.getCpu(),
                        node.getCapacity().getMemoryMb(),
                        node.getUsed().getMemoryMb(),
                        available.getMemoryMb()));
            }
            return new ClusterSnapshot(List.copyOf(queued), List.copyOf(running), nodeViews);
        }
    }
}
