package com.github.feng111627.clusterjobschedulerjava.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 本地集群作业调度与队列管理（纯内存实现，线程安全）。
 *
 * <p>设计要点：
 * <ul>
 *   <li>所有对节点账目、作业队列和索引的读写都在同一把 {@link ReentrantLock} 下完成，
 *       因此并发提交 / 释放 / 调度是串行化的，同一份资源不可能被两个作业重复分配；</li>
 *   <li>队列按 (priority 降序, sequence 升序) 排序，sequence 为提交时的单调序号，
 *       调度顺序稳定、可解释；</li>
 *   <li>作业内多任务整批调度：逐个尝试放到第一台放得下的节点，任意一个放不下，
 *       已放好的任务全部归还，作业留在队列，节点账目恢复原样；</li>
 *   <li>释放（作业完成 / 节点掉线 / 任务失败）都幂等：找不到或已释放直接跳过，
 *       重复释放不会让可用资源变大，释放先后顺序不影响最终账目；</li>
 *   <li>每次成功的提交 / 释放 / 掉线后自动再跑一轮调度，完整模拟
 *       "提交 → 排队 → 调度 → 释放 → 再调度" 的过程。</li>
 * </ul>
 */
public class SchedulerService {

    private static final Logger log = LoggerFactory.getLogger(SchedulerService.class);

    /** 队列排序器：优先级大的在前，同优先级按提交序号小的在前。 */
    private static final Comparator<Job> QUEUE_ORDER =
            Comparator.comparingInt(Job::getPriority).reversed()
                    .thenComparingLong(Job::getSequence);

    // 公平锁：并发提交时按线程到达顺序分配锁，提交序号与排队顺序直观一致。
    private final ReentrantLock lock = new ReentrantLock(true);

    /** nodeId -> 节点，TreeMap 保证遍历顺序稳定（按节点 id 字典序）。 */
    private final NavigableMap<String, Node> nodes = new TreeMap<>();
    /** jobId -> 作业（含已结束的作业，便于查询最终状态）。 */
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    /** taskId -> 任务，全局唯一，供 taskFailed 快速定位。 */
    private final Map<String, Task> taskIndex = new LinkedHashMap<>();
    /** taskId -> 所属 jobId。 */
    private final Map<String, String> taskToJob = new LinkedHashMap<>();

    private final List<Job> queue = new ArrayList<>();
    private long sequenceGenerator = 0;

    public SchedulerService() {
    }

    // ------------------------------------------------------------------
    // 节点管理
    // ------------------------------------------------------------------

    /** 注册（或重新上线）一台节点。重复注册同一 id 会被拒绝，避免账目被意外清零。 */
    public void registerNode(String nodeId, Resources capacity) {
        lock.lock();
        try {
            if (nodes.containsKey(nodeId)) {
                throw new IllegalArgumentException("node already registered: " + nodeId);
            }
            Node node = new Node(nodeId, capacity);
            nodes.put(nodeId, node);
            log.info("[node] register {} capacity={}", nodeId, capacity);
            scheduleInternal("registerNode:" + nodeId);
        } finally {
            lock.unlock();
        }
    }

    /** 节点重新上线（容量以当前注册值为准，账目从 0 开始），随后触发一次调度。 */
    public void nodeOnline(String nodeId) {
        lock.lock();
        try {
            Node node = requireNode(nodeId);
            if (node.isOnline()) {
                log.info("[node] online skipped (already online): {}", nodeId);
                return;
            }
            node.setOnline(true);
            log.info("[node] online {} available={}", nodeId, node.getAvailable());
            scheduleInternal("nodeOnline:" + nodeId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 节点掉线：释放它上面所有任务占用的资源，对应作业标记为 FAILED 并移出队列/运行集合，
     * 然后节点置为离线。重复掉线是幂等的。
     */
    public void nodeOffline(String nodeId) {
        lock.lock();
        try {
            Node node = requireNode(nodeId);
            if (!node.isOnline()) {
                log.info("[node] offline skipped (already offline): {}", nodeId);
                return;
            }
            // 收集该节点上的所有任务（每个作业取一个代表即可，随后按作业整体处理）。
            Map<String, List<Task>> tasksByJob = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : taskToJob.entrySet()) {
                Task task = taskIndex.get(entry.getKey());
                if (nodeId.equals(task.getAssignedNodeId())) {
                    tasksByJob.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(task);
                }
            }
            for (Map.Entry<String, List<Task>> entry : tasksByJob.entrySet()) {
                Job job = jobs.get(entry.getKey());
                releaseJobInternal(job, "nodeOffline:" + nodeId);
                job.markFailed();
                log.info("[node] offline {} -> job {} marked FAILED", nodeId, job.getId());
            }
            node.resetAllocation();
            node.setOnline(false);
            log.info("[node] offline {} (released {} affected jobs)", nodeId, tasksByJob.size());
            // 掉线会连带释放同机/跨节点作业占有的资源，腾出的容量让队列里的作业补位。
            scheduleInternal("nodeOffline:" + nodeId);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 作业提交
    // ------------------------------------------------------------------

    /**
     * 提交作业并立即尝试调度。
     *
     * @return accepted=true 表示已入队（可能当场被调度运行）；
     *         accepted=false 时通过 {@link RejectReason} 区分拒绝原因
     */
    public SubmissionResult submitJob(String jobId, List<Task> tasks, int priority) {
        lock.lock();
        try {
            // 1) 参数校验
            if (jobId == null || jobId.isBlank()) {
                return reject(RejectReason.INVALID_REQUEST, "job id must not be blank");
            }
            if (jobs.containsKey(jobId)) {
                return reject(RejectReason.INVALID_REQUEST, "duplicated job id: " + jobId);
            }
            if (tasks == null || tasks.isEmpty()) {
                return reject(RejectReason.INVALID_REQUEST, "job must contain at least one task");
            }
            for (Task task : tasks) {
                if (task == null) {
                    return reject(RejectReason.INVALID_REQUEST, "task must not be null");
                }
                if (taskIndex.containsKey(task.getId())) {
                    return reject(RejectReason.INVALID_REQUEST, "duplicated task id: " + task.getId());
                }
            }

            // 2) 单任务需求不能超过任何一台在线节点的容量
            Resources largest = largestOnlineNodeCapacity();
            for (Task task : tasks) {
                if (!largest.canFit(task.getRequirement())) {
                    return reject(RejectReason.TASK_EXCEEDS_LARGEST_NODE,
                            "task " + task.getId() + " needs " + task.getRequirement()
                                    + ", largest online node capacity is " + largest);
                }
            }

            // 3) 作业总需求不能超过在线集群总容量
            Resources totalCapacity = totalOnlineCapacity();
            Resources required = Resources.ZERO;
            for (Task task : tasks) {
                required = required.plus(task.getRequirement());
            }
            if (!totalCapacity.canFit(required)) {
                return reject(RejectReason.JOB_EXCEEDS_CLUSTER_CAPACITY,
                        "job needs " + required + ", online cluster capacity is " + totalCapacity);
            }

            // 4) 接受入队
            Job job = new Job(jobId, tasks, priority, sequenceGenerator++);
            jobs.put(jobId, job);
            queue.add(job);
            for (Task task : tasks) {
                taskIndex.put(task.getId(), task);
                taskToJob.put(task.getId(), jobId);
            }
            queue.sort(QUEUE_ORDER);
            log.info("[submit] job {} accepted: tasks={}, priority={}, seq={}, totalNeed={}",
                    jobId, tasks.size(), priority, job.getSequence(), required);

            scheduleInternal("submit:" + jobId);
            return SubmissionResult.accepted(jobId);
        } finally {
            lock.unlock();
        }
    }

    private SubmissionResult reject(RejectReason reason, String message) {
        log.info("[submit] rejected reason={} detail={}", reason, message);
        return SubmissionResult.rejected(reason, message);
    }

    // ------------------------------------------------------------------
    // 释放
    // ------------------------------------------------------------------

    /** 作业正常完成：归还全部任务占用的资源。重复释放幂等。 */
    public void releaseJob(String jobId) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                log.info("[release] job {} not found, skip", jobId);
                return;
            }
            if (job.getState() != JobState.RUNNING) {
                log.info("[release] job {} state={}, nothing to release", jobId, job.getState());
                return;
            }
            releaseJobInternal(job, "releaseJob");
            job.markCompleted();
            log.info("[release] job {} COMPLETED", jobId);
            scheduleInternal("releaseJob:" + jobId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 单个任务失败：该作业整体失败并释放它占有的全部资源（同作业其它任务也一并下线），
     * 然后触发调度让队列里的作业顶上。重复上报幂等。
     */
    public void taskFailed(String taskId) {
        lock.lock();
        try {
            String jobId = taskToJob.get(taskId);
            if (jobId == null) {
                log.info("[fail] task {} not found, skip", taskId);
                return;
            }
            Job job = jobs.get(jobId);
            if (job == null) {
                log.info("[fail] task {} references missing job {}, skip", taskId, jobId);
                return;
            }
            if (job.getState() != JobState.RUNNING) {
                log.info("[fail] task {} job {} state={}, nothing to release", taskId, jobId, job.getState());
                return;
            }
            releaseJobInternal(job, "taskFailed:" + taskId);
            job.markFailed();
            log.info("[fail] task {} -> job {} FAILED", taskId, jobId);
            scheduleInternal("taskFailed:" + taskId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放一个运行中作业占有的全部资源。只归还当前确有分配的任务，
     * 因此对同一作业重复调用第二次时无资源可还，账目不会被多加。
     */
    private void releaseJobInternal(Job job, String trigger) {
        List<String> released = new ArrayList<>();
        for (Task task : job.getTasks()) {
            String nodeId = task.getAssignedNodeId();
            if (nodeId == null) {
                continue;
            }
            Node node = nodes.get(nodeId);
            // 节点已被注销的极端情况下，账目已随节点消失，只需清理任务指针。
            if (node != null) {
                node.release(task.getRequirement());
            }
            task.clearAssignment();
            released.add(task.getId() + "@" + nodeId);
        }
        log.info("[release] job {} freed {} task(s) by [{}]: {}",
                job.getId(), released.size(), trigger, released);
    }

    // ------------------------------------------------------------------
    // 调度
    // ------------------------------------------------------------------

    /** 对外暴露的调度入口（一般无需手动调用，提交/释放后会自动调度）。返回本轮新运行的作业数。 */
    public int runScheduling() {
        lock.lock();
        try {
            return scheduleInternal("manual");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按队列顺序逐批调度：每个作业必须所有任务一次性放得下，否则整体跳过、留在队列。
     * 队首暂放不下时不会阻塞后面能放下的作业（避免资源碎片被白白浪费）；
     * 被跳过的作业保留在队列原位置，资源变化后下一轮仍按原顺序参与调度。
     */
    private int scheduleInternal(String trigger) {
        int scheduled = 0;
        // 基于当前队列快照顺序遍历；成功的作业移出队列，跳过的作业留在原位。
        List<Job> snapshot = new ArrayList<>(queue);
        for (Job job : snapshot) {
            if (job.getState() != JobState.PENDING) {
                continue;
            }
            List<Map.Entry<Task, Node>> tentative = tryPlaceAllTasks(job);
            if (tentative == null) {
                log.info("[schedule] job {} cannot fit as a whole now, stays queued (trigger={})",
                        job.getId(), trigger);
                continue;
            }
            commitPlacement(job, tentative);
            queue.remove(job);
            scheduled++;
            log.info("[schedule] job {} RUNNING with placement {} (trigger={})",
                    job.getId(), placementView(tentative), trigger);
        }
        return scheduled;
    }

    /**
     * 尝试把作业的所有任务放到节点上。任一任务放不下 -> 回滚本作业此前的全部试探分配，
     * 返回 null，节点账目与调用前完全一致。
     */
    private List<Map.Entry<Task, Node>> tryPlaceAllTasks(Job job) {
        List<Map.Entry<Task, Node>> tentative = new ArrayList<>();
        for (Task task : job.getTasks()) {
            Node target = null;
            for (Node node : nodes.values()) {
                if (node.isOnline() && node.getAvailable().canFit(task.getRequirement())) {
                    target = node;
                    break;
                }
            }
            if (target == null || !target.tryAllocate(task.getRequirement())) {
                rollback(tentative);
                return null;
            }
            tentative.add(Map.entry(task, target));
        }
        return tentative;
    }

    private void rollback(List<Map.Entry<Task, Node>> tentative) {
        // 逆序归还，保证账目逐维回到调用前；tryAllocate 成功才会进列表，故一定够还。
        for (int i = tentative.size() - 1; i >= 0; i--) {
            Map.Entry<Task, Node> entry = tentative.get(i);
            entry.getValue().release(entry.getKey().getRequirement());
        }
    }

    private void commitPlacement(Job job, List<Map.Entry<Task, Node>> tentative) {
        for (Map.Entry<Task, Node> entry : tentative) {
            entry.getKey().assignTo(entry.getValue().getId());
        }
        job.markRunning();
    }

    private List<String> placementView(List<Map.Entry<Task, Node>> tentative) {
        return tentative.stream()
                .map(e -> e.getKey().getId() + "->" + e.getValue().getId())
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    public JobState getJobState(String jobId) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            return job == null ? null : job.getState();
        } finally {
            lock.unlock();
        }
    }

    /** 返回某作业 taskId -> nodeId 的分配快照（未分配或已结束的作业可能为空）。 */
    public Map<String, String> getTaskAssignments(String jobId) {
        lock.lock();
        try {
            Job job = jobs.get(jobId);
            if (job == null) {
                return Map.of();
            }
            Map<String, String> result = new LinkedHashMap<>();
            for (Task task : job.getTasks()) {
                result.put(task.getId(), task.getAssignedNodeId());
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /** 当前排队作业 id（按调度顺序）。 */
    public List<String> queuedJobIds() {
        lock.lock();
        try {
            return queue.stream().map(Job::getId).collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    /** 节点资源快照：nodeId -> [allocated, available, online]，供校验与日志使用。 */
    public Map<String, String> nodeSnapshot() {
        lock.lock();
        try {
            Map<String, String> snapshot = new LinkedHashMap<>();
            for (Node node : nodes.values()) {
                snapshot.put(node.getId(),
                        "online=" + node.isOnline()
                                + ", allocated=" + node.getAllocated()
                                + ", available=" + node.getAvailable());
            }
            return snapshot;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 内部工具
    // ------------------------------------------------------------------

    private Node requireNode(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("node not registered: " + nodeId);
        }
        return node;
    }

    private Resources largestOnlineNodeCapacity() {
        Resources largest = Resources.ZERO;
        for (Node node : nodes.values()) {
            if (node.isOnline()) {
                largest = largest.max(node.getCapacity());
            }
        }
        return largest;
    }

    private Resources totalOnlineCapacity() {
        Resources total = Resources.ZERO;
        for (Node node : nodes.values()) {
            if (node.isOnline()) {
                total = total.plus(node.getCapacity());
            }
        }
        return total;
    }
}
