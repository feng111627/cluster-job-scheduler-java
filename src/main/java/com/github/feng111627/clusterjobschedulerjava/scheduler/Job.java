package com.github.feng111627.clusterjobschedulerjava.scheduler;

import java.util.List;

/**
 * 作业：由一到多个任务组成，携带优先级。
 *
 * <p>排队顺序规则（稳定、可解释）：优先级数值大的排前面；优先级相同则先提交的
 * （sequence 小）排前面。sequence 由调度器按提交顺序单调分配，因此相邻两次
 * 调度只要队列内容一致，结果顺序就一致。
 *
 * <p>同一作业内的任务要么全部被分配（RUNNING），要么一个都不分配（PENDING）。
 */
public final class Job {

    private final String id;
    private final List<Task> tasks;
    private final int priority;
    private final long sequence;
    private JobState state = JobState.PENDING;
    private RejectReason rejectReason;

    public Job(String id, List<Task> tasks, int priority, long sequence) {
        this.id = id;
        this.tasks = List.copyOf(tasks);
        this.priority = priority;
        this.sequence = sequence;
    }

    public String getId() {
        return id;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public int getPriority() {
        return priority;
    }

    public long getSequence() {
        return sequence;
    }

    public JobState getState() {
        return state;
    }

    public RejectReason getRejectReason() {
        return rejectReason;
    }

    /** 作业所有任务的资源需求总和，用于提交时与集群总容量比较。 */
    public Resources totalRequirement() {
        Resources sum = Resources.ZERO;
        for (Task task : tasks) {
            sum = sum.plus(task.getRequirement());
        }
        return sum;
    }

    public boolean isAllTasksAssigned() {
        return tasks.stream().allMatch(Task::isAssigned);
    }

    boolean markRunning() {
        if (state != JobState.PENDING) {
            return false;
        }
        state = JobState.RUNNING;
        return true;
    }

    void markCompleted() {
        state = JobState.COMPLETED;
    }

    void markFailed() {
        state = JobState.FAILED;
    }

    void markRejected(RejectReason reason) {
        this.state = JobState.REJECTED;
        this.rejectReason = reason;
    }

    @Override
    public String toString() {
        return "Job{" + id + ", priority=" + priority + ", seq=" + sequence
                + ", tasks=" + tasks.size() + ", state=" + state + "}";
    }
}
