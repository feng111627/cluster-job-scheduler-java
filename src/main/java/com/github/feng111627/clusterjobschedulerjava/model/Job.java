package com.github.feng111627.clusterjobschedulerjava.model;

import java.util.List;

/**
 * 一个作业。一个作业由 taskCount 个相同资源需求 {@code taskResource} 的任务组成，
 * 调度时必须整批全部落位，否则一个都不分配。
 */
public final class Job {
    private final String id;
    private final int priority;
    private final int taskCount;
    private final Resource taskResource;
    private final long submitSequence;

    private volatile JobState state;
    private volatile RejectReason rejectReason;
    private volatile List<TaskPlacement> placements;

    public Job(String id, int priority, int taskCount, Resource taskResource, long submitSequence) {
        this.id = id;
        this.priority = priority;
        this.taskCount = taskCount;
        this.taskResource = taskResource;
        this.submitSequence = submitSequence;
        this.state = JobState.PENDING;
        this.placements = List.of();
    }

    public String getId() {
        return id;
    }

    public int getPriority() {
        return priority;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public Resource getTaskResource() {
        return taskResource;
    }

    public long getSubmitSequence() {
        return submitSequence;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public RejectReason getRejectReason() {
        return rejectReason;
    }

    public void setRejectReason(RejectReason rejectReason) {
        this.rejectReason = rejectReason;
    }

    public List<TaskPlacement> getPlacements() {
        return placements;
    }

    public void setPlacements(List<TaskPlacement> placements) {
        this.placements = placements;
    }

    /** 作业全部任务的总需求 = 单任务资源 × 任务数。 */
    public Resource totalResource() {
        return new Resource(taskResource.getCpu() * taskCount,
                taskResource.getMemoryMb() * taskCount);
    }
}
