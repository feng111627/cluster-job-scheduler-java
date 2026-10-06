package com.github.feng111627.clusterjobschedulerjava.scheduler;

/**
 * 作业中的一个任务，携带资源需求；被调度后记录所在节点。
 *
 * <p>{@code assignedNodeId == null} 表示尚未分配。作业级的全有或全无语义由
 * {@link SchedulerService} 保证：先尝试全部任务，任何一个放不下就整体回滚，
 * 因此不会出现"任务已标了节点但作业仍排队"的中间态。
 */
public final class Task {

    private final String id;
    private final Resources requirement;
    private String assignedNodeId;

    public Task(String id, Resources requirement) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
        if (requirement == null) {
            throw new IllegalArgumentException("task requirement must not be null");
        }
        this.id = id;
        this.requirement = requirement;
    }

    public String getId() {
        return id;
    }

    public Resources getRequirement() {
        return requirement;
    }

    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    public boolean isAssigned() {
        return assignedNodeId != null;
    }

    void assignTo(String nodeId) {
        this.assignedNodeId = nodeId;
    }

    void clearAssignment() {
        this.assignedNodeId = null;
    }

    @Override
    public String toString() {
        return "Task{" + id + ", need=" + requirement
                + (assignedNodeId != null ? ", on=" + assignedNodeId : ", unassigned") + "}";
    }
}
