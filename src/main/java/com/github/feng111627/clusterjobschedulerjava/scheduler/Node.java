package com.github.feng111627.clusterjobschedulerjava.scheduler;

/**
 * 集群节点：记录总容量 capacity、已分配量 allocated 与在线状态。
 *
 * <p>资源账目不变量（所有变更都在 {@link SchedulerService} 的锁内进行）：
 * <ul>
 *   <li>allocated 每一维都在 [0, capacity] 区间内，剩余资源永远不为负；</li>
 *   <li>分配失败时 allocated 保持原样，不会留下部分占用。</li>
 * </ul>
 */
public final class Node {

    private final String id;
    private final Resources capacity;
    private Resources allocated = Resources.ZERO;
    private boolean online = true;

    public Node(String id, Resources capacity) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        this.id = id;
        this.capacity = capacity;
    }

    public String getId() {
        return id;
    }

    public Resources getCapacity() {
        return capacity;
    }

    public Resources getAllocated() {
        return allocated;
    }

    /** 剩余容量；节点掉线时视为不可用（返回 ZERO 也不会被调度器选中）。 */
    public Resources getAvailable() {
        if (!online) {
            return Resources.ZERO;
        }
        return capacity.minus(allocated);
    }

    public boolean isOnline() {
        return online;
    }

    void setOnline(boolean online) {
        this.online = online;
    }

    /**
     * 尝试占用资源：仅当节点在线且剩余容量每一维都够时才成功。
     * 成功才改账，失败完全不动 allocated（全有或全无）。
     */
    boolean tryAllocate(Resources requirement) {
        if (!online || !getAvailable().canFit(requirement)) {
            return false;
        }
        allocated = allocated.plus(requirement);
        return true;
    }

    /**
     * 归还资源。{@code minus} 在资源不足时会抛异常，
     * 因此重复归还（可能把账目算成负）会被立刻拦截而不是静默放大可用资源。
     */
    void release(Resources requirement) {
        allocated = allocated.minus(requirement);
    }

    /** 节点掉线时清空它登记的占用账目（释放动作由调度器逐任务完成后调用）。 */
    void resetAllocation() {
        allocated = Resources.ZERO;
    }

    @Override
    public String toString() {
        return "Node{" + id + ", online=" + online + ", capacity=" + capacity
                + ", allocated=" + allocated + ", available=" + getAvailable() + "}";
    }
}
