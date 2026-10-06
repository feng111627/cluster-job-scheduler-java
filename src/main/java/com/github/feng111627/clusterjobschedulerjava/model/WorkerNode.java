package com.github.feng111627.clusterjobschedulerjava.model;

/**
 * 集群中的一个计算节点。capacity 为固定总资源，
 * used 为已分配资源，available = capacity - used。
 *
 * <p>本类自身不加锁；所有资源变动都在 {@code ClusterScheduler} 的同一把锁下进行，
 * 因此这里只保留最朴素的账目逻辑，便于解释和审计。
 */
public final class WorkerNode {
    private final String id;
    private final Resource capacity;
    private Resource used;
    private NodeState state;

    public WorkerNode(String id, Resource capacity) {
        this.id = id;
        this.capacity = capacity;
        this.used = Resource.ZERO;
        this.state = NodeState.ONLINE;
    }

    public String getId() {
        return id;
    }

    public Resource getCapacity() {
        return capacity;
    }

    public Resource getUsed() {
        return used;
    }

    /** 剩余资源 = 总量 - 已用，恒不为负。 */
    public Resource getAvailable() {
        return capacity.subtract(used);
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public boolean isOnline() {
        return state == NodeState.ONLINE;
    }

    /**
     * 占用资源。只有在余量足够且节点在线时才允许，
     * 从根上保证节点任何一个维度的剩余资源不会变成负数。
     */
    public void allocate(Resource req) {
        if (!isOnline()) {
            throw new IllegalStateException("节点 " + id + " 不在线，无法分配资源");
        }
        if (!getAvailable().canFit(req)) {
            throw new IllegalStateException(
                    "节点 " + id + " 资源不足: 剩余=" + getAvailable() + ", 请求=" + req);
        }
        used = used.add(req);
    }

    /**
     * 归还资源。{@code subtract} 会在账目要变负时抛出异常，
     * 因此重复/超额归还不会让可用资源越算越多。
     */
    public void release(Resource req) {
        used = used.subtract(req);
    }
}
