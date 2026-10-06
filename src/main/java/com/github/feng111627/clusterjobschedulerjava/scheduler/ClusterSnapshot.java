package com.github.feng111627.clusterjobschedulerjava.scheduler;

import java.util.List;

/**
 * 集群与队列的只读快照，供查询/演示接口使用。
 */
public record ClusterSnapshot(List<String> queuedJobIds,
                              List<String> runningJobIds,
                              List<NodeView> nodes) {

    /** 单个节点的账目视图。 */
    public record NodeView(String nodeId,
                           String state,
                           int cpuCapacity,
                           int cpuUsed,
                           int cpuAvailable,
                           int memoryCapacityMb,
                           int memoryUsedMb,
                           int memoryAvailableMb) {
    }
}
