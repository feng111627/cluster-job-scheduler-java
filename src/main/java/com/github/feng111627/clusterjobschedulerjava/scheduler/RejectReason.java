package com.github.feng111627.clusterjobschedulerjava.scheduler;

/** 作业提交被拒绝的可区分原因。 */
public enum RejectReason {
    /** 参数非法（空任务、资源为负等）。 */
    INVALID_REQUEST,
    /** 单个任务的资源需求超过集群中任何单台在线节点的容量。 */
    TASK_EXCEEDS_LARGEST_NODE,
    /** 作业所有任务的资源需求总和超过集群总容量。 */
    JOB_EXCEEDS_CLUSTER_CAPACITY
}
