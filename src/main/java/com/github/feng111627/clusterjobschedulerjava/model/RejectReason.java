package com.github.feng111627.clusterjobschedulerjava.model;

/** 作业提交被拒绝的原因，便于区分不同拒绝路径。 */
public enum RejectReason {
    /** 单个任务的资源需求超过任意一个节点的容量，永远无法放下。 */
    TASK_EXCEEDS_LARGEST_NODE,
    /** 全部任务的总资源需求超过整个集群的总容量，即使集群全空也放不下。 */
    TOTAL_EXCEEDS_CLUSTER_CAPACITY
}
