package com.github.feng111627.clusterjobschedulerjava.model;

/** 作业生命周期状态。 */
public enum JobState {
    /** 已提交，在等待队列中。 */
    PENDING,
    /** 全部任务已分配到节点，正在运行。 */
    RUNNING,
    /** 作业被主动完成/释放。 */
    COMPLETED,
    /** 因节点故障等原因中止，其占用资源已全部归还。 */
    FAILED,
    /** 提交时被拒绝（未进入队列）。 */
    REJECTED
}
