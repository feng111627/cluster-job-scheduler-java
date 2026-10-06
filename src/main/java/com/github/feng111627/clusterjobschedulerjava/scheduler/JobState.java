package com.github.feng111627.clusterjobschedulerjava.scheduler;

/** 作业生命周期状态。 */
public enum JobState {
    /** 已接受，排队等待调度。 */
    PENDING,
    /** 全部任务已分配到节点运行。 */
    RUNNING,
    /** 作业完成（主动释放）。 */
    COMPLETED,
    /** 因节点掉线或任务失败而终止。 */
    FAILED,
    /** 提交时被拒绝，未进入队列。 */
    REJECTED
}
