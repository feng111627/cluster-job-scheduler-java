package com.github.feng111627.clusterjobschedulerjava.model;

/** 节点状态。 */
public enum NodeState {
    /** 在线，可接受调度。 */
    ONLINE,
    /** 掉线，不再参与调度，其上任务视为失败并释放。 */
    OFFLINE
}
