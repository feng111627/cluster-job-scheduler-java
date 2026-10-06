package com.github.feng111627.clusterjobschedulerjava.model;

/**
 * 单个任务的调度落点：属于哪个作业的第几个任务，放到哪个节点、占用多少资源。
 */
public record TaskPlacement(String jobId, int taskIndex, String nodeId, Resource resource) {
}
