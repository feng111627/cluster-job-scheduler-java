package com.github.feng111627.clusterjobschedulerjava.scheduler;

import java.util.List;

/**
 * 一次调度周期的结果：本次成功整批落位的作业 id，以及仍在队列中的作业 id。
 */
public record ScheduleResult(List<String> scheduledJobIds, List<String> queuedJobIds) {
}
