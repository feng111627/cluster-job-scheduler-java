package com.github.feng111627.clusterjobschedulerjava.scheduler;

import com.github.feng111627.clusterjobschedulerjava.model.Job;
import com.github.feng111627.clusterjobschedulerjava.model.RejectReason;

/**
 * 提交结果。accepted 为 true 时 job 处于 PENDING；
 * 为 false 时 rejectReason 给出区分明确的拒绝原因。
 */
public record SubmitResult(boolean accepted, Job job, RejectReason rejectReason, String message) {

    public static SubmitResult accepted(Job job) {
        return new SubmitResult(true, job, null, "作业已进入等待队列");
    }

    public static SubmitResult rejected(RejectReason reason, String message) {
        return new SubmitResult(false, null, reason, message);
    }
}
