package com.github.feng111627.clusterjobschedulerjava.scheduler;

/** 提交作业的结果：accepted=false 时 rejectReason 必填。 */
public record SubmissionResult(boolean accepted, String jobId, RejectReason rejectReason, String message) {

    public static SubmissionResult accepted(String jobId) {
        return new SubmissionResult(true, jobId, null, "accepted");
    }

    public static SubmissionResult rejected(RejectReason reason, String message) {
        return new SubmissionResult(false, null, reason, message);
    }
}
