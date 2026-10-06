package com.github.feng111627.clusterjobschedulerjava.web;

import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import com.github.feng111627.clusterjobschedulerjava.scheduler.ClusterScheduler;
import com.github.feng111627.clusterjobschedulerjava.scheduler.ClusterSnapshot;
import com.github.feng111627.clusterjobschedulerjava.scheduler.ScheduleResult;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SubmitResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 调度器的 HTTP 演示接口。提交与调度分离：POST 作业只负责排队，
 * 需要显式调用 {@code /schedule} 才会尝试整批调度。
 */
@RestController
@RequestMapping("/api/scheduler")
public class SchedulerController {

    private final ClusterScheduler scheduler;

    public SchedulerController(ClusterScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /** 提交请求体。priority 数值越大越优先。 */
    public record SubmitRequest(int priority, int taskCount, int cpuPerTask, int memoryMbPerTask) {
    }

    /** 提交作业：资源需求超过物理容量时返回 accepted=false 及具体拒绝原因。 */
    @PostMapping("/jobs/{jobId}")
    public SubmitResult submit(@PathVariable String jobId, @RequestBody SubmitRequest req) {
        return scheduler.submitJob(jobId, req.priority(), req.taskCount(),
                new Resource(req.cpuPerTask(), req.memoryMbPerTask()));
    }

    /** 执行一轮调度，返回本轮落位作业与仍排队作业。 */
    @PostMapping("/schedule")
    public ScheduleResult schedule() {
        return scheduler.schedulePendingJobs();
    }

    /** 正常完成并释放资源。 */
    @PostMapping("/jobs/{jobId}/complete")
    public Map<String, Object> complete(@PathVariable String jobId) {
        boolean changed = scheduler.completeJob(jobId);
        return Map.of("jobId", jobId, "released", changed);
    }

    /** 任务失败并释放资源。 */
    @PostMapping("/jobs/{jobId}/fail")
    public Map<String, Object> fail(@PathVariable String jobId) {
        boolean changed = scheduler.failJob(jobId, "manual-failure");
        return Map.of("jobId", jobId, "released", changed);
    }

    /** 节点掉线：其上作业整体失败并释放资源。 */
    @PostMapping("/nodes/{nodeId}/offline")
    public Map<String, Object> nodeOffline(@PathVariable String nodeId) {
        int failedJobs = scheduler.markNodeOffline(nodeId);
        return Map.of("nodeId", nodeId, "failedJobs", failedJobs);
    }

    /** 节点恢复上线。 */
    @PostMapping("/nodes/{nodeId}/online")
    public Map<String, Object> nodeOnline(@PathVariable String nodeId) {
        boolean changed = scheduler.markNodeOnline(nodeId);
        return Map.of("nodeId", nodeId, "online", changed);
    }

    /** 查看队列、运行中作业以及各节点的资源账目。 */
    @GetMapping("/snapshot")
    public ClusterSnapshot snapshot() {
        return scheduler.snapshot();
    }
}
