package com.github.feng111627.clusterjobschedulerjava.web;

import com.github.feng111627.clusterjobschedulerjava.scheduler.Resources;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SchedulerService;
import com.github.feng111627.clusterjobschedulerjava.scheduler.SubmissionResult;
import com.github.feng111627.clusterjobschedulerjava.scheduler.Task;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群调度的 HTTP 入口（演示用，核心逻辑全部在 {@link SchedulerService}，
 * 线程安全由其保证）。
 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final SchedulerService scheduler;

    public ClusterController(SchedulerService scheduler) {
        this.scheduler = scheduler;
    }

    public record ResourceDto(int cpu, int memoryMb, int diskMb) {
    }

    public record TaskDto(String taskId, ResourceDto requirement) {
    }

    public record SubmitJobRequest(String jobId, int priority, List<TaskDto> tasks) {
    }

    @PostMapping("/nodes/{nodeId}")
    public Map<String, Object> registerNode(@PathVariable String nodeId,
                                            @RequestBody ResourceDto body) {
        scheduler.registerNode(nodeId, new Resources(body.cpu(), body.memoryMb(), body.diskMb()));
        return Map.of("registered", nodeId);
    }

    @PostMapping("/jobs")
    public SubmissionResult submitJob(@RequestBody SubmitJobRequest request) {
        List<Task> tasks = new ArrayList<>();
        if (request.tasks() != null) {
            for (TaskDto dto : request.tasks()) {
                ResourceDto r = dto.requirement();
                tasks.add(new Task(dto.taskId(), new Resources(r.cpu(), r.memoryMb(), r.diskMb())));
            }
        }
        return scheduler.submitJob(request.jobId(), tasks, request.priority());
    }

    @PostMapping("/jobs/{jobId}/release")
    public Map<String, Object> releaseJob(@PathVariable String jobId) {
        scheduler.releaseJob(jobId);
        return state(jobId);
    }

    @PostMapping("/tasks/{taskId}/fail")
    public Map<String, Object> taskFailed(@PathVariable String taskId) {
        scheduler.taskFailed(taskId);
        return Map.of("failedTask", taskId);
    }

    @PostMapping("/nodes/{nodeId}/offline")
    public Map<String, Object> nodeOffline(@PathVariable String nodeId) {
        scheduler.nodeOffline(nodeId);
        return Map.of("offline", nodeId, "nodes", scheduler.nodeSnapshot());
    }

    @PostMapping("/nodes/{nodeId}/online")
    public Map<String, Object> nodeOnline(@PathVariable String nodeId) {
        scheduler.nodeOnline(nodeId);
        return Map.of("online", nodeId, "nodes", scheduler.nodeSnapshot());
    }

    @PostMapping("/schedule")
    public Map<String, Object> schedule() {
        int n = scheduler.runScheduling();
        return Map.of("scheduledJobs", n, "queue", scheduler.queuedJobIds());
    }

    @GetMapping("/jobs/{jobId}")
    public Map<String, Object> state(@PathVariable String jobId) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("jobId", jobId);
        view.put("state", scheduler.getJobState(jobId));
        view.put("assignments", scheduler.getTaskAssignments(jobId));
        return view;
    }

    @GetMapping("/snapshot")
    public Map<String, Object> snapshot() {
        return Map.of(
                "queue", scheduler.queuedJobIds(),
                "nodes", scheduler.nodeSnapshot());
    }
}
