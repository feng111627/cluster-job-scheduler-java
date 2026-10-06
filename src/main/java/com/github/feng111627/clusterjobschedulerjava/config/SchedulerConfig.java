package com.github.feng111627.clusterjobschedulerjava.config;

import com.github.feng111627.clusterjobschedulerjava.model.Resource;
import com.github.feng111627.clusterjobschedulerjava.scheduler.ClusterScheduler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 构造内存调度器并注册一组演示节点（单机模拟一个 3 节点小集群）：
 * node-a/node-b/node-c 各 8 CPU、16384 MB 内存，集群总量 24 CPU / 49152 MB。
 */
@Configuration
public class SchedulerConfig {

    @Bean
    public ClusterScheduler clusterScheduler() {
        ClusterScheduler scheduler = new ClusterScheduler();
        Resource nodeCapacity = new Resource(8, 16384);
        scheduler.registerNode("node-a", nodeCapacity);
        scheduler.registerNode("node-b", nodeCapacity);
        scheduler.registerNode("node-c", nodeCapacity);
        return scheduler;
    }
}
