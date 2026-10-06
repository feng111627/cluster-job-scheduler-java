package com.github.feng111627.clusterjobschedulerjava.web;

import com.github.feng111627.clusterjobschedulerjava.scheduler.SchedulerService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 将内存调度器注册为 Spring Bean。 */
@Configuration
public class SchedulerConfig {

    @Bean
    public SchedulerService schedulerService() {
        return new SchedulerService();
    }
}
