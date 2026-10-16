package com.yuki.reactor.core;

import com.yuki.reactor.core.retry.RetryPolicy;
import com.yuki.reactor.core.retry.RetryProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Random;
import java.util.random.RandomGenerator;

/**
 * Wiring shared by every module that uses the task domain.
 */
@Configuration
@MapperScan("com.yuki.reactor.core.mapper")
@EnableConfigurationProperties(RetryProperties.class)
public class ReactorCoreConfiguration {

    /**
     * All persisted timestamps come from this clock and are UTC (AGENTS.md 3). Injecting it keeps
     * tests and production code on the same notion of "now" instead of scattered system calls.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Source of the retry jitter. It is a bean so that an experiment can replace it with a seeded
     * generator and reproduce a run exactly (PRD 5.7, 10.3).
     */
    @Bean
    public RandomGenerator randomGenerator() {
        return new Random();
    }

    /**
     * Exponential backoff with jitter, shared by every worker of this process (PRD 5.4).
     */
    @Bean
    public RetryPolicy retryPolicy(RetryProperties properties, RandomGenerator randomGenerator) {
        return new RetryPolicy(properties, randomGenerator);
    }
}
