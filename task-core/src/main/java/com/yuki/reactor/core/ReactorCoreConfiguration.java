package com.yuki.reactor.core;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Wiring shared by every module that uses the task domain.
 */
@Configuration
@MapperScan("com.yuki.reactor.core.mapper")
public class ReactorCoreConfiguration {

    /**
     * All persisted timestamps come from this clock and are UTC (AGENTS.md 3). Injecting it keeps
     * tests and production code on the same notion of "now" instead of scattered system calls.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
