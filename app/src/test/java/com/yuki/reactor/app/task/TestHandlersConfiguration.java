package com.yuki.reactor.app.task;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Registers the test only task types. Imported explicitly by the test classes that need them, so the
 * other integration tests keep the plain production context.
 */
@TestConfiguration
public class TestHandlersConfiguration {

    @Bean
    public ScenarioTaskHandler scenarioTaskHandler() {
        return new ScenarioTaskHandler();
    }

    @Bean
    public ProbeTaskHandler probeTaskHandler() {
        return new ProbeTaskHandler();
    }

    @Bean
    public SideEffectTaskHandler sideEffectTaskHandler(JdbcTemplate jdbcTemplate) {
        return new SideEffectTaskHandler(jdbcTemplate);
    }
}
