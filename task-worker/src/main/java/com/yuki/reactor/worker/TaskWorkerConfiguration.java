package com.yuki.reactor.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.retry.RetryProperties;
import com.yuki.reactor.core.service.TaskOperations;
import com.yuki.reactor.core.signals.RuntimeSignals;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Registers the worker engine unless {@code reactor.worker.enabled=false}.
 *
 * <p>Disabling the worker is how a process runs as API-only, and how tests keep control over who
 * executes tasks.
 *
 * <p>The concurrency limit is looked up instead of required: without an adaptive controller the
 * worker uses a fixed limit from its own configuration.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TaskWorkerProperties.class)
public class TaskWorkerConfiguration {

    @Bean
    @ConditionalOnProperty(name = "reactor.worker.enabled", havingValue = "true", matchIfMissing = true)
    public TaskWorkerEngine taskWorkerEngine(TaskOperations taskOperations,
                                            TaskHandlerRegistry handlerRegistry,
                                            TaskWorkerProperties properties,
                                            RetryProperties retryProperties,
                                            RuntimeSignals runtimeSignals,
                                            ObjectProvider<ConcurrencyLimit> concurrencyLimit,
                                            ObjectMapper objectMapper,
                                            Clock clock) {
        return new TaskWorkerEngine(taskOperations, handlerRegistry, properties, retryProperties,
                runtimeSignals, concurrencyLimit.getIfAvailable(), objectMapper, clock, null);
    }
}
