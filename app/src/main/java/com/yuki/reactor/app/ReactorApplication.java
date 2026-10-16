package com.yuki.reactor.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Reactor: persistent asynchronous task execution engine.
 *
 * <p>This module only wires modules together; behaviour lives in {@code task-core},
 * {@code task-api} and {@code task-worker}. Disable the worker with
 * {@code reactor.worker.enabled=false} to run a process that only serves the API.
 */
@SpringBootApplication(scanBasePackages = "com.yuki.reactor")
@ConfigurationPropertiesScan("com.yuki.reactor")
public class ReactorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactorApplication.class, args);
    }
}
