package com.yuki.reactor.observability;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

/**
 * Wiring for the observability module.
 *
 * <p>Its mapper lives in this module's own package, so it is scanned here instead of relying on the
 * mapper scan of {@code task-core}.
 */
@Configuration
@MapperScan("com.yuki.reactor.observability.mapper")
public class ObservabilityConfiguration {
}
