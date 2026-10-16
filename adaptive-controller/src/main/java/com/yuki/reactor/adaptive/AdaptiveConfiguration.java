package com.yuki.reactor.adaptive;

import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.signals.RuntimeSignals;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Wires the adaptive controller only when {@code reactor.adaptive.enabled} is set. Without it the
 * worker falls back to its configured fixed concurrency, so the first two phases keep their
 * semantics.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AdaptiveProperties.class)
public class AdaptiveConfiguration {

    @Bean
    @ConditionalOnProperty(name = "reactor.adaptive.enabled", havingValue = "true")
    public ConcurrencyLimit concurrencyLimit(AdaptiveProperties properties) {
        properties.validate();
        return new ConcurrencyLimit(properties.getMinConcurrency(), properties.getMaxConcurrency(),
                properties.getInitialConcurrency());
    }

    @Bean
    @ConditionalOnProperty(name = "reactor.adaptive.enabled", havingValue = "true")
    public AimdController aimdController(AdaptiveProperties properties, ConcurrencyLimit concurrencyLimit,
                                         RuntimeSignals runtimeSignals, TaskMapper taskMapper,
                                         ConcurrencyDecisionRecorder recorder, MeterRegistry meterRegistry,
                                         Clock clock) {
        return new AimdController(properties, concurrencyLimit, runtimeSignals, taskMapper, recorder, meterRegistry,
                clock);
    }
}
