package com.yuki.reactor.core.retry;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Retry behaviour of the engine (PRD 5.4): exponential backoff with jitter, plus a coarse limit on
 * how many retries may be dispatched per second so that retried work cannot crowd out fresh work.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "reactor.retry")
public class RetryProperties {

    /**
     * Delay before the first retry; multiplied by {@link #multiplier} for every further attempt.
     */
    private Duration baseDelay = Duration.ofSeconds(1);

    /**
     * Growth factor of the backoff.
     */
    private double multiplier = 2.0d;

    /**
     * Upper bound of the computed delay. A larger {@code retryAfter} hint from a downstream
     * dependency wins over this bound.
     */
    private Duration maxDelay = Duration.ofMinutes(5);

    /**
     * Relative jitter applied to the computed delay, between 0 and 1 (0.2 = +/-20%).
     */
    private double jitter = 0.2d;

    /**
     * Maximum number of {@code RETRY_WAIT} tasks a single worker may claim per second.
     * Zero or negative disables the limit.
     */
    private int maxDispatchesPerSecond = 20;
}
