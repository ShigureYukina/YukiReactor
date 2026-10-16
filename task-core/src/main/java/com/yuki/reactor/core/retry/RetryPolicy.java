package com.yuki.reactor.core.retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with jitter (PRD 5.4).
 *
 * <p>The delay for the n-th failed attempt is {@code baseDelay * multiplier^(n-1)}, capped at
 * {@code maxDelay}, then jittered by +/-{@code jitter}. A larger minimum delay requested by the
 * failing dependency (for example a {@code Retry-After} instruction) is honoured and is not capped,
 * because the dependency's own instruction is the better source of truth.
 *
 * <p>The randomness comes from an injected {@link RandomGenerator} so that experiments can replace
 * it with a seeded instance.
 */
public class RetryPolicy {

    private final RetryProperties properties;
    private final RandomGenerator random;

    public RetryPolicy(RetryProperties properties, RandomGenerator random) {
        this.properties = properties;
        this.random = random;
    }

    /**
     * @param now          current time (UTC)
     * @param attemptCount number of attempts that have already been used, at least 1
     * @param minimumDelay minimum delay requested by the dependency, may be {@code null}
     * @return the earliest time the task may be claimed again
     */
    public LocalDateTime nextRunAt(LocalDateTime now, int attemptCount, Duration minimumDelay) {
        Duration delay = delayFor(attemptCount);
        if (minimumDelay != null && minimumDelay.compareTo(delay) > 0) {
            delay = minimumDelay;
        }
        return now.plus(delay);
    }

    /**
     * Backoff delay for the given number of used attempts, including jitter and the upper bound.
     */
    public Duration delayFor(int attemptCount) {
        int attempts = Math.max(1, attemptCount);
        long maxMillis = Math.max(1L, properties.getMaxDelay().toMillis());
        double grown = properties.getBaseDelay().toMillis() * Math.pow(properties.getMultiplier(), attempts - 1);
        long capped = Math.min(Math.max(1L, Math.round(grown)), maxMillis);

        double jitter = Math.max(0d, Math.min(1d, properties.getJitter()));
        double factor = 1d + (random.nextDouble() * 2d - 1d) * jitter;
        long millis = Math.min(Math.max(1L, Math.round(capped * factor)), maxMillis);
        return Duration.ofMillis(millis);
    }
}
