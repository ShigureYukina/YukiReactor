package com.yuki.reactor.core.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backoff maths of PRD 5.4: exponential growth, a hard upper bound, bounded jitter and respect for
 * a minimum delay requested by the failing dependency.
 */
class RetryPolicyTest {

    private final RetryProperties properties = new RetryProperties();

    @Test
    void growsExponentiallyAndStopsAtMaxDelay() {
        properties.setBaseDelay(Duration.ofSeconds(1));
        properties.setMultiplier(2.0);
        properties.setMaxDelay(Duration.ofSeconds(10));
        properties.setJitter(0);
        RetryPolicy policy = policy();

        assertThat(policy.delayFor(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayFor(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayFor(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayFor(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delayFor(5)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.delayFor(100)).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void treatsMissingAttemptCountAsTheFirstAttempt() {
        properties.setJitter(0);
        RetryPolicy policy = policy();

        assertThat(policy.delayFor(0)).isEqualTo(policy.delayFor(1));
        assertThat(policy.delayFor(-5)).isEqualTo(policy.delayFor(1));
    }

    @Test
    void appliesJitterWithinTheConfiguredBounds() {
        properties.setBaseDelay(Duration.ofSeconds(4));
        properties.setMaxDelay(Duration.ofSeconds(30));
        properties.setJitter(0.5);
        RetryPolicy policy = policy();

        // The first attempt may be jittered by +/-50% around the base delay.
        assertThat(policy.delayFor(1)).isBetween(Duration.ofSeconds(2), Duration.ofSeconds(6));

        // Every delay stays positive and inside the upper bound, however large the attempt number.
        for (int attempt = 1; attempt <= 40; attempt++) {
            Duration delay = policy.delayFor(attempt);
            assertThat(delay).isGreaterThanOrEqualTo(Duration.ofMillis(1));
            assertThat(delay).isLessThanOrEqualTo(properties.getMaxDelay());
        }
    }

    @Test
    void honoursALargerMinimumDelayFromTheDependency() {
        properties.setBaseDelay(Duration.ofSeconds(1));
        properties.setMaxDelay(Duration.ofSeconds(10));
        properties.setJitter(0);
        RetryPolicy policy = policy();
        LocalDateTime now = LocalDateTime.of(2026, 1, 1, 12, 0);

        // A Retry-After instruction wins over the computed backoff, even beyond max-delay.
        assertThat(policy.nextRunAt(now, 1, Duration.ofSeconds(60))).isEqualTo(now.plusSeconds(60));
        assertThat(policy.nextRunAt(now, 3, Duration.ofSeconds(1))).isEqualTo(now.plusSeconds(4));
        assertThat(policy.nextRunAt(now, 3, null)).isEqualTo(now.plusSeconds(4));
    }

    @Test
    void neverSchedulesTheNextAttemptInThePast() {
        properties.setJitter(1.0);
        RetryPolicy policy = policy();
        LocalDateTime now = LocalDateTime.of(2026, 1, 1, 12, 0);

        assertThat(policy.nextRunAt(now, 1, null)).isAfter(now);
    }

    private RetryPolicy policy() {
        return new RetryPolicy(properties, new Random(42));
    }
}
