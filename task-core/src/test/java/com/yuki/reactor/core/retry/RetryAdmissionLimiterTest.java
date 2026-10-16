package com.yuki.reactor.core.retry;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retry admission limit of PRD 5.4: retries are spread over time so that a wave of failing tasks
 * cannot take over every execution slot. The window is driven by the injected clock.
 */
class RetryAdmissionLimiterTest {

    private final RetryProperties properties = new RetryProperties();
    private final TestClock clock = new TestClock(Instant.parse("2026-01-01T00:00:00Z"));

    @Test
    void admitsAtMostTheConfiguredNumberOfRetriesPerWindow() {
        properties.setMaxDispatchesPerSecond(2);
        RetryAdmissionLimiter limiter = new RetryAdmissionLimiter(properties, clock);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.tryAcquire()).isFalse();

        clock.advance(Duration.ofSeconds(1));
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void doesNotAdmitMoreThanTheWindowAllowsWithinTheSameSecond() {
        properties.setMaxDispatchesPerSecond(5);
        RetryAdmissionLimiter limiter = new RetryAdmissionLimiter(properties, clock);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(limiter.tryAcquire()).isFalse();
        }

        clock.advance(Duration.ofMillis(200));
        assertThat(limiter.tryAcquire()).isFalse();

        clock.advance(Duration.ofMillis(800));
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    void isUnlimitedWhenTheLimitIsDisabled() {
        properties.setMaxDispatchesPerSecond(0);
        RetryAdmissionLimiter limiter = new RetryAdmissionLimiter(properties, clock);

        for (int i = 0; i < 1000; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.maxPerSecond()).isZero();
    }

    @Test
    void exposesTheConfiguredLimit() {
        properties.setMaxDispatchesPerSecond(7);
        RetryAdmissionLimiter limiter = new RetryAdmissionLimiter(properties, clock);

        assertThat(limiter.maxPerSecond()).isEqualTo(7);
    }

    private static final class TestClock extends Clock {

        private Instant instant;

        private TestClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
