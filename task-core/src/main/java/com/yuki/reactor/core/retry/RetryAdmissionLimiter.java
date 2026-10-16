package com.yuki.reactor.core.retry;

import java.time.Clock;

/**
 * Coarse admission control for retried work (PRD 5.4): retries are useful, but a burst of retries
 * must not crowd out tasks that have never run. The limiter uses a fixed one-second window driven
 * by the injected {@link Clock}, which keeps it cheap and easy to explain. Tasks that are denied
 * simply stay in {@code RETRY_WAIT} and are considered again on the next poll.
 */
public class RetryAdmissionLimiter {

    private final int maxPerSecond;
    private final Clock clock;

    private long window = Long.MIN_VALUE;
    private int admitted;

    public RetryAdmissionLimiter(RetryProperties properties, Clock clock) {
        this.maxPerSecond = properties.getMaxDispatchesPerSecond();
        this.clock = clock;
    }

    /**
     * @return {@code true} when a retry may be dispatched in the current window
     */
    public synchronized boolean tryAcquire() {
        if (maxPerSecond <= 0) {
            return true;
        }
        long currentWindow = clock.millis() / 1000L;
        if (currentWindow != window) {
            window = currentWindow;
            admitted = 0;
        }
        if (admitted >= maxPerSecond) {
            return false;
        }
        admitted++;
        return true;
    }

    public int maxPerSecond() {
        return maxPerSecond;
    }
}
