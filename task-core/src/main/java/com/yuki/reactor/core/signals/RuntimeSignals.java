package com.yuki.reactor.core.signals;

import com.yuki.reactor.core.error.ErrorCategory;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Rolling window of what this process observed recently: the outcome of task attempts and the latency of
 * calls to a downstream service (PRD 5.5).
 *
 * <p>Only the last {@code capacity} samples are kept, so the window describes current behaviour rather than
 * the whole history of the process. Recording is cheap (one lock, no I/O) because it happens on every
 * attempt; reading happens once per controller interval.
 *
 * <p>This is deliberately per process: every worker instance decides for itself how much work it may take,
 * and a decision record names the worker it belongs to.
 */
@Component
public class RuntimeSignals {

    private static final int DEFAULT_CAPACITY = 200;

    private final int capacity;
    private final Deque<AttemptSample> attempts = new ArrayDeque<>();
    private final Deque<DownstreamSample> downstreamCalls = new ArrayDeque<>();

    public RuntimeSignals() {
        this(DEFAULT_CAPACITY);
    }

    public RuntimeSignals(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("signal window capacity must be at least 1");
        }
        this.capacity = capacity;
    }

    /**
     * Records one finished attempt.
     *
     * @param latencyMillis how long the attempt took
     * @param queueWaitMillis how long the task waited before it was claimed
     * @param category the failure category, or {@code null} when the attempt succeeded
     * @param timedOut whether the attempt hit the execution timeout
     */
    public synchronized void recordAttempt(long latencyMillis, long queueWaitMillis, ErrorCategory category, boolean timedOut) {
        push(attempts, new AttemptSample(Math.max(0L, latencyMillis), Math.max(0L, queueWaitMillis), category, timedOut));
    }

    /** Records one call to a downstream service; {@code failed} means the call returned an error. */
    public synchronized void recordDownstreamCall(long latencyMillis, boolean failed) {
        push(downstreamCalls, new DownstreamSample(Math.max(0L, latencyMillis), failed));
    }

    public synchronized SignalSnapshot snapshot() {
        if (attempts.isEmpty() && downstreamCalls.isEmpty()) {
            return SignalSnapshot.empty();
        }
        long attemptLatency = 0L;
        long queueWait = 0L;
        int downstreamishFailures = 0;
        int timeouts = 0;
        for (AttemptSample sample : attempts) {
            attemptLatency += sample.latencyMillis();
            queueWait += sample.queueWaitMillis();
            if (isDownstreamError(sample.category())) {
                downstreamishFailures++;
            }
            if (sample.timedOut()) {
                timeouts++;
            }
        }
        long downstreamLatency = 0L;
        int downstreamFailures = 0;
        for (DownstreamSample sample : downstreamCalls) {
            downstreamLatency += sample.latencyMillis();
            if (sample.failed()) {
                downstreamFailures++;
            }
        }
        int attemptCount = attempts.size();
        int downstreamCount = downstreamCalls.size();
        return new SignalSnapshot(
                attemptCount,
                downstreamCount,
                attemptCount == 0 ? 0.0 : (double) attemptLatency / attemptCount,
                attemptCount == 0 ? 0.0 : (double) downstreamishFailures / attemptCount,
                attemptCount == 0 ? 0.0 : (double) timeouts / attemptCount,
                attemptCount == 0 ? 0.0 : (double) queueWait / attemptCount,
                downstreamCount == 0 ? 0.0 : (double) downstreamLatency / downstreamCount,
                downstreamCount == 0 ? 0.0 : (double) downstreamFailures / downstreamCount);
    }

    public synchronized void reset() {
        attempts.clear();
        downstreamCalls.clear();
    }

    public int capacity() {
        return capacity;
    }

    /**
     * Failures that describe load on something outside this process. A business rejection or an invalid
     * parameter is a deliberate answer from the work itself, so it must not be read as downstream load.
     */
    private static boolean isDownstreamError(ErrorCategory category) {
        return category == ErrorCategory.TRANSIENT
                || category == ErrorCategory.THROTTLED
                || category == ErrorCategory.OVERLOADED
                || category == ErrorCategory.TIMEOUT;
    }

    private <T> void push(Deque<T> window, T sample) {
        window.addLast(sample);
        while (window.size() > capacity) {
            window.removeFirst();
        }
    }

    private record AttemptSample(long latencyMillis, long queueWaitMillis, ErrorCategory category, boolean timedOut) {
    }

    private record DownstreamSample(long latencyMillis, boolean failed) {
    }
}
