package com.yuki.reactor.core.signals;

/**
 * Aggregated execution signals observed by one process (PRD 5.5).
 *
 * <p>Latency and error rate are kept apart for attempts and for calls to a downstream service: a task may
 * be slow for reasons that have nothing to do with a downstream, and only the downstream numbers describe
 * the load that a controller should react to.
 */
public record SignalSnapshot(
        int attempts,
        int downstreamCalls,
        double averageAttemptLatencyMillis,
        double attemptErrorRate,
        double timeoutRate,
        double averageQueueWaitMillis,
        double averageDownstreamLatencyMillis,
        double downstreamErrorRate) {

    public static SignalSnapshot empty() {
        return new SignalSnapshot(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }

    /**
     * Whether anything was observed at all. A controller must treat an empty snapshot as "unknown" and
     * fall back to a conservative concurrency instead of assuming that the system is healthy (PRD 5.5).
     */
    public boolean measured() {
        return attempts > 0 || downstreamCalls > 0;
    }
}
