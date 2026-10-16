package com.yuki.reactor.faultlab;

import com.yuki.reactor.core.error.TransientTaskException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Simulated downstream service with controllable latency and error rate (PRD 5.7). All configuration
 * can be changed while the application runs, and failures are drawn from a seeded generator so an
 * experiment can record the seed and be reproduced.
 */
@Component
public class MockDownstream {

    private static final Logger log = LoggerFactory.getLogger(MockDownstream.class);

    private final Object monitor = new Object();
    private final Timer successLatency;
    private final Timer failureLatency;

    private long latencyMs;
    private long jitterMs;
    private double errorRate;
    private DownstreamFailureKind failureKind;
    private long retryAfterMs;
    private long seed;
    private Random random;
    private long calls;
    private long failures;

    public MockDownstream(FaultLabProperties properties, MeterRegistry meterRegistry) {
        properties.validate();
        synchronized (monitor) {
            this.latencyMs = properties.getLatencyMs();
            this.jitterMs = properties.getJitterMs();
            this.errorRate = properties.getErrorRate();
            this.failureKind = properties.getFailureKind();
            this.retryAfterMs = properties.getRetryAfterMs();
            this.seed = properties.getSeed();
            this.random = new Random(this.seed);
        }
        this.successLatency = meterRegistry.timer("reactor.faultlab.downstream.latency", "outcome", "success");
        this.failureLatency = meterRegistry.timer("reactor.faultlab.downstream.latency", "outcome", "failure");
    }

    /**
     * Performs one simulated call: waits for the configured latency first and then decides whether
     * the call fails. The decision is taken before waiting so that a "slow" call is slow regardless
     * of its outcome.
     */
    public DownstreamResult call() {
        long plannedLatency;
        boolean failed;
        DownstreamFailureKind kind;
        long announcedRetryAfter;
        synchronized (monitor) {
            plannedLatency = latencyMs + (jitterMs > 0 ? (long) (random.nextDouble() * jitterMs) : 0L);
            failed = random.nextDouble() < errorRate;
            kind = failureKind;
            announcedRetryAfter = retryAfterMs;
            calls++;
            if (failed) {
                failures++;
            }
        }

        long startedNanos = System.nanoTime();
        sleep(plannedLatency);
        long observedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
        (failed ? failureLatency : successLatency).record(observedMillis, TimeUnit.MILLISECONDS);

        String detail = failed
                ? "mock downstream failed as " + kind + " after " + observedMillis + " ms"
                : "mock downstream responded in " + observedMillis + " ms";
        return new DownstreamResult(failed, observedMillis, kind, detail, announcedRetryAfter);
    }

    /**
     * Changes the behaviour of the mock at runtime. {@code null} keeps the current value.
     */
    public DownstreamConfig configure(Long latencyMs, Double errorRate, DownstreamFailureKind failureKind,
                                      Long retryAfterMs, Long seed, Long jitterMs) {
        synchronized (monitor) {
            if (latencyMs != null) {
                if (latencyMs < 0) {
                    throw new IllegalArgumentException("latencyMs must not be negative");
                }
                this.latencyMs = latencyMs;
            }
            if (jitterMs != null) {
                if (jitterMs < 0) {
                    throw new IllegalArgumentException("jitterMs must not be negative");
                }
                this.jitterMs = jitterMs;
            }
            if (errorRate != null) {
                if (errorRate < 0 || errorRate > 1) {
                    throw new IllegalArgumentException("errorRate must be between 0 and 1");
                }
                this.errorRate = errorRate;
            }
            if (failureKind != null) {
                this.failureKind = failureKind;
            }
            if (retryAfterMs != null) {
                if (retryAfterMs < 0) {
                    throw new IllegalArgumentException("retryAfterMs must not be negative");
                }
                this.retryAfterMs = retryAfterMs;
            }
            if (seed != null) {
                this.seed = seed;
                this.random = new Random(seed);
            }
            DownstreamConfig updated = snapshotLocked();
            log.info("mock downstream reconfigured: latencyMs={} jitterMs={} errorRate={} failureKind={} retryAfterMs={} seed={}",
                    updated.latencyMs(), updated.jitterMs(), updated.errorRate(), updated.failureKind(),
                    updated.retryAfterMs(), updated.seed());
            return updated;
        }
    }

    public DownstreamConfig snapshot() {
        synchronized (monitor) {
            return snapshotLocked();
        }
    }

    private DownstreamConfig snapshotLocked() {
        return new DownstreamConfig(latencyMs, jitterMs, errorRate, failureKind, retryAfterMs, seed, calls, failures);
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientTaskException("mock downstream call was interrupted", e);
        }
    }
}
