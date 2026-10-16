package com.yuki.reactor.faultlab;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Initial configuration of the mock downstream service. Every value can be changed at runtime
 * through {@code PUT /api/v1/fault-lab/downstream} so an experiment can inject and remove faults
 * without restarting the application.
 */
@Getter
@Setter
@ConfigurationProperties("reactor.fault-lab")
public class FaultLabProperties {

    /** Base response delay of the simulated downstream, in milliseconds. */
    private long latencyMs = 0;

    /** Extra uniform random delay added on top of {@link #latencyMs}, in milliseconds. */
    private long jitterMs = 0;

    /** Probability in [0, 1] that a call fails. */
    private double errorRate = 0.0;

    /** Which kind of failure is reported when a call fails. */
    private DownstreamFailureKind failureKind = DownstreamFailureKind.TRANSIENT;

    /** Wait time the mock announces together with a {@link DownstreamFailureKind#THROTTLED} failure. */
    private long retryAfterMs = 2000;

    /**
     * Seed of the random generator that decides failures. Experiments record it so that a run can be
     * reproduced instead of being a one-off observation (PRD 5.7).
     */
    private long seed = 0;

    public void validate() {
        if (latencyMs < 0) {
            throw new IllegalArgumentException("latency-ms must not be negative");
        }
        if (jitterMs < 0) {
            throw new IllegalArgumentException("jitter-ms must not be negative");
        }
        if (errorRate < 0 || errorRate > 1) {
            throw new IllegalArgumentException("error-rate must be between 0 and 1");
        }
        if (retryAfterMs < 0) {
            throw new IllegalArgumentException("retry-after-ms must not be negative");
        }
        if (failureKind == null) {
            throw new IllegalArgumentException("failure-kind must be set");
        }
    }
}
