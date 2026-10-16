package com.yuki.reactor.faultlab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Control surface of the simulated downstream (PRD 5.7). An experiment drives faults from here
 * instead of restarting the application with different settings.
 *
 * <p>Changing the behaviour of a shared downstream is an operational write, so the request must
 * carry the operator and the change is logged.
 */
@RestController
@RequestMapping("/api/v1/fault-lab/downstream")
public class FaultLabController {

    private static final Logger log = LoggerFactory.getLogger(FaultLabController.class);

    private final MockDownstream downstream;

    public FaultLabController(MockDownstream downstream) {
        this.downstream = downstream;
    }

    @GetMapping
    public DownstreamConfig current() {
        return downstream.snapshot();
    }

    @PutMapping
    public DownstreamConfig configure(@RequestBody DownstreamRequest request) {
        if (request == null || request.operator() == null || request.operator().isBlank()) {
            throw new IllegalArgumentException("operator is required to change the simulated downstream");
        }
        DownstreamConfig updated = downstream.configure(request.latencyMs(), request.errorRate(), request.failureKind(),
                request.retryAfterMs(), request.seed(), request.jitterMs());
        log.info("fault injection by {}: latencyMs={} jitterMs={} errorRate={} failureKind={} retryAfterMs={} seed={}",
                request.operator(), updated.latencyMs(), updated.jitterMs(), updated.errorRate(),
                updated.failureKind(), updated.retryAfterMs(), updated.seed());
        return updated;
    }

    @PostMapping("/call")
    public DownstreamResult call() {
        return downstream.call();
    }

    /**
     * {@code null} fields keep the current value.
     */
    public record DownstreamRequest(String operator, Long latencyMs, Double errorRate,
                                    DownstreamFailureKind failureKind, Long retryAfterMs, Long seed, Long jitterMs) {
    }
}
