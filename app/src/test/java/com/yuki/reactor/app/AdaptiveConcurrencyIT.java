package com.yuki.reactor.app;

import com.yuki.reactor.adaptive.AimdController;
import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.signals.RuntimeSignals;
import com.yuki.reactor.faultlab.DownstreamFailureKind;
import com.yuki.reactor.faultlab.MockDownstream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end evidence that the engine reacts to a real downstream: overload observed through real
 * task failures lowers the concurrency limit, recovery observed through real successes raises it
 * again, and every real adjustment is written to {@code scheduler_decision} with its reason code
 * (PRD 5.5, 5.6).
 */
@SpringBootTest(properties = {
        "reactor.adaptive.enabled=true",
        "reactor.adaptive.min-concurrency=1",
        "reactor.adaptive.max-concurrency=4",
        "reactor.adaptive.initial-concurrency=4",
        "reactor.adaptive.sample-interval=200ms",
        "reactor.adaptive.latency-threshold=400ms",
        "reactor.adaptive.error-rate-threshold=0.3",
        "reactor.adaptive.cooldown=300ms",
        "reactor.adaptive.consecutive-breaches-before-decrease=2",
        "reactor.adaptive.recovery-observation=300ms",
        "reactor.adaptive.conservative-concurrency=1"
})
class AdaptiveConcurrencyIT extends AbstractIntegrationTest {

    @Autowired
    private ConcurrencyLimit concurrencyLimit;

    @Autowired
    private AimdController controller;

    @Autowired
    private MockDownstream downstream;

    @Autowired
    private RuntimeSignals signals;

    private com.yuki.reactor.worker.TaskWorkerEngine worker;

    /**
     * The controller is a lifecycle bean, so it starts sampling as soon as the context is up. The
     * manual tests below drive it explicitly to keep the sequence of adjustments deterministic.
     */
    @BeforeEach
    void stopBackgroundSampling() {
        controller.stop();
    }

    @Test
    void lowersTheLimitUnderOverloadAndRaisesItAfterRecovery() {
        overloadTheDownstream();
        List<Long> overloadedTasks = submitDownstreamTasks(8, 1);
        awaitAllTerminal(overloadedTasks);

        controller.sampleOnce();
        controller.sampleOnce();

        assertThat(concurrencyLimit.current()).isEqualTo(2);
        assertThat(decisionCount(ReasonCodes.DOWNSTREAM_OVERLOAD)).isEqualTo(1);
        assertThat(lastDecision(ReasonCodes.DOWNSTREAM_OVERLOAD))
                .containsEntry("concurrency_before", 4)
                .containsEntry("concurrency_after", 2)
                .containsEntry("reason_code", ReasonCodes.DOWNSTREAM_OVERLOAD)
                .containsEntry("decision_type", DecisionType.CONCURRENCY_ADJUSTMENT.name());

        healTheDownstream();
        List<Long> healthyTasks = submitDownstreamTasks(25, 1);
        awaitAllTerminal(healthyTasks);

        controller.sampleOnce();
        assertThat(concurrencyLimit.current()).isEqualTo(2);

        sleep(400);
        controller.sampleOnce();

        assertThat(concurrencyLimit.current()).isEqualTo(3);
        assertThat(decisionCount(ReasonCodes.RECOVERY_OBSERVED)).isEqualTo(1);
        assertThat(lastDecision(ReasonCodes.RECOVERY_OBSERVED))
                .containsEntry("concurrency_before", 2)
                .containsEntry("concurrency_after", 3);
    }

    @Test
    void adjustsConcurrencyFromTheScheduledSamplingLoop() {
        overloadTheDownstream();
        List<Long> tasks = submitDownstreamTasks(4, 1);
        awaitAllTerminal(tasks);
        int before = concurrencyLimit.current();

        controller.start();
        awaitUntil(() -> concurrencyLimit.current() < before, Duration.ofSeconds(15));
        awaitUntil(() -> decisionCount(ReasonCodes.DOWNSTREAM_OVERLOAD) > 0, Duration.ofSeconds(15));

        assertThat(controller.isRunning()).isTrue();
        assertThat(concurrencyLimit.current()).isLessThan(before);
    }

    private void overloadTheDownstream() {
        signals.reset();
        downstream.configure(900L, 1.0, DownstreamFailureKind.TRANSIENT, null, null, null);
    }

    private void healTheDownstream() {
        downstream.configure(50L, 0.0, DownstreamFailureKind.TRANSIENT, null, null, null);
    }

    private List<Long> submitDownstreamTasks(int count, int maxAttempts) {
        ensureWorker();
        List<Long> taskIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskIds.add(submit("downstream", "{}", maxAttempts).task().id());
        }
        return taskIds;
    }

    private void ensureWorker() {
        if (worker == null) {
            worker = startWorker(workerProperties(4), retryWith(Duration.ofMillis(50), 0.0), concurrencyLimit);
        }
    }

    private void awaitAllTerminal(List<Long> taskIds) {
        for (Long taskId : taskIds) {
            awaitUntil(() -> {
                TaskStatus status = task(taskId).status();
                return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED;
            }, Duration.ofMinutes(2));
        }
    }

    private long decisionCount(String reasonCode) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM scheduler_decision WHERE reason_code = ?", Long.class, reasonCode);
        return count == null ? 0L : count;
    }

    private java.util.Map<String, Object> lastDecision(String reasonCode) {
        return jdbcTemplate.queryForMap(
                "SELECT decision_type, reason_code, concurrency_before, concurrency_after, worker_id "
                        + "FROM scheduler_decision WHERE reason_code = ? ORDER BY id DESC LIMIT 1", reasonCode);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test was interrupted", e);
        }
    }
}
