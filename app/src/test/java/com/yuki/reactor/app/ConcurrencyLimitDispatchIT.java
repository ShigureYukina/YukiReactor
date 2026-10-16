package com.yuki.reactor.app;

import com.yuki.reactor.app.task.ProbeTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.worker.TaskWorkerEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A concurrency limit owned by the outside world (the adaptive controller) must change what the
 * engine dispatches without restarting the worker: the pool is sized by {@code max}, the number of
 * parallel attempts is decided by {@code current} on every poll (PRD 5.5).
 */
@Import(TestHandlersConfiguration.class)
class ConcurrencyLimitDispatchIT extends AbstractIntegrationTest {

    @BeforeEach
    void resetProbe() {
        ProbeTaskHandler.reset();
    }

    @Test
    void appliesAnExternallyChangedConcurrencyLimitWithoutRestart() {
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 1);
        TaskWorkerEngine engine = startWorker(workerProperties(4), retryProperties, limit);

        assertThat(engine.getEffectiveConcurrency()).isEqualTo(1);
        assertThat(limit.max()).isEqualTo(4);

        List<Long> taskIds = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            taskIds.add(submit("probe", "{\"sleepMs\": 300}").task().id());
        }
        for (Long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
            assertThat(countExecutions(taskId)).isEqualTo(1);
        }

        assertThat(ProbeTaskHandler.maxObservedConcurrency()).isEqualTo(1);

        limit.apply(2);
        assertThat(engine.getEffectiveConcurrency()).isEqualTo(2);
        limit.apply(4);
        assertThat(engine.getEffectiveConcurrency()).isEqualTo(4);
        assertThat(engine.getConcurrencyLimit()).isSameAs(limit);
    }
}
