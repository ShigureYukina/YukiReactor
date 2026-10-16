package com.yuki.reactor.app;

import com.yuki.reactor.app.task.ProbeTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.worker.TaskWorkerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per task type concurrency limit (PRD 5.2). The worker pool is wide enough for all tasks, but the
 * configured limit of the task type keeps them from running in parallel.
 */
@Import(TestHandlersConfiguration.class)
class TaskTypeConcurrencyLimitIT extends AbstractIntegrationTest {

    @Test
    void keepsEachTaskTypeWithinItsOwnConcurrencyLimit() {
        ProbeTaskHandler.reset();
        TaskWorkerProperties properties = workerProperties(4);
        properties.setTaskTypeLimits(Map.of("probe", 1));
        startWorker(properties);

        List<Long> taskIds = IntStream.range(0, 4)
                .mapToObj(index -> submit("probe", "{\"sleepMs\": 300}").task().id())
                .toList();

        for (long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
            assertThat(countExecutions(taskId)).isEqualTo(1);
        }
        assertThat(ProbeTaskHandler.maxObservedConcurrency())
                .as("the type limit, not the worker concurrency, governs parallelism")
                .isEqualTo(1);
    }
}
