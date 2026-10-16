package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.worker.TaskWorkerEngine;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two worker instances competing for the same queue (PRD 5.2, PRD 9 "multi instance claiming"): every
 * task is executed exactly once even though several workers poll the same table.
 */
class MultiWorkerIT extends AbstractIntegrationTest {

    @Test
    void competingWorkersNeverExecuteTheSameTaskTwice() {
        TaskWorkerEngine first = startWorker(2);
        TaskWorkerEngine second = startWorker(2);
        assertThat(first.getWorkerId()).isNotEqualTo(second.getWorkerId());

        List<Long> taskIds = IntStream.range(0, 20)
                .mapToObj(index -> submit("echo", "{\"sleepMs\": 50}").task().id())
                .toList();

        Set<String> observingWorkers = new HashSet<>();
        for (long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(60));
            List<TaskExecutionView> executions = executions(taskId);
            assertThat(executions).hasSize(1);
            assertThat(executions.get(0).result()).isEqualTo(ExecutionResult.SUCCEEDED);
            assertThat(task(taskId).attemptCount()).isEqualTo(1);
            observingWorkers.add(executions.get(0).workerId());
        }

        assertThat(observingWorkers).isNotEmpty();
        assertThat(Set.of(first.getWorkerId(), second.getWorkerId())).containsAll(observingWorkers);
    }
}
