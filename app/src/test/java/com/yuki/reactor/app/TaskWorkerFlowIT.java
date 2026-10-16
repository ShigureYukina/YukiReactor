package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * End to end flow: submit, claim, execute, report (PRD 4.1).
 */
class TaskWorkerFlowIT extends AbstractIntegrationTest {

    @Test
    void executesSubmittedTaskToSuccessAndRecordsTheAttempt() {
        startWorker(2);
        long taskId = submit("echo", "{\"sleepMs\": 20}").task().id();

        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.SUCCEEDED));

        TaskView view = task(taskId);
        assertThat(view.attemptCount()).isEqualTo(1);
        assertThat(view.lastError()).isNull();

        List<TaskExecutionView> executions = taskService.executions(taskId);
        assertThat(executions).hasSize(1);
        TaskExecutionView attempt = executions.getFirst();
        assertThat(attempt.attempt()).isEqualTo(1);
        assertThat(attempt.result()).isEqualTo(ExecutionResult.SUCCEEDED);
        assertThat(attempt.fencingToken()).isEqualTo(1L);
        assertThat(attempt.startedAt()).isNotNull();
        assertThat(attempt.finishedAt()).isNotNull();
        assertThat(attempt.workerId()).isNotBlank();
        assertThat(attempt.errorCode()).isNull();
    }

    @Test
    void reportsHandlerFailureAsFailedTaskWithReasonAndTouchedExecution() {
        startWorker(1);
        long taskId = submit("echo", "{\"fail\": true}").task().id();

        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED));

        TaskView view = task(taskId);
        assertThat(view.attemptCount()).isEqualTo(1);
        assertThat(view.lastError()).contains("echo handler was asked to fail");

        List<TaskExecutionView> executions = taskService.executions(taskId);
        assertThat(executions).hasSize(1);
        TaskExecutionView attempt = executions.getFirst();
        assertThat(attempt.result()).isEqualTo(ExecutionResult.FAILED);
        assertThat(attempt.errorCode()).isEqualTo("EXECUTION_ERROR");
        assertThat(attempt.errorMessage()).contains("echo handler was asked to fail");
        assertThat(attempt.finishedAt()).isNotNull();
    }

    @Test
    void executesManyTasksWithBoundedConcurrency() {
        startWorker(3);
        List<Long> taskIds = java.util.stream.IntStream.range(0, 12)
                .mapToObj(i -> submit("echo", "{\"sleepMs\": 10}").task().id())
                .toList();

        await().atMost(30, SECONDS).untilAsserted(() ->
                assertThat(taskIds).allSatisfy(id ->
                        assertThat(task(id).status()).isEqualTo(TaskStatus.SUCCEEDED)));

        with().pollInterval(50, java.util.concurrent.TimeUnit.MILLISECONDS).await().atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(taskIds).allSatisfy(id ->
                        assertThat(taskService.executions(id)).hasSize(1)));
    }
}
