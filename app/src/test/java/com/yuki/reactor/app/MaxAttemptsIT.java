package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.error.ErrorCategory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Attempt budget (PRD 5.4): retrying is bounded, the last attempt is reported with the reason code
 * that explains why the system stopped, and nothing is dispatched afterwards.
 */
class MaxAttemptsIT extends AbstractIntegrationTest {

    @Test
    void stopsRetryingAfterTheConfiguredAttemptBudget() throws InterruptedException {
        startWorker(2);
        // The echo handler throws an unclassified exception, which is treated as retryable.
        long taskId = submit("echo", "{\"fail\": true}", 2).task().id();

        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(30));

        List<TaskExecutionView> executions = executions(taskId);
        assertThat(executions).hasSize(2);
        assertThat(executions).extracting(TaskExecutionView::attempt).containsExactly(1, 2);
        assertThat(executions).extracting(TaskExecutionView::result)
                .containsExactly(ExecutionResult.FAILED, ExecutionResult.FAILED);
        assertThat(executions).extracting(TaskExecutionView::errorCode)
                .as("the budget is spent on the second attempt")
                .containsExactly(ErrorCodes.EXECUTION_ERROR, ErrorCodes.MAX_RETRIES_EXCEEDED);
        assertThat(executions).extracting(TaskExecutionView::errorCategory)
                .containsExactly(ErrorCategory.UNKNOWN, ErrorCategory.UNKNOWN);

        assertThat(task(taskId).attemptCount()).isEqualTo(2);
        assertThat(task(taskId).lastError()).contains("max attempts reached: 2/2");

        Thread.sleep(1500L);
        assertThat(countExecutions(taskId)).isEqualTo(2);
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED);
    }
}
