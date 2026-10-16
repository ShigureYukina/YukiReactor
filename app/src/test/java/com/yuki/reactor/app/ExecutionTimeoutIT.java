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
 * Execution timeout (PRD 5.4). The worker interrupts the executing thread when the task exceeds its
 * configured timeout and classifies the attempt as a timeout failure.
 */
class ExecutionTimeoutIT extends AbstractIntegrationTest {

    @Test
    void reportsTimeoutAndSpendsTheAttemptBudget() {
        startWorker(2);
        long taskId = submit("echo", "{\"sleepMs\": 5000}", 2, 500).task().id();

        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(30));

        List<TaskExecutionView> executions = executions(taskId);
        assertThat(executions).hasSize(2);
        assertThat(executions).extracting(TaskExecutionView::result)
                .containsExactly(ExecutionResult.FAILED, ExecutionResult.FAILED);
        assertThat(executions).extracting(TaskExecutionView::errorCategory)
                .containsExactly(ErrorCategory.TIMEOUT, ErrorCategory.TIMEOUT);
        assertThat(executions.get(0).errorCode()).isEqualTo(ErrorCodes.EXECUTION_TIMEOUT);
        assertThat(executions.get(0).errorMessage()).contains("exceeded the configured timeout");
        assertThat(executions.get(1).errorCode()).isEqualTo(ErrorCodes.MAX_RETRIES_EXCEEDED);
        assertThat(executions).allSatisfy(execution -> assertThat(execution.finishedAt()).isNotNull());
        assertThat(task(taskId).lastError())
                .as("the task names the spent budget; the timeout itself stays in the message and the attempt rows")
                .contains(ErrorCodes.MAX_RETRIES_EXCEEDED)
                .contains("exceeded the configured timeout");
    }

    @Test
    void letsTasksThatFinishInTimeSucceed() {
        startWorker(2);
        long taskId = submit("echo", "{\"sleepMs\": 200}", 2, 5000).task().id();

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(20));

        List<TaskExecutionView> executions = executions(taskId);
        assertThat(executions).hasSize(1);
        assertThat(executions.get(0).result()).isEqualTo(ExecutionResult.SUCCEEDED);
        assertThat(executions.get(0).errorCategory()).isNull();
        assertThat(executions.get(0).errorCode()).isNull();
    }
}
