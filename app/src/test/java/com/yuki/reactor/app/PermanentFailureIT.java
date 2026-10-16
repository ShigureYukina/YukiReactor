package com.yuki.reactor.app;

import com.yuki.reactor.app.task.ScenarioTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.error.ErrorCategory;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Non retryable errors (PRD 5.4): a rejected parameter or an explicit business refusal must fail the
 * task immediately instead of consuming the whole attempt budget.
 */
@Import(TestHandlersConfiguration.class)
class PermanentFailureIT extends AbstractIntegrationTest {

    @Test
    void failsInvalidParametersWithoutRetrying() throws InterruptedException {
        assertFailsOnFirstAttempt("invalid-parameter", ErrorCategory.INVALID_PARAMETER);
    }

    @Test
    void failsBusinessRejectionsWithoutRetrying() throws InterruptedException {
        assertFailsOnFirstAttempt("rejection", ErrorCategory.BUSINESS_REJECTION);
    }

    private void assertFailsOnFirstAttempt(String failureKind, ErrorCategory expected) throws InterruptedException {
        startWorker(2);
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE,
                "{\"failures\": 1, \"failureKind\": \"" + failureKind + "\"}", 3).task().id();

        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(20));

        TaskExecutionView attempt = executions(taskId).getFirst();
        assertThat(attempt.result()).isEqualTo(ExecutionResult.FAILED);
        assertThat(attempt.errorCategory()).isEqualTo(expected);
        assertThat(attempt.attempt()).isEqualTo(1);
        assertThat(task(taskId).attemptCount()).isEqualTo(1);

        // Wait longer than the configured backoff: a permanent failure must not produce a new attempt.
        Thread.sleep(1500L);
        assertThat(countExecutions(taskId)).isEqualTo(1);
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED);
    }
}
