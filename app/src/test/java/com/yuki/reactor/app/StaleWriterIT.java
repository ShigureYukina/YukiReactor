package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.error.TaskFailure;
import com.yuki.reactor.core.service.TaskClaim;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ownership guard (PRD 8.1): a result arriving from an attempt that no longer owns the task must be
 * rejected, and it must not overwrite the state written by the current owner.
 *
 * <p>Phase 1 has no lease recovery yet, so the superseded-attempt situation is produced directly:
 * the attempt reports with a fencing token that is not the one currently stored on the task.
 */
class StaleWriterIT extends AbstractIntegrationTest {

    @Test
    void rejectsSuccessFromAnAttemptThatNoLongerOwnsTheTask() {
        long taskId = submit("echo", "{}").task().id();
        LocalDateTime now = LocalDateTime.now(clock);
        TaskClaim claim = taskOperations.claimAndOpen(taskId, "worker-a", now, Duration.ofSeconds(30));
        assertThat(claim).isNotNull();
        long currentToken = claim.task().getFencingToken();

        boolean applied = taskOperations.complete(taskId, claim.execution().getId(), currentToken - 1, now);

        assertThat(applied).isFalse();
        assertThat(taskOperations.get(taskId).getStatus()).as("state must not be overwritten").isEqualTo(TaskStatus.RUNNING);
        TaskExecutionView attempt = taskService.executions(taskId).getFirst();
        assertThat(attempt.result()).isEqualTo(ExecutionResult.ABANDONED);
        assertThat(attempt.errorCode()).isEqualTo(ErrorCodes.STALE_WRITER);
        assertThat(attempt.finishedAt()).isNotNull();
    }

    @Test
    void acceptsTheReportOfTheCurrentOwner() {
        long taskId = submit("echo", "{}").task().id();
        LocalDateTime now = LocalDateTime.now(clock);
        TaskClaim claim = taskOperations.claimAndOpen(taskId, "worker-a", now, Duration.ofSeconds(30));
        assertThat(claim).isNotNull();
        long currentToken = claim.task().getFencingToken();

        assertThat(taskOperations.complete(taskId, claim.execution().getId(), currentToken, now)).isTrue();

        assertThat(taskOperations.get(taskId).getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(taskService.executions(taskId).getFirst().result()).isEqualTo(ExecutionResult.SUCCEEDED);
    }

    @Test
    void rejectsFailureFromAnAttemptThatNoLongerOwnsTheTask() {
        long taskId = submit("echo", "{}").task().id();
        LocalDateTime now = LocalDateTime.now(clock);
        TaskClaim claim = taskOperations.claimAndOpen(taskId, "worker-a", now, Duration.ofSeconds(30));
        assertThat(claim).isNotNull();
        long currentToken = claim.task().getFencingToken();

        boolean applied = taskOperations.fail(taskId, claim.execution().getId(), currentToken + 1,
                TaskFailure.of(ErrorCategory.TRANSIENT, ErrorCodes.EXECUTION_ERROR, "boom"), now);

        assertThat(applied).isFalse();
        assertThat(taskOperations.get(taskId).getStatus()).isEqualTo(TaskStatus.RUNNING);
        assertThat(taskOperations.get(taskId).getLastError()).isNull();

        List<TaskExecutionView> executions = taskService.executions(taskId);
        assertThat(executions).hasSize(1);
        assertThat(executions.getFirst().result()).isEqualTo(ExecutionResult.ABANDONED);
    }

    @Test
    void completedTaskCannotBeReopenedByALateFailure() {
        long taskId = submit("echo", "{}").task().id();
        LocalDateTime now = LocalDateTime.now(clock);
        TaskClaim claim = taskOperations.claimAndOpen(taskId, "worker-a", now, Duration.ofSeconds(30));
        assertThat(claim).isNotNull();
        long token = claim.task().getFencingToken();
        assertThat(taskOperations.complete(taskId, claim.execution().getId(), token, now)).isTrue();

        assertThat(taskOperations.fail(taskId, claim.execution().getId(), token,
                TaskFailure.of(ErrorCategory.TRANSIENT, ErrorCodes.EXECUTION_ERROR, "late failure"), now)).isFalse();

        assertThat(taskOperations.get(taskId).getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(taskOperations.get(taskId).getLastError()).isNull();
        assertThat(taskService.executions(taskId).getFirst().result())
                .as("a finished attempt keeps the outcome that was accepted first")
                .isEqualTo(ExecutionResult.SUCCEEDED);
    }
}
