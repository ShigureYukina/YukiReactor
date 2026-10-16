package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.statemachine.IllegalTaskTransitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Administrative operations: cancelling work that has not started and replaying failed work
 * (PRD 5.1, 4.2).
 */
class TaskAdminIT extends AbstractIntegrationTest {

    @Autowired
    private TaskMapper taskMapper;

    @Test
    void cancelsTaskThatHasNotStartedAndRecordsWhoDidIt() {
        long taskId = submit("echo", "{}").task().id();

        TaskView cancelled = taskService.cancel(taskId, "alice", "no longer needed");

        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(cancelled.lastError()).contains("alice").contains("no longer needed");
    }

    @Test
    void rejectsCancellingARunningTask() {
        startWorker(1);
        long taskId = submit("echo", "{\"sleepMs\": 3000}").task().id();
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING));

        assertThatThrownBy(() -> taskService.cancel(taskId, "alice", "stop"))
                .isInstanceOf(IllegalTaskTransitionException.class)
                .hasMessageContaining("RUNNING -> CANCELLED");
    }

    /**
     * Even if a caller skipped the state machine, the database must refuse to cancel a running task.
     */
    @Test
    void databaseRefusesToCancelARunningTask() {
        startWorker(1);
        long taskId = submit("echo", "{\"sleepMs\": 3000}").task().id();
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING));

        int updated = taskMapper.markCancelled(taskId, "bypassing the state machine", LocalDateTime.now(clock));

        assertThat(updated).isZero();
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void rejectsASecondCancellation() {
        long taskId = submit("echo", "{}").task().id();
        taskService.cancel(taskId, "alice", "duplicate request");

        assertThatThrownBy(() -> taskService.cancel(taskId, "bob", "again"))
                .isInstanceOf(IllegalTaskTransitionException.class)
                .hasMessageContaining("CANCELLED -> CANCELLED");
    }

    @Test
    void requiresOperatorAndReasonForAdministrativeOperations() {
        long taskId = submit("echo", "{}").task().id();

        assertThatThrownBy(() -> taskService.cancel(taskId, "  ", "why"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator");
        assertThatThrownBy(() -> taskService.cancel(taskId, "alice", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reason");
    }

    @Test
    void replayOfFailedTaskRestartsExecutionAndKeepsBothAttempts() {
        startWorker(2);
        long taskId = submit("echo", "{\"fail\": true}").task().id();
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED));

        TaskView replayed = taskService.replay(taskId, "alice", "downstream recovered");

        assertThat(replayed.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(replayed.attemptCount()).isZero();

        // The payload still fails, so the point of this test is that a replay really executes again
        // and that both attempts remain visible.
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED));
        await().atMost(15, SECONDS).untilAsserted(() ->
                assertThat(taskService.executions(taskId)).hasSize(2));

        List<TaskExecutionView> executions = taskService.executions(taskId);
        assertThat(executions).extracting(TaskExecutionView::attempt).containsExactly(1, 1);
        assertThat(executions).extracting(TaskExecutionView::result)
                .containsExactly(ExecutionResult.FAILED, ExecutionResult.FAILED);
        assertThat(executions.getLast().fencingToken()).isEqualTo(2L);
    }

    @Test
    void rejectsReplayOfATaskThatDidNotFail() {
        long pendingTaskId = submit("echo", "{}").task().id();
        assertThatThrownBy(() -> taskService.replay(pendingTaskId, "alice", "just in case"))
                .isInstanceOf(IllegalTaskTransitionException.class)
                .hasMessageContaining("PENDING -> PENDING");

        long cancelledTaskId = submit("echo", "{}").task().id();
        taskService.cancel(cancelledTaskId, "alice", "not needed");
        assertThatThrownBy(() -> taskService.replay(cancelledTaskId, "alice", "actually needed"))
                .isInstanceOf(IllegalTaskTransitionException.class);
    }

    @Test
    void reportsUnknownTaskIdsInsteadOfSilentlySucceeding() {
        assertThatThrownBy(() -> taskService.cancel(999_999L, "alice", "cleanup"))
                .isInstanceOf(TaskNotFoundException.class);
        assertThatThrownBy(() -> taskService.replay(999_999L, "alice", "cleanup"))
                .isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void cancellingAFinishedTaskIsRejectedByTheConditionalUpdate() {
        long taskId = submit("echo", "{}").task().id();
        taskService.cancel(taskId, "alice", "first");

        assertThat(taskMapper.markCancelled(taskId, "second attempt", LocalDateTime.now(clock))).isZero();
        assertThatThrownBy(() -> taskService.cancel(taskId, "bob", "second"))
                .isInstanceOf(IllegalTaskTransitionException.class);
    }
}
