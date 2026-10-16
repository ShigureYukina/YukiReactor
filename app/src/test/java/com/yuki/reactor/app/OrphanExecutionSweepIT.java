package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.recovery.RecoveryScanResult;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consistency repair (PRD 5.4 "result status write failure"): an execution row can stay open when the
 * result could not be written. Once the task has moved on, the row is closed as abandoned, while rows
 * of attempts that are genuinely in flight are left untouched.
 */
class OrphanExecutionSweepIT extends AbstractIntegrationTest {

    @Test
    void closesExecutionRowsWhoseTaskHasMovedOn() {
        long abandonedTask = submit("echo", "{}").task().id();
        jdbcTemplate.update("""
                INSERT INTO task_execution (task_id, attempt, worker_id, fencing_token, started_at)
                VALUES (?, ?, ?, ?, ?)
                """, abandonedTask, 1, "worker-ghost", 1L, Timestamp.valueOf(LocalDateTime.now(clock)));

        long runningTask = submit("echo", "{}").task().id();
        assertThat(taskOperations.claimAndOpen(
                runningTask, "worker-alive", LocalDateTime.now(clock), Duration.ofSeconds(60))).isNotNull();

        RecoveryScanResult result = scanOnce();

        assertThat(result.closedOrphanExecutions()).isEqualTo(1);
        List<TaskExecutionView> orphan = executions(abandonedTask);
        assertThat(orphan).hasSize(1);
        assertThat(orphan.get(0).result()).isEqualTo(ExecutionResult.ABANDONED);
        assertThat(orphan.get(0).errorCode()).isEqualTo(ErrorCodes.ORPHANED_EXECUTION);
        assertThat(orphan.get(0).errorCategory()).isEqualTo(ErrorCategory.EXECUTOR_LOST);
        assertThat(orphan.get(0).finishedAt()).isNotNull();

        List<TaskExecutionView> inFlight = executions(runningTask);
        assertThat(inFlight).hasSize(1);
        assertThat(inFlight.get(0).finishedAt()).as("a live attempt keeps its open record").isNull();
        assertThat(task(runningTask).status()).isEqualTo(TaskStatus.RUNNING);
    }
}
