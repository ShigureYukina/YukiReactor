package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.service.TaskClaim;
import com.yuki.reactor.recovery.RecoveryScanResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lease expiry recovery (PRD 5.3). The executor is simulated as lost: the task is claimed but the
 * lease is never renewed. The recovery scan must reclaim it, keep both execution records, and reject
 * any late result coming from the previous owner.
 */
class LeaseExpiryRecoveryIT extends AbstractIntegrationTest {

    @Test
    void reclaimsAnExpiredLeaseAndLetsAnotherWorkerFinishTheTask() throws InterruptedException {
        long taskId = submit("echo", "{\"sleepMs\": 10}").task().id();
        TaskClaim claim = taskOperations.claimAndOpen(
                taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));
        assertThat(claim).isNotNull();

        assertThat(scanOnce().recoveredTasks())
                .as("a valid lease must never be reclaimed")
                .isZero();

        Thread.sleep(1200L);
        RecoveryScanResult result = scanOnce();
        assertThat(result.reclaimedForRetry()).isEqualTo(1);
        assertThat(result.reclaimedAsFailed()).isZero();

        TaskView reclaimed = task(taskId);
        assertThat(reclaimed.status()).isEqualTo(TaskStatus.RETRY_WAIT);
        assertThat(reclaimed.lastError()).contains(ErrorCodes.LEASE_EXPIRED);
        assertThat(reclaimed.nextRunAt()).isNotNull();

        assertThat(taskOperations.complete(
                taskId, claim.execution().getId(), claim.task().getFencingToken(), LocalDateTime.now(clock)))
                .as("the lost executor must not be able to report a result")
                .isFalse();
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RETRY_WAIT);

        startWorker(1);
        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        List<TaskExecutionView> executions = executions(taskId);
        assertThat(executions).hasSize(2);
        assertThat(executions).extracting(TaskExecutionView::attempt).containsExactly(1, 2);
        assertThat(executions).extracting(TaskExecutionView::result)
                .containsExactly(ExecutionResult.ABANDONED, ExecutionResult.SUCCEEDED);
        assertThat(executions.get(0).errorCode()).isEqualTo(ErrorCodes.LEASE_EXPIRED);
        assertThat(executions.get(0).errorCategory()).isEqualTo(ErrorCategory.EXECUTOR_LOST);
        assertThat(executions.get(0).finishedAt()).isNotNull();
        assertThat(executions.get(1).fencingToken()).as("the new attempt owns a newer token").isEqualTo(2L);
    }

    @Test
    void failsTheTaskWhenTheAttemptBudgetIsSpent() throws InterruptedException {
        long taskId = submit("echo", "{}", 1).task().id();
        taskOperations.claimAndOpen(taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));

        Thread.sleep(1200L);
        RecoveryScanResult result = scanOnce();

        assertThat(result.reclaimedAsFailed()).isEqualTo(1);
        assertThat(result.reclaimedForRetry()).isZero();
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task(taskId).lastError()).contains(ErrorCodes.LEASE_EXPIRED).contains("retry limit reached");
    }

    @Test
    void scheduledScannerRecoversExpiredLeasesOnItsOwn() throws InterruptedException {
        long taskId = submit("echo", "{\"sleepMs\": 10}").task().id();
        taskOperations.claimAndOpen(taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));

        startRecoveryScanner();

        awaitStatus(taskId, TaskStatus.RETRY_WAIT, Duration.ofSeconds(15));
        assertThat(task(taskId).lastError()).contains(ErrorCodes.LEASE_EXPIRED);
        assertThat(countExecutions(taskId)).isEqualTo(1);
    }
}
