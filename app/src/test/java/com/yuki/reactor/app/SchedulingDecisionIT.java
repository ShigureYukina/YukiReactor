package com.yuki.reactor.app;

import com.yuki.reactor.app.task.ScenarioTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.service.TaskClaim;
import com.yuki.reactor.worker.TaskWorkerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task level scheduling decisions (PRD 5.6): every row must come from the code path that took the
 * decision - the retry policy when it reschedules or gives up on a failed attempt, the dispatch loop
 * when it leaves a due task unclaimed, the recovery scanner when it takes a task back from an
 * executor that stopped renewing its lease. The tests read the rows from MySQL, so they also pin the
 * shape of the decision table a reader will query later.
 */
@Import(TestHandlersConfiguration.class)
class SchedulingDecisionIT extends AbstractIntegrationTest {

    @Test
    void recordsTheBackoffChosenForARescheduledAttempt() {
        startWorker(workerProperties(1));
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();

        awaitUntil(() -> decisionCount(taskId, ReasonCodes.RETRY_BACKOFF) > 0, Duration.ofSeconds(10));

        Map<String, Object> decision = lastDecision(taskId, ReasonCodes.RETRY_BACKOFF);
        assertThat(decision.get("decision_type")).isEqualTo("RETRY_SCHEDULED");
        assertThat(decision.get("execution_id")).isEqualTo(firstExecutionId(taskId));
        assertThat(decision.get("next_run_at")).as("the chosen retry time is part of the decision").isNotNull();
        assertThat(decision.get("concurrency_limit")).isEqualTo(1);
        assertThat(decision.get("worker_id")).isNotNull();
        assertThat(decision.get("running_tasks")).isNotNull();
        assertThat(decision.get("backlog")).isNotNull();
        assertThat((String) decision.get("observed_details"))
                .contains("attempt 1/3").contains("TRANSIENT").contains("backed off by")
                .contains("next attempt not before");
        assertThat(decisionNextRunAt(taskId))
                .as("the decision carries the retry time the task was actually given")
                .isEqualTo(taskNextRunAt(taskId));

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        assertThat(decisionCount(taskId, ReasonCodes.RETRY_BACKOFF))
                .as("a later successful attempt adds no second backoff decision")
                .isEqualTo(1);
    }

    @Test
    void recordsAUsedUpRetryBudgetAsTheDecisionThatEndedTheTask() {
        startWorker(workerProperties(1), retryWith(Duration.ofMillis(50), 0.0));
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 5}", 2).task().id();

        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(30));

        Map<String, Object> decision = lastDecision(taskId, ReasonCodes.MAX_RETRIES_EXCEEDED);
        assertThat(decision.get("decision_type")).isEqualTo("RETRY_EXHAUSTED");
        assertThat(decision.get("execution_id")).isEqualTo(lastExecutionId(taskId));
        assertThat((String) decision.get("observed_details"))
                .contains("attempt 2/2").contains("retry budget exhausted");
        assertThat(decisionCount(taskId, ReasonCodes.MAX_RETRIES_EXCEEDED)).isEqualTo(1);
        assertThat(decisionCount(taskId, ReasonCodes.RETRY_BACKOFF))
                .as("the first attempt was still rescheduled and keeps its own explanation")
                .isEqualTo(1);
        assertThat(task(taskId).lastError()).contains(ErrorCodes.MAX_RETRIES_EXCEEDED);
    }

    @Test
    void writesNoDecisionForAFailureThatWasNeverRetryable() throws InterruptedException {
        startWorker(2);
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE,
                "{\"failures\": 1, \"failureKind\": \"rejection\"}", 3).task().id();

        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(20));
        Thread.sleep(500L);

        assertThat(decisionCountForTask(taskId))
                .as("a business rejection is not a scheduling decision; the execution record explains it")
                .isZero();
    }

    @Test
    void recordsTheReclaimedLeaseOfAnExpiredAttempt() throws InterruptedException {
        long taskId = submit("echo", "{\"sleepMs\": 10}").task().id();
        TaskClaim claim = taskOperations.claimAndOpen(
                taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));

        Thread.sleep(1200L);
        assertThat(scanOnce().reclaimedForRetry()).isEqualTo(1);

        Map<String, Object> decision = lastDecision(taskId, ReasonCodes.LEASE_EXPIRED);
        assertThat(decision.get("decision_type")).isEqualTo("LEASE_RECLAIMED");
        assertThat(decision.get("worker_id")).isEqualTo("worker-crashed");
        assertThat(decision.get("execution_id")).isEqualTo(claim.execution().getId());
        assertThat((String) decision.get("observed_details"))
                .contains("lease expired").contains("requeued for the next claim");
    }

    @Test
    void recordsAReclaimedLeaseThatAlsoUsedUpTheAttemptBudget() throws InterruptedException {
        long taskId = submit("echo", "{}", 1).task().id();
        taskOperations.claimAndOpen(taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));

        Thread.sleep(1200L);
        assertThat(scanOnce().reclaimedAsFailed()).isEqualTo(1);

        Map<String, Object> decision = lastDecision(taskId, ReasonCodes.LEASE_EXPIRED);
        assertThat(decision.get("decision_type")).isEqualTo("LEASE_RECLAIMED");
        assertThat((String) decision.get("observed_details")).contains("retry budget exhausted");
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    void explainsACapacityDeferralOncePerAttempt() {
        // Submitted before the worker starts, so the first dispatch round certainly sees both as due.
        long holder = submit(ScenarioTaskHandler.TASK_TYPE, "{\"sleepMs\": 1500}").task().id();
        long waiting = submit("echo", "{}").task().id();
        startWorker(workerProperties(1));

        awaitUntil(() -> decisionCount(waiting, ReasonCodes.CAPACITY_LIMIT) > 0, Duration.ofSeconds(10));

        Map<String, Object> decision = lastDecision(waiting, ReasonCodes.CAPACITY_LIMIT);
        assertThat(decision.get("decision_type")).isEqualTo("TASK_DEFERRAL");
        assertThat(decision.get("execution_id")).as("a waiting task has no attempt record yet").isNull();
        assertThat(decision.get("next_run_at")).isNull();
        assertThat(decision.get("concurrency_limit")).isEqualTo(1);
        assertThat((String) decision.get("observed_details"))
                .contains("attempt 0 left unclaimed").contains("execution slots were free");

        awaitStatus(holder, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        awaitStatus(waiting, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        assertThat(decisionCount(waiting, ReasonCodes.CAPACITY_LIMIT))
                .as("the task was left waiting for many polling rounds but keeps a single explanation")
                .isEqualTo(1);
    }

    @Test
    void explainsAPriorityWaitWhenTheBatchWasFilledWithSlotsLeft() {
        long holder = submit(ScenarioTaskHandler.TASK_TYPE, "{\"sleepMs\": 1500}").task().id();
        long waiting = submit("echo", "{}").task().id();
        TaskWorkerProperties properties = workerProperties(3);
        properties.setBatchSize(1);
        startWorker(properties);

        awaitUntil(() -> decisionCount(waiting, ReasonCodes.PRIORITY_WAIT) > 0, Duration.ofSeconds(10));

        Map<String, Object> decision = lastDecision(waiting, ReasonCodes.PRIORITY_WAIT);
        assertThat(decision.get("decision_type")).isEqualTo("TASK_DEFERRAL");
        assertThat(decision.get("concurrency_limit")).isEqualTo(3);
        assertThat((String) decision.get("observed_details"))
                .contains("attempt 0 left unclaimed").contains("batch size 1 was filled");

        awaitStatus(holder, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        awaitStatus(waiting, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
    }

    private LocalDateTime taskNextRunAt(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT next_run_at FROM task WHERE id = ?", LocalDateTime.class, taskId);
    }

    private LocalDateTime decisionNextRunAt(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT next_run_at FROM scheduler_decision WHERE task_id = ? AND reason_code = ?",
                LocalDateTime.class, taskId, ReasonCodes.RETRY_BACKOFF);
    }

    private long decisionCount(long taskId, String reasonCode) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM scheduler_decision WHERE task_id = ? AND reason_code = ?",
                Long.class, taskId, reasonCode);
        return count == null ? 0L : count;
    }

    private long decisionCountForTask(long taskId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM scheduler_decision WHERE task_id = ?", Long.class, taskId);
        return count == null ? 0L : count;
    }

    private Map<String, Object> lastDecision(long taskId, String reasonCode) {
        return jdbcTemplate.queryForMap(
                "SELECT decision_type, reason_code, execution_id, concurrency_limit, next_run_at, running_tasks, "
                        + "backlog, worker_id, observed_details FROM scheduler_decision "
                        + "WHERE task_id = ? AND reason_code = ? ORDER BY id DESC LIMIT 1",
                taskId, reasonCode);
    }

    private Long firstExecutionId(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT id FROM task_execution WHERE task_id = ? AND attempt = 1", Long.class, taskId);
    }

    private Long lastExecutionId(long taskId) {
        return jdbcTemplate.queryForObject(
                "SELECT id FROM task_execution WHERE task_id = ? ORDER BY id DESC LIMIT 1", Long.class, taskId);
    }
}
