package com.yuki.reactor.core.decision;

import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.signals.RuntimeSignals;
import com.yuki.reactor.core.signals.SignalSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes the task level scheduling decisions to {@code scheduler_decision} (PRD 5.6, 7.3).
 *
 * <p>Each row comes from the code that actually took the decision: the retry policy when it
 * reschedules or gives up on a failed attempt, the dispatch loop when it leaves a due task
 * unclaimed, the recovery scanner when it takes a task back from an executor that stopped renewing
 * its lease. No row is derived from a task status by a later reader.
 *
 * <p>The context of a decision - how much is running, how much is due, what the process observed
 * downstream - is read here, in the same transaction as the state change it explains.
 *
 * <p>A deferral is only recorded once per task and reason as long as the task stays on the same
 * attempt: a task that keeps waiting is explained by one row, not by one row per polling round.
 * That keeps the audit table small (PRD 7.3) without hiding a repeated decision on a later attempt.
 */
@Service
public class SchedulerDecisionRecorder {

    private static final Logger log = LoggerFactory.getLogger(SchedulerDecisionRecorder.class);

    private static final int MAX_DETAILS_LENGTH = 500;

    /** Upper bound of the in-memory deferral memory; beyond it the guard restarts (worst case: a repeat). */
    private static final int MAX_DEFERRAL_MEMORY = 20_000;

    private final SchedulerDecisionMapper decisionMapper;
    private final TaskMapper taskMapper;
    private final RuntimeSignals signals;

    /** {@code taskId|reasonCode} -> attempt count that was already explained, this process only. */
    private final Map<String, Integer> deferralsExplained = new ConcurrentHashMap<>();

    public SchedulerDecisionRecorder(SchedulerDecisionMapper decisionMapper,
                                     TaskMapper taskMapper,
                                     RuntimeSignals signals) {
        this.decisionMapper = decisionMapper;
        this.taskMapper = taskMapper;
        this.signals = signals;
    }

    /**
     * A failed attempt was rescheduled with a backoff delay (PRD 5.4).
     *
     * @param now       time of the state change, used as the decision time
     * @param nextRunAt earliest time the task may be claimed again
     */
    public void recordRetryScheduled(long taskId, long executionId, String workerId, Integer concurrencyLimit,
                                     LocalDateTime now, LocalDateTime nextRunAt, int attempt, int maxAttempts,
                                     ErrorCategory category) {
        long delayMillis = Math.max(0L, Duration.between(now, nextRunAt).toMillis());
        String details = "attempt " + attempt + "/" + maxAttempts + " failed as " + category
                + "; backed off by " + delayMillis + " ms, next attempt not before " + nextRunAt;
        write(DecisionType.RETRY_SCHEDULED, ReasonCodes.RETRY_BACKOFF, taskId, executionId, workerId,
                concurrencyLimit, nextRunAt, details, now);
    }

    /**
     * A retryable failure used the last attempt, so the task reached {@code FAILED} (PRD 5.4).
     */
    public void recordRetriesExhausted(long taskId, long executionId, String workerId, Integer concurrencyLimit,
                                       LocalDateTime now, int attempt, int maxAttempts, ErrorCategory category) {
        String details = "attempt " + attempt + "/" + maxAttempts + " failed as " + category
                + "; retry budget exhausted, task failed";
        write(DecisionType.RETRY_EXHAUSTED, ReasonCodes.MAX_RETRIES_EXCEEDED, taskId, executionId, workerId,
                concurrencyLimit, null, details, now);
    }

    /**
     * Recovery took a task back from an executor whose lease expired (PRD 5.3).
     *
     * @param executionId  the abandoned attempt that was closed, or {@code null} when none was open
     * @param leaseOwner   worker that stopped renewing the lease
     * @param leaseUntil   the lease that expired
     * @param exhausted    whether the attempt budget was used up, which makes the task {@code FAILED}
     */
    public void recordLeaseReclaimed(long taskId, Long executionId, String leaseOwner, LocalDateTime leaseUntil,
                                     LocalDateTime now, int attempt, int maxAttempts, boolean exhausted) {
        String details = "lease expired at " + leaseUntil + " without a renewal from " + leaseOwner
                + "; attempt " + attempt + "/" + maxAttempts + (exhausted
                ? ", retry budget exhausted, task failed"
                : ", requeued for the next claim");
        write(DecisionType.LEASE_RECLAIMED, ReasonCodes.LEASE_EXPIRED, taskId, executionId, leaseOwner,
                null, null, details, now);
    }

    /**
     * A due task was not admitted by the dispatch round.
     *
     * @param reasonCode {@link ReasonCodes#CAPACITY_LIMIT} or {@link ReasonCodes#PRIORITY_WAIT}
     * @param cause      which rule deferred it, written into {@code observed_details}
     */
    public void recordTaskDeferral(long taskId, int attempt, String reasonCode, String workerId,
                                   Integer concurrencyLimit, String cause, LocalDateTime now) {
        String key = taskId + "|" + reasonCode;
        Integer explainedAttempt = deferralsExplained.get(key);
        if (explainedAttempt != null && explainedAttempt == attempt) {
            return;
        }
        if (deferralsExplained.size() >= MAX_DEFERRAL_MEMORY) {
            deferralsExplained.clear();
        }
        deferralsExplained.put(key, attempt);

        String details = "attempt " + attempt + " left unclaimed: " + cause;
        write(DecisionType.TASK_DEFERRAL, reasonCode, taskId, null, workerId, concurrencyLimit, null, details, now);
    }

    private void write(DecisionType decisionType, String reasonCode, long taskId, Long executionId, String workerId,
                       Integer concurrencyLimit, LocalDateTime nextRunAt, String details, LocalDateTime now) {
        SchedulerDecision decision = new SchedulerDecision();
        decision.setTaskId(taskId);
        decision.setExecutionId(executionId);
        decision.setDecisionType(decisionType);
        decision.setReasonCode(reasonCode);
        decision.setWorkerId(workerId);
        decision.setConcurrencyLimit(concurrencyLimit);
        decision.setNextRunAt(nextRunAt);
        decision.setRunningTasks((int) taskMapper.countRunning());
        decision.setBacklog((int) taskMapper.countDueBacklog(now));
        SignalSnapshot snapshot = signals.snapshot();
        if (snapshot.measured()) {
            decision.setDownstreamLatencyMs(snapshot.averageDownstreamLatencyMillis());
            decision.setDownstreamErrorRate(snapshot.downstreamErrorRate());
        }
        decision.setObservedDetails(truncate(details));
        decision.setCreatedAt(now);
        decisionMapper.insert(decision);
        log.info("decision {} {} for task {} (attempt record {}): running={} backlog={} concurrencyLimit={} details={}",
                decisionType, reasonCode, taskId, executionId, decision.getRunningTasks(), decision.getBacklog(),
                concurrencyLimit, decision.getObservedDetails());
    }

    private static String truncate(String details) {
        if (details == null) {
            return null;
        }
        return details.length() <= MAX_DETAILS_LENGTH ? details : details.substring(0, MAX_DETAILS_LENGTH);
    }
}
