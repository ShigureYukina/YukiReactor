package com.yuki.reactor.core.decision;

import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.signals.RuntimeSignals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The decisions written for a task are the explanation a reader gets later (PRD 5.6): each real
 * decision must produce the row that describes it, with the context it was taken in, and a task that
 * keeps waiting must stay at one row per attempt instead of filling the audit table (PRD 7.3).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SchedulerDecisionRecorderTest {

    private final LocalDateTime now = LocalDateTime.of(2026, 10, 16, 10, 0, 0, 0);

    @Mock
    private SchedulerDecisionMapper decisionMapper;

    @Mock
    private TaskMapper taskMapper;

    private RuntimeSignals signals;

    private SchedulerDecisionRecorder recorder;

    @BeforeEach
    void setUp() {
        signals = new RuntimeSignals();
        recorder = new SchedulerDecisionRecorder(decisionMapper, taskMapper, signals);
        when(taskMapper.countRunning()).thenReturn(3L);
        when(taskMapper.countDueBacklog(any())).thenReturn(7L);
    }

    @Test
    void recordsTheBackoffChosenForARescheduledAttempt() {
        signals.recordDownstreamCall(900, false);
        LocalDateTime nextRunAt = now.plusSeconds(2);

        recorder.recordRetryScheduled(42L, 7L, "worker-a", 4, now, nextRunAt, 2, 3, ErrorCategory.TRANSIENT);

        SchedulerDecision decision = capturedSingle();
        assertThat(decision.getTaskId()).isEqualTo(42L);
        assertThat(decision.getExecutionId()).isEqualTo(7L);
        assertThat(decision.getDecisionType()).isEqualTo(DecisionType.RETRY_SCHEDULED);
        assertThat(decision.getReasonCode()).isEqualTo(ReasonCodes.RETRY_BACKOFF);
        assertThat(decision.getWorkerId()).isEqualTo("worker-a");
        assertThat(decision.getConcurrencyLimit()).isEqualTo(4);
        assertThat(decision.getNextRunAt()).isEqualTo(nextRunAt);
        assertThat(decision.getRunningTasks()).isEqualTo(3);
        assertThat(decision.getBacklog()).isEqualTo(7);
        assertThat(decision.getDownstreamLatencyMs()).isEqualTo(900.0);
        assertThat(decision.getDownstreamErrorRate()).isZero();
        assertThat(decision.getCreatedAt()).isEqualTo(now);
        assertThat(decision.getObservedDetails())
                .contains("attempt 2/3").contains("TRANSIENT").contains("backed off by 2000 ms");
    }

    @Test
    void recordsAUsedUpRetryBudgetAsTheEndOfTheTask() {
        recorder.recordRetriesExhausted(11L, 22L, "worker-b", 2, now, 3, 3, ErrorCategory.TRANSIENT);

        SchedulerDecision decision = capturedSingle();
        assertThat(decision.getDecisionType()).isEqualTo(DecisionType.RETRY_EXHAUSTED);
        assertThat(decision.getReasonCode()).isEqualTo(ReasonCodes.MAX_RETRIES_EXCEEDED);
        assertThat(decision.getExecutionId()).isEqualTo(22L);
        assertThat(decision.getNextRunAt()).as("nothing is rescheduled when the budget is used up").isNull();
        assertThat(decision.getDownstreamLatencyMs())
                .as("an empty signal window stays unknown instead of looking healthy")
                .isNull();
        assertThat(decision.getDownstreamErrorRate()).isNull();
        assertThat(decision.getObservedDetails()).contains("attempt 3/3").contains("retry budget exhausted");
    }

    @Test
    void attributesAReclaimedLeaseToTheExecutorThatHeldIt() {
        LocalDateTime leaseUntil = now.minusSeconds(1);

        recorder.recordLeaseReclaimed(9L, 33L, "worker-gone", leaseUntil, now, 1, 2, false);

        SchedulerDecision decision = capturedSingle();
        assertThat(decision.getDecisionType()).isEqualTo(DecisionType.LEASE_RECLAIMED);
        assertThat(decision.getReasonCode()).isEqualTo(ReasonCodes.LEASE_EXPIRED);
        assertThat(decision.getTaskId()).isEqualTo(9L);
        assertThat(decision.getExecutionId()).isEqualTo(33L);
        assertThat(decision.getWorkerId()).isEqualTo("worker-gone");
        assertThat(decision.getObservedDetails())
                .contains("lease expired at " + leaseUntil).contains("requeued for the next claim");
    }

    @Test
    void explainsARepeatedDeferralOnceWhileTheAttemptStaysTheSame() {
        recorder.recordTaskDeferral(5L, 0, ReasonCodes.CAPACITY_LIMIT, "worker-a", 1,
                "concurrency limit of 1 is fully used by running attempts", now);
        recorder.recordTaskDeferral(5L, 0, ReasonCodes.CAPACITY_LIMIT, "worker-a", 1,
                "concurrency limit of 1 is fully used by running attempts", now);

        ArgumentCaptor<SchedulerDecision> captor = ArgumentCaptor.forClass(SchedulerDecision.class);
        verify(decisionMapper, times(1)).insert(captor.capture());

        SchedulerDecision decision = captor.getValue();
        assertThat(decision.getDecisionType()).isEqualTo(DecisionType.TASK_DEFERRAL);
        assertThat(decision.getReasonCode()).isEqualTo(ReasonCodes.CAPACITY_LIMIT);
        assertThat(decision.getExecutionId()).as("no attempt record exists for a waiting task").isNull();
        assertThat(decision.getConcurrencyLimit()).isEqualTo(1);
        assertThat(decision.getObservedDetails()).startsWith("attempt 0 left unclaimed");

        recorder.recordTaskDeferral(5L, 0, ReasonCodes.PRIORITY_WAIT, "worker-a", 1, "batch size 1 was filled", now);
        verify(decisionMapper, times(2)).insert(any(SchedulerDecision.class));

        recorder.recordTaskDeferral(5L, 1, ReasonCodes.CAPACITY_LIMIT, "worker-a", 1,
                "concurrency limit of 1 is fully used by running attempts", now);
        verify(decisionMapper, times(3)).insert(any(SchedulerDecision.class));
    }

    @Test
    void truncatesTheDetailsOfALongCause() {
        recorder.recordTaskDeferral(6L, 0, ReasonCodes.PRIORITY_WAIT, "worker-a", 2, "x".repeat(700), now);

        assertThat(capturedSingle().getObservedDetails()).hasSize(500);
    }

    private SchedulerDecision capturedSingle() {
        ArgumentCaptor<SchedulerDecision> captor = ArgumentCaptor.forClass(SchedulerDecision.class);
        verify(decisionMapper).insert(captor.capture());
        return captor.getValue();
    }
}
