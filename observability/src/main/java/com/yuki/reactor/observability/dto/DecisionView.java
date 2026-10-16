package com.yuki.reactor.observability.dto;

import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.SchedulerDecision;

import java.time.LocalDateTime;

/**
 * Read model of one recorded scheduling decision (PRD 5.6).
 *
 * <p>Everything here was written by the code that took the decision. A reader must never have to
 * derive a reason from the task status, which is why the reason code and the observed values are
 * part of the decision itself.
 */
public record DecisionView(
        long id,
        Long taskId,
        Long executionId,
        DecisionType decisionType,
        String reasonCode,
        String workerId,
        Integer concurrencyBefore,
        Integer concurrencyAfter,
        Integer concurrencyLimit,
        LocalDateTime nextRunAt,
        Integer runningTasks,
        Integer backlog,
        Double downstreamLatencyMs,
        Double downstreamErrorRate,
        String observedDetails,
        LocalDateTime createdAt) {

    public static DecisionView from(SchedulerDecision decision) {
        return new DecisionView(
                decision.getId(),
                decision.getTaskId(),
                decision.getExecutionId(),
                decision.getDecisionType(),
                decision.getReasonCode(),
                decision.getWorkerId(),
                decision.getConcurrencyBefore(),
                decision.getConcurrencyAfter(),
                decision.getConcurrencyLimit(),
                decision.getNextRunAt(),
                decision.getRunningTasks(),
                decision.getBacklog(),
                decision.getDownstreamLatencyMs(),
                decision.getDownstreamErrorRate(),
                decision.getObservedDetails(),
                decision.getCreatedAt());
    }
}
