package com.yuki.reactor.adaptive;

import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;

/**
 * Writes the scheduling decisions of the adaptive controller to {@code scheduler_decision}. Only
 * real adjustments and real fallbacks are recorded - never a sampling point - so the audit table
 * stays small (PRD 7.3).
 */
@Service
public class ConcurrencyDecisionRecorder {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyDecisionRecorder.class);

    private static final int MAX_DETAILS_LENGTH = 500;

    private final SchedulerDecisionMapper decisionMapper;
    private final Clock clock;

    public ConcurrencyDecisionRecorder(SchedulerDecisionMapper decisionMapper, Clock clock) {
        this.decisionMapper = decisionMapper;
        this.clock = clock;
    }

    public void record(DecisionType decisionType, String reasonCode, String workerId, int concurrencyBefore,
                       int concurrencyAfter, Long runningTasks, Long backlog, Double downstreamLatencyMs,
                       Double downstreamErrorRate, String details) {
        SchedulerDecision decision = new SchedulerDecision();
        decision.setDecisionType(decisionType);
        decision.setReasonCode(reasonCode);
        decision.setWorkerId(workerId);
        decision.setConcurrencyBefore(concurrencyBefore);
        decision.setConcurrencyAfter(concurrencyAfter);
        decision.setRunningTasks(runningTasks == null ? null : runningTasks.intValue());
        decision.setBacklog(backlog == null ? null : backlog.intValue());
        decision.setDownstreamLatencyMs(downstreamLatencyMs);
        decision.setDownstreamErrorRate(downstreamErrorRate);
        decision.setObservedDetails(truncate(details));
        decision.setCreatedAt(LocalDateTime.now(clock));
        decisionMapper.insert(decision);
        log.info("decision {} {} on {}: concurrency {} -> {} (running={} backlog={} downstreamLatencyMs={} downstreamErrorRate={} details={})",
                decisionType, reasonCode, workerId, concurrencyBefore, concurrencyAfter, runningTasks, backlog,
                downstreamLatencyMs, downstreamErrorRate, decision.getObservedDetails());
    }

    private static String truncate(String details) {
        if (details == null) {
            return null;
        }
        return details.length() <= MAX_DETAILS_LENGTH ? details : details.substring(0, MAX_DETAILS_LENGTH);
    }
}
