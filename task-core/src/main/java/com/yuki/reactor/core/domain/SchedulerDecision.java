package com.yuki.reactor.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One scheduling or control decision, stored for later explanation (PRD 5.6, 7.3).
 *
 * <p>Only real decisions are stored, not every metric sample: the table is audit data and must not become
 * the place where high frequency metrics pile up. Task level fields stay {@code null} for process level
 * decisions such as a concurrency adjustment.
 */
@Data
@TableName("scheduler_decision")
public class SchedulerDecision {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long taskId;

    /** Attempt the decision is about; {@code null} for process level decisions. */
    private Long executionId;

    private DecisionType decisionType;

    private String reasonCode;

    private String workerId;

    /** Concurrency before an adjustment; only written by {@link DecisionType#CONCURRENCY_ADJUSTMENT}. */
    private Integer concurrencyBefore;

    /** Concurrency after an adjustment; only written by {@link DecisionType#CONCURRENCY_ADJUSTMENT}. */
    private Integer concurrencyAfter;

    /** Concurrency in force when the decision was taken, for decisions that do not change it. */
    private Integer concurrencyLimit;

    /** Retry time the decision chose, written by {@link ReasonCodes#RETRY_BACKOFF}. */
    private LocalDateTime nextRunAt;

    private Integer runningTasks;

    private Integer backlog;

    private Double downstreamLatencyMs;

    private Double downstreamErrorRate;

    private String observedDetails;

    private LocalDateTime createdAt;
}
