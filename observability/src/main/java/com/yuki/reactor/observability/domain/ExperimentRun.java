package com.yuki.reactor.observability.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One round of one experiment (PRD 7.4).
 *
 * <p>Structured snapshots (concurrency configuration, fault injection, environment, result summary)
 * are kept as JSON text: they are data for later comparison, never interpreted as code, and their
 * shape may legitimately differ between experiments. {@code resultSummary} stays {@code null} while
 * the round runs, and nothing here is filled in by guessing - a round only gets a result when the
 * experiment really produced one.
 */
@Data
@TableName("experiment_run")
public class ExperimentRun {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String experimentName;

    private int roundIndex;

    private ExperimentStrategy strategy;

    private int taskCount;

    private String concurrencyConfig;

    private String faultConfig;

    private Long randomSeed;

    private String environment;

    private LocalDateTime startedAt;

    private LocalDateTime finishedAt;

    private String resultSummary;

    private String notes;

    private LocalDateTime createdAt;
}
