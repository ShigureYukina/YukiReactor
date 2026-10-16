package com.yuki.reactor.observability.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactor.observability.domain.ExperimentStrategy;

import java.time.LocalDateTime;

/**
 * One experiment round as reported by the caller that ran it (PRD 7.4, 10.1).
 *
 * <p>Structured snapshots are passed as JSON objects. They are stored as data and never interpreted
 * as code (PRD 8.4), and their shape may differ between experiments.
 */
public record ExperimentRunRequest(
        String experimentName,
        Integer roundIndex,
        ExperimentStrategy strategy,
        Integer taskCount,
        JsonNode concurrencyConfig,
        JsonNode faultConfig,
        Long randomSeed,
        JsonNode environment,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        JsonNode resultSummary,
        String notes) {
}
