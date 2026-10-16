package com.yuki.reactor.observability.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;

/** Read model of one recorded experiment round (PRD 7.4). */
public record ExperimentRunView(
        long id,
        String experimentName,
        int roundIndex,
        String strategy,
        int taskCount,
        JsonNode concurrencyConfig,
        JsonNode faultConfig,
        Long randomSeed,
        JsonNode environment,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        JsonNode resultSummary,
        String notes,
        LocalDateTime createdAt) {
}
