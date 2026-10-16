package com.yuki.reactor.observability.dto;

import com.yuki.reactor.core.domain.DecisionType;

import java.time.LocalDateTime;

/** Filter for the decision query: every field is optional (PRD 5.6). */
public record DecisionQuery(
        Long taskId,
        DecisionType type,
        String reasonCode,
        LocalDateTime from,
        LocalDateTime to,
        int page,
        int size) {
}
