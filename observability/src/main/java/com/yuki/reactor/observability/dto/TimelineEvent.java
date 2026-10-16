package com.yuki.reactor.observability.dto;

import com.yuki.reactor.core.domain.DecisionType;

import java.time.LocalDateTime;

/**
 * One moment on a task timeline.
 *
 * <p>{@code decisionType} and {@code reasonCode} are set only for {@link TimelineEventType#DECISION}
 * events and carry the recorded reason, not an interpretation of the current task status.
 */
public record TimelineEvent(
        LocalDateTime at,
        TimelineEventType type,
        Long executionId,
        DecisionType decisionType,
        String reasonCode,
        String details) {
}
