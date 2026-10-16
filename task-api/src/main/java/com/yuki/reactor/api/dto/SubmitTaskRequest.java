package com.yuki.reactor.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactor.core.service.SubmitTaskCommand;

/**
 * Request body for {@code POST /api/v1/tasks}.
 *
 * <p>{@code payload} is bound as a JSON node so a malformed document is rejected before it reaches
 * the domain, and so nothing here can be interpreted as executable text.
 */
public record SubmitTaskRequest(
        String taskType,
        JsonNode payload,
        Integer priority,
        Integer maxAttempts,
        Long timeoutMs,
        String idempotencyScope,
        String idempotencyKey) {

    public SubmitTaskCommand toCommand() {
        return new SubmitTaskCommand(
                taskType,
                payload == null ? null : payload.toString(),
                priority,
                maxAttempts,
                timeoutMs,
                idempotencyScope,
                idempotencyKey);
    }
}
