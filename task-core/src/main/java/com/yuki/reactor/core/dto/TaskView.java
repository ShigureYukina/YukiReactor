package com.yuki.reactor.core.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskStatus;

import java.time.LocalDateTime;

/**
 * Read model of a task as returned by the API.
 *
 * <p>Lease ownership and fencing tokens are deliberately absent: they describe internal scheduling
 * mechanics, and exposing them would invite clients to depend on them.
 */
public record TaskView(
        long id,
        String taskType,
        JsonNode payload,
        TaskStatus status,
        int priority,
        String idempotencyScope,
        String idempotencyKey,
        int attemptCount,
        int maxAttempts,
        LocalDateTime nextRunAt,
        long timeoutMs,
        String lastError,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {

    public static TaskView from(Task task, ObjectMapper objectMapper) {
        return new TaskView(
                task.getId(),
                task.getTaskType(),
                parsePayload(task.getPayload(), objectMapper),
                task.getStatus(),
                task.getPriority(),
                task.getIdempotencyScope(),
                task.getIdempotencyKey(),
                task.getAttemptCount(),
                task.getMaxAttempts(),
                task.getNextRunAt(),
                task.getTimeoutMs(),
                task.getLastError(),
                task.getCreatedAt(),
                task.getUpdatedAt());
    }

    private static JsonNode parsePayload(String payload, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(payload);
        } catch (Exception e) {
            // The payload was validated at submission time; this only guards against hand-edited rows.
            return objectMapper.getNodeFactory().textNode(payload);
        }
    }
}
