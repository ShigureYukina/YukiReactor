package com.yuki.reactor.core.service;

/**
 * Submission request after HTTP-level validation.
 *
 * @param taskType         task type that must resolve to a registered handler
 * @param payload          JSON object as text, or {@code null} for an empty payload
 * @param priority         lower value is scheduled first, defaults to 100
 * @param maxAttempts      attempt budget, defaults to 3
 * @param timeoutMs        execution timeout, defaults to 60000
 * @param idempotencyScope scope owning the idempotency key, defaults to {@code default}
 * @param idempotencyKey   client supplied key, or {@code null} when the caller accepts duplicates
 */
public record SubmitTaskCommand(
        String taskType,
        String payload,
        Integer priority,
        Integer maxAttempts,
        Long timeoutMs,
        String idempotencyScope,
        String idempotencyKey) {
}
