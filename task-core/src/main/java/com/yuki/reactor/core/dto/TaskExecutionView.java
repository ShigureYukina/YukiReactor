package com.yuki.reactor.core.dto;

import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.error.ErrorCategory;

import java.time.LocalDateTime;

/**
 * Read model of one execution attempt (PRD 7.2). {@code result} is {@code null} while the attempt
 * is still running; {@code errorCategory} classifies a failure (PRD 5.4).
 */
public record TaskExecutionView(
        long id,
        long taskId,
        int attempt,
        String workerId,
        long fencingToken,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        ExecutionResult result,
        String errorCode,
        ErrorCategory errorCategory,
        String errorMessage) {

    public static TaskExecutionView from(TaskExecution execution) {
        return new TaskExecutionView(
                execution.getId(),
                execution.getTaskId(),
                execution.getAttempt(),
                execution.getWorkerId(),
                execution.getFencingToken(),
                execution.getStartedAt(),
                execution.getFinishedAt(),
                execution.getResult(),
                execution.getErrorCode(),
                execution.getErrorCategory(),
                execution.getErrorMessage());
    }
}
