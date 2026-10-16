package com.yuki.reactor.core.error;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.support.Messages;

import java.time.Duration;
import java.util.Objects;

/**
 * A classified failure as understood by the engine: what kind of error it was, the code written to
 * the execution history, the human readable message and an optional hint from the dependency about
 * how long to wait before the next attempt.
 *
 * @param category       classification used to decide whether to retry (PRD 5.4)
 * @param code           error code stored on {@code task_execution.error_code}
 * @param message        human readable description stored on {@code task_execution.error_message}
 * @param retryAfterHint minimum delay requested by the failing dependency, may be {@code null}
 */
public record TaskFailure(ErrorCategory category, String code, String message, Duration retryAfterHint) {

    public TaskFailure {
        Objects.requireNonNull(category, "category");
        if (code == null || code.isBlank()) {
            code = ErrorCodes.EXECUTION_ERROR;
        }
        if (message == null || message.isBlank()) {
            message = category.name();
        }
    }

    /**
     * Classifies a handler exception. {@link TaskFailureException} carries its own category and
     * optional hint; every other throwable is treated as {@link ErrorCategory#UNKNOWN}.
     */
    public static TaskFailure classify(Throwable failure) {
        if (failure instanceof TaskFailureException described) {
            return new TaskFailure(
                    described.category(),
                    ErrorCodes.EXECUTION_ERROR,
                    Messages.describe(described),
                    described.retryAfter());
        }
        return new TaskFailure(
                ErrorCategory.UNKNOWN,
                ErrorCodes.EXECUTION_ERROR,
                Messages.describe(failure),
                null);
    }

    /**
     * Failure produced by the worker watchdog when an attempt exceeded its configured timeout.
     */
    public static TaskFailure timeout(long timeoutMs) {
        return new TaskFailure(
                ErrorCategory.TIMEOUT,
                ErrorCodes.EXECUTION_TIMEOUT,
                "execution exceeded the configured timeout of " + timeoutMs + " ms",
                null);
    }

    public static TaskFailure of(ErrorCategory category, String code, String message) {
        return new TaskFailure(category, code, message, null);
    }

    public static TaskFailure of(ErrorCategory category, String code, String message, Duration retryAfterHint) {
        return new TaskFailure(category, code, message, retryAfterHint);
    }

    public boolean retryable() {
        return category.retryable();
    }
}
