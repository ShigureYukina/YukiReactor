package com.yuki.reactor.core.error;

import java.time.Duration;

/**
 * Base type for handler failures that carry an explicit {@link ErrorCategory}.
 *
 * <p>Handlers throw one of the concrete subclasses to tell the engine whether the failure is
 * worth retrying. Anything else a handler throws is classified as {@link ErrorCategory#UNKNOWN}
 * and retried until {@code max_attempts} is reached (PRD 5.4).
 */
public abstract class TaskFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final ErrorCategory category;

    protected TaskFailureException(ErrorCategory category, String message, Throwable cause) {
        super(message, cause);
        this.category = category;
    }

    public ErrorCategory category() {
        return category;
    }

    /**
     * Minimum delay the failing dependency asked for before the next attempt, if any.
     */
    public Duration retryAfter() {
        return null;
    }
}
