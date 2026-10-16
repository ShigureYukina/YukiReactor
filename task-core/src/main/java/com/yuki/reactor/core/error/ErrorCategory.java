package com.yuki.reactor.core.error;

/**
 * Classification of a failed attempt (PRD 5.4).
 *
 * <p>The classification decides whether the scheduler retries the task or stops. It is stored on
 * every failed execution record so an operator can see <em>why</em> a task was retried or given up,
 * instead of only seeing the raw exception.
 */
public enum ErrorCategory {

    /** The parameters do not describe an executable task. Retrying cannot help. */
    INVALID_PARAMETER(false),

    /** The business explicitly rejected the request. Retrying cannot help. */
    BUSINESS_REJECTION(false),

    /** A temporary error: connection reset, unavailable downstream, unreadable response. */
    TRANSIENT(true),

    /** The downstream asked us to slow down; the retry respects the requested delay. */
    THROTTLED(true),

    /** The downstream is overloaded; the retry is delayed to give it room to recover. */
    OVERLOADED(true),

    /** The attempt exceeded {@code task.timeout_ms} and was interrupted. */
    TIMEOUT(true),

    /** The attempt lost its lease: the executor crashed or stalled and recovery took over. */
    EXECUTOR_LOST(true),

    /** An exception the task did not classify. Retried, bounded by {@code max_attempts}. */
    UNKNOWN(true);

    private final boolean retryable;

    ErrorCategory(boolean retryable) {
        this.retryable = retryable;
    }

    /** Whether an attempt failing with this category may be scheduled again. */
    public boolean retryable() {
        return retryable;
    }
}
