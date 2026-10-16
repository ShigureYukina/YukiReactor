package com.yuki.reactor.core.domain;

/**
 * Centralised error and reason codes.
 *
 * <p>Codes are referenced by execution records, administrative notes and later by scheduler
 * decisions, so they are defined once instead of being scattered as string literals
 * (see AGENTS.md 11).
 */
public final class ErrorCodes {

    /** A task handler threw an exception while executing the task. */
    public static final String EXECUTION_ERROR = "EXECUTION_ERROR";

    /** A result arrived from an attempt that no longer owns the task. */
    public static final String STALE_WRITER = "STALE_WRITER";

    /** An operator cancelled the task or wrote an administrative note. */
    public static final String CANCELLED_BY_OPERATOR = "CANCELLED_BY_OPERATOR";

    /** Administrative replay of a failed task. */
    public static final String REPLAYED_BY_OPERATOR = "REPLAYED_BY_OPERATOR";

    /** The attempt was stopped because it exceeded {@code task.timeout_ms}. */
    public static final String EXECUTION_TIMEOUT = "EXECUTION_TIMEOUT";

    /** The lease of a running attempt expired and the task was taken back by recovery. */
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";

    /** A running attempt could not extend its lease and therefore lost ownership. */
    public static final String LEASE_RENEWAL_FAILED = "LEASE_RENEWAL_FAILED";

    /** A retryable failure could not be retried because {@code max_attempts} was reached. */
    public static final String MAX_RETRIES_EXCEEDED = "MAX_RETRIES_EXCEEDED";

    /** An execution record was still open although the task had already moved on. */
    public static final String ORPHANED_EXECUTION = "ORPHANED_EXECUTION";

    private ErrorCodes() {
    }
}
