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

    private ErrorCodes() {
    }
}
