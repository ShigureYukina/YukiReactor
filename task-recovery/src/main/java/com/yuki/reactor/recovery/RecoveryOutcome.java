package com.yuki.reactor.recovery;

/**
 * Outcome of trying to reclaim a single task whose lease expired.
 */
public enum RecoveryOutcome {

    /** The task was moved back into the dispatch flow ({@code RUNNING -> RETRY_WAIT}). */
    RECLAIMED_FOR_RETRY,

    /** The lease expired, but no attempt was left, so the task reached its terminal state. */
    RECLAIMED_AS_FAILED,

    /** Nothing was done: the lease was renewed meanwhile, or the attempt already reported a result. */
    SKIPPED
}
