package com.yuki.reactor.core.domain;

/**
 * Outcome of a single execution attempt, stored on {@code task_execution.result}.
 */
public enum ExecutionResult {

    /** The attempt finished and the task state was updated to SUCCEEDED. */
    SUCCEEDED,

    /** The attempt failed and the task state was updated to FAILED. */
    FAILED,

    /**
     * The attempt finished but its result was rejected because the task state had already moved on
     * (expired lease, superseded fencing token, or a lost claim). Nothing was overwritten.
     */
    ABANDONED
}
