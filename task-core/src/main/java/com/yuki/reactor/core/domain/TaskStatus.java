package com.yuki.reactor.core.domain;

/**
 * Task lifecycle states (PRD 4.2).
 *
 * <p>Legal transitions are defined exclusively in {@link com.yuki.reactor.core.statemachine.TaskStateMachine}.
 */
public enum TaskStatus {

    /** Waiting to be scheduled. */
    PENDING,

    /** Claimed by a worker and currently executing. */
    RUNNING,

    /** Failed with a retriable error, waiting for the next attempt. */
    RETRY_WAIT,

    /** Execution completed successfully. */
    SUCCEEDED,

    /** Execution failed and will not be retried automatically. */
    FAILED,

    /** Cancelled before a new execution could start. */
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
