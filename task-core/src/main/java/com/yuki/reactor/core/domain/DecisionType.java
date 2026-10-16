package com.yuki.reactor.core.domain;

/** Kind of scheduling decision recorded in {@code scheduler_decision} (PRD 5.6, 7.3). */
public enum DecisionType {

    /** The adaptive controller changed how many tasks this process may run at once (PRD 5.5). */
    CONCURRENCY_ADJUSTMENT,

    /**
     * A due task was left unclaimed this round because a scheduling rule did not admit it yet.
     * The reason code says which rule: {@link ReasonCodes#CAPACITY_LIMIT} or
     * {@link ReasonCodes#PRIORITY_WAIT}.
     */
    TASK_DEFERRAL,

    /** A failed attempt was put back into the dispatch flow after a backoff delay (PRD 5.4). */
    RETRY_SCHEDULED,

    /** A retryable failure used the last attempt, so the task reached its terminal state (PRD 5.4). */
    RETRY_EXHAUSTED,

    /** Recovery took a task back from an executor that stopped renewing its lease (PRD 5.3). */
    LEASE_RECLAIMED
}
