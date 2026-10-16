package com.yuki.reactor.observability.domain;

/**
 * Scheduling strategy an experiment round ran under (PRD 10.1).
 *
 * <p>The names are stored as written, so a reader of the table sees which of the two strategies the
 * round belongs to without consulting code.
 */
public enum ExperimentStrategy {

    /** Fixed concurrency: the configured limit is used as is. */
    FIXED,

    /** Adaptive concurrency: the controller raises and lowers the limit from runtime signals. */
    ADAPTIVE
}
