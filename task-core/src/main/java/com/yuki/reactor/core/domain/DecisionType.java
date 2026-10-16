package com.yuki.reactor.core.domain;

/** Kind of scheduling decision recorded in {@code scheduler_decision} (PRD 5.6, 7.3). */
public enum DecisionType {

    /** The adaptive controller changed how many tasks this process may run at once (PRD 5.5). */
    CONCURRENCY_ADJUSTMENT
}
