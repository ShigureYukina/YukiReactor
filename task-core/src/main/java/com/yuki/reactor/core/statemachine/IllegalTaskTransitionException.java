package com.yuki.reactor.core.statemachine;

import com.yuki.reactor.core.domain.TaskStatus;

/**
 * Thrown when a caller requests a state change that the state machine does not allow (PRD 4.2).
 */
public class IllegalTaskTransitionException extends RuntimeException {

    private final TaskStatus from;
    private final TaskStatus to;

    public IllegalTaskTransitionException(TaskStatus from, TaskStatus to) {
        super("illegal task transition: " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public TaskStatus getFrom() {
        return from;
    }

    public TaskStatus getTo() {
        return to;
    }
}
