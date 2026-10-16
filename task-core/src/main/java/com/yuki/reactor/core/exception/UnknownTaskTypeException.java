package com.yuki.reactor.core.exception;

/**
 * The submitted task type has no registered {@link com.yuki.reactor.core.handler.TaskHandler}.
 * Reported as HTTP 400: the task is rejected instead of being persisted and never executed.
 */
public class UnknownTaskTypeException extends RuntimeException {

    public UnknownTaskTypeException(String taskType) {
        super("no TaskHandler registered for task type '" + taskType + "'");
    }
}
