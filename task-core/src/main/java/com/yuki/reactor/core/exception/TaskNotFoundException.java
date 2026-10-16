package com.yuki.reactor.core.exception;

/** No task exists for the requested identifier. */
public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(long taskId) {
        super("task " + taskId + " not found");
    }
}
