package com.yuki.reactor.core.error;

/**
 * The attempt ran longer than the task timeout. Raised by the engine after it interrupted a
 * cooperative handler, never by handlers themselves.
 */
public class TaskTimeoutException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    public TaskTimeoutException(String message) {
        super(ErrorCategory.TIMEOUT, message, null);
    }
}
