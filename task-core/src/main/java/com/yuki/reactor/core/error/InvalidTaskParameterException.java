package com.yuki.reactor.core.error;

/**
 * The task payload cannot be handled as given. Never retried automatically.
 */
public class InvalidTaskParameterException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    public InvalidTaskParameterException(String message) {
        this(message, null);
    }

    public InvalidTaskParameterException(String message, Throwable cause) {
        super(ErrorCategory.INVALID_PARAMETER, message, cause);
    }
}
