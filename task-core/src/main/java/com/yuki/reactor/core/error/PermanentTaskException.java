package com.yuki.reactor.core.error;

/**
 * The business explicitly rejected the request. Never retried automatically.
 */
public class PermanentTaskException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    public PermanentTaskException(String message) {
        this(message, null);
    }

    public PermanentTaskException(String message, Throwable cause) {
        super(ErrorCategory.BUSINESS_REJECTION, message, cause);
    }
}
