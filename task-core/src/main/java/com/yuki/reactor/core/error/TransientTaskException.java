package com.yuki.reactor.core.error;

/**
 * A failure that is expected to disappear on its own, such as a temporary network error.
 * Retried according to the configured backoff policy.
 */
public class TransientTaskException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    public TransientTaskException(String message) {
        this(message, null);
    }

    public TransientTaskException(String message, Throwable cause) {
        super(ErrorCategory.TRANSIENT, message, cause);
    }
}
