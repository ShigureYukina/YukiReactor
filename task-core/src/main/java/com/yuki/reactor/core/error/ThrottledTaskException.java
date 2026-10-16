package com.yuki.reactor.core.error;

import java.time.Duration;

/**
 * The downstream dependency asked us to slow down (rate limiting). The optional
 * {@link #retryAfter()} value is honoured as a lower bound of the next retry delay.
 */
public class ThrottledTaskException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    private final Duration retryAfter;

    public ThrottledTaskException(String message) {
        this(message, null, null);
    }

    public ThrottledTaskException(String message, Duration retryAfter) {
        this(message, null, retryAfter);
    }

    public ThrottledTaskException(String message, Throwable cause, Duration retryAfter) {
        super(ErrorCategory.THROTTLED, message, cause);
        this.retryAfter = retryAfter;
    }

    @Override
    public Duration retryAfter() {
        return retryAfter;
    }
}
