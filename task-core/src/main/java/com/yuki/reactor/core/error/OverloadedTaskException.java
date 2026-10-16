package com.yuki.reactor.core.error;

/**
 * The downstream dependency is overloaded. Retried with backoff; from phase 3 this category also
 * feeds the adaptive concurrency controller (PRD 5.5).
 */
public class OverloadedTaskException extends TaskFailureException {

    private static final long serialVersionUID = 1L;

    public OverloadedTaskException(String message) {
        this(message, null);
    }

    public OverloadedTaskException(String message, Throwable cause) {
        super(ErrorCategory.OVERLOADED, message, cause);
    }
}
