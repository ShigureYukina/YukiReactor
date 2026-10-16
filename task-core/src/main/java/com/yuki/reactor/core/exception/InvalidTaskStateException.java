package com.yuki.reactor.core.exception;

/**
 * The requested operation does not apply to the current task state, or the state moved between the
 * validation read and the conditional update. Reported as HTTP 409.
 */
public class InvalidTaskStateException extends RuntimeException {

    public InvalidTaskStateException(String message) {
        super(message);
    }
}
