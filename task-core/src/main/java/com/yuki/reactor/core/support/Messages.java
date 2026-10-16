package com.yuki.reactor.core.support;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.error.ErrorCategory;

/**
 * Small text helpers shared by the task services.
 */
public final class Messages {

    /** Longest message that fits into the {@code last_error} / {@code error_message} columns. */
    public static final int MAX_ERROR_LENGTH = 500;

    private Messages() {
    }

    /** Shortens a message so it always fits into the persisted column. */
    public static String truncate(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    /**
     * Administrative note recorded on the task when an operator cancels or replays it, so the action
     * stays auditable after the fact.
     */
    public static String operatorNote(String action, String operator, String reason) {
        return action + " by " + operator + ": " + reason;
    }

    /** Human readable description of a handler failure, used as the failed-attempt message. */
    public static String describe(Throwable throwable) {
        if (throwable == null) {
            return "unknown error";
        }
        String message = throwable.getMessage();
        if (message == null || message.isBlank()) {
            return throwable.getClass().getName();
        }
        return throwable.getClass().getSimpleName() + ": " + message;
    }

    /**
     * Failure note persisted on {@code task.last_error}, prefixed with the error category so the
     * classification of the last failure stays visible on the task itself.
     */
    public static String describe(ErrorCategory category, String message) {
        String detail = (message == null || message.isBlank()) ? category.name() : message;
        return truncate(category.name() + ": " + detail, MAX_ERROR_LENGTH);
    }

    /**
     * Failure note persisted on {@code task.last_error} when the attempt carries its own reason code.
     *
     * <p>The prefix is the most specific fact available: a concrete reason code
     * ({@code LEASE_EXPIRED}, {@code EXECUTION_TIMEOUT}, {@code MAX_RETRIES_EXCEEDED}, ...) when the
     * failure has one, otherwise the error category ({@code TRANSIENT}, {@code UNKNOWN}, ...).
     * {@code EXECUTION_ERROR} is the generic code for "the handler threw", so the category is more
     * informative there.
     */
    public static String describe(String code, ErrorCategory category, String message) {
        String detail = (message == null || message.isBlank()) ? category.name() : message;
        String prefix = (code == null || code.isBlank() || ErrorCodes.EXECUTION_ERROR.equals(code))
                ? category.name()
                : code;
        return truncate(prefix + ": " + detail, MAX_ERROR_LENGTH);
    }
}
