package com.yuki.reactor.core.support;

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
}
