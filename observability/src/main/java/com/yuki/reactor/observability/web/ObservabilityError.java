package com.yuki.reactor.observability.web;

/**
 * Error body of the observability endpoints, shaped like {@code ApiError} in the task API so clients
 * see one error contract across the whole service.
 */
public record ObservabilityError(String code, String message) {
}
