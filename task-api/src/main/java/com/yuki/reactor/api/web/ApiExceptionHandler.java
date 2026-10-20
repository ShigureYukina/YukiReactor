package com.yuki.reactor.api.web;

import com.yuki.reactor.common.web.ApiError;
import com.yuki.reactor.core.exception.InvalidTaskStateException;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.exception.UnknownTaskTypeException;
import com.yuki.reactor.core.statemachine.IllegalTaskTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Turns domain failures into one consistent error shape (AGENTS.md 11):
 * 400 for bad input, 404 for unknown tasks, 409 for state conflicts, 500 for everything else.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(TaskNotFoundException e) {
        return error(HttpStatus.NOT_FOUND, "TASK_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(UnknownTaskTypeException.class)
    public ResponseEntity<ApiError> handleUnknownTaskType(UnknownTaskTypeException e) {
        return error(HttpStatus.BAD_REQUEST, "UNKNOWN_TASK_TYPE", e.getMessage());
    }

    @ExceptionHandler(IllegalTaskTransitionException.class)
    public ResponseEntity<ApiError> handleIllegalTransition(IllegalTaskTransitionException e) {
        return error(HttpStatus.CONFLICT, "ILLEGAL_TASK_TRANSITION", e.getMessage());
    }

    @ExceptionHandler(InvalidTaskStateException.class)
    public ResponseEntity<ApiError> handleInvalidState(InvalidTaskStateException e) {
        return error(HttpStatus.CONFLICT, "INVALID_TASK_STATE", e.getMessage());
    }

    @ExceptionHandler(DuplicateKeyException.class)
    public ResponseEntity<ApiError> handleDuplicateKey(DuplicateKeyException e) {
        return error(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT",
                "a conflicting task already exists for this idempotency key");
    }

    @ExceptionHandler({
            IllegalArgumentException.class,
            MethodArgumentTypeMismatchException.class,
            HttpMessageNotReadableException.class,
            MissingServletRequestParameterException.class
    })
    public ResponseEntity<ApiError> handleBadRequest(Exception e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception e) {
        log.error("unhandled error while serving request", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "unexpected server error");
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(new ApiError(code, message));
    }
}
