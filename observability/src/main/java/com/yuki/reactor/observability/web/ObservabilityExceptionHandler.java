package com.yuki.reactor.observability.web;

import com.yuki.reactor.observability.exception.DuplicateExperimentRoundException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Turns observability specific failures into the shared error shape. Everything else is already
 * handled by the task API advice, which applies to every controller in the application.
 *
 * <p>This advice must run first: the task API advice ends with a catch-all {@code Exception} handler,
 * so without an explicit precedence an exception raised here would be reported as an internal error
 * instead of the conflict it really is.
 */
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ObservabilityExceptionHandler {

    @ExceptionHandler(DuplicateExperimentRoundException.class)
    public ResponseEntity<ObservabilityError> handleDuplicateRound(DuplicateExperimentRoundException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ObservabilityError("DUPLICATE_EXPERIMENT_ROUND", e.getMessage()));
    }
}
