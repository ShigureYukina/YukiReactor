package com.yuki.reactor.core.error;

import com.yuki.reactor.core.domain.ErrorCodes;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Classification of handler failures into the error categories of PRD 5.4. The category decides
 * whether an attempt is retried and is stored on every failed execution record.
 */
class TaskFailureTest {

    @Test
    void classifiesRetryableHandlerFailures() {
        assertThat(TaskFailure.classify(new TransientTaskException("connection reset")))
                .satisfies(failure -> {
                    assertThat(failure.category()).isEqualTo(ErrorCategory.TRANSIENT);
                    assertThat(failure.code()).isEqualTo(ErrorCodes.EXECUTION_ERROR);
                    assertThat(failure.message()).contains("TransientTaskException: connection reset");
                    assertThat(failure.retryAfterHint()).isNull();
                    assertThat(failure.retryable()).isTrue();
                });

        assertThat(TaskFailure.classify(new OverloadedTaskException("downstream is saturated")).category())
                .isEqualTo(ErrorCategory.OVERLOADED);
    }

    @Test
    void keepsTheRetryAfterInstructionOfAThrottledDependency() {
        TaskFailure failure = TaskFailure.classify(new ThrottledTaskException("slow down", Duration.ofSeconds(30)));

        assertThat(failure.category()).isEqualTo(ErrorCategory.THROTTLED);
        assertThat(failure.retryAfterHint()).isEqualTo(Duration.ofSeconds(30));
        assertThat(failure.retryable()).isTrue();
    }

    @Test
    void classifiesBusinessRejectionsAsTerminal() {
        assertThat(TaskFailure.classify(new PermanentTaskException("order already cancelled")))
                .satisfies(failure -> {
                    assertThat(failure.category()).isEqualTo(ErrorCategory.BUSINESS_REJECTION);
                    assertThat(failure.retryable()).isFalse();
                });

        assertThat(TaskFailure.classify(new InvalidTaskParameterException("amount must be positive")))
                .satisfies(failure -> {
                    assertThat(failure.category()).isEqualTo(ErrorCategory.INVALID_PARAMETER);
                    assertThat(failure.retryable()).isFalse();
                });
    }

    @Test
    void treatsUnclassifiedFailuresAsRetryableButBounded() {
        TaskFailure failure = TaskFailure.classify(new IllegalStateException("boom"));

        assertThat(failure.category()).isEqualTo(ErrorCategory.UNKNOWN);
        assertThat(failure.code()).isEqualTo(ErrorCodes.EXECUTION_ERROR);
        assertThat(failure.message()).contains("IllegalStateException: boom");
        assertThat(failure.retryable()).isTrue();
    }

    @Test
    void describesFailuresWithoutAMessage() {
        TaskFailure failure = TaskFailure.classify(new IllegalStateException());

        assertThat(failure.message())
                .as("without a message the fully qualified class name is the most specific fact left")
                .isEqualTo(IllegalStateException.class.getName());
    }

    @Test
    void buildsTimeoutFailures() {
        TaskFailure failure = TaskFailure.timeout(1500);

        assertThat(failure.category()).isEqualTo(ErrorCategory.TIMEOUT);
        assertThat(failure.code()).isEqualTo(ErrorCodes.EXECUTION_TIMEOUT);
        assertThat(failure.message()).contains("1500");
        assertThat(failure.retryable()).isTrue();
    }

    @Test
    void fallsBackToTheCategoryNameWhenTheMessageIsBlank() {
        TaskFailure failure = TaskFailure.of(ErrorCategory.TRANSIENT, "  ", "  ");

        assertThat(failure.code()).isEqualTo(ErrorCodes.EXECUTION_ERROR);
        assertThat(failure.message()).isEqualTo("TRANSIENT");
    }
}
