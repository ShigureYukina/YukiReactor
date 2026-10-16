package com.yuki.reactor.core.signals;

import com.yuki.reactor.core.error.ErrorCategory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The signal window feeds the adaptive controller, so the aggregation rules matter: only errors
 * that indicate downstream or execution pressure may count towards the error rate; a business
 * rejection must not look like an overloaded downstream (PRD 5.5).
 */
class RuntimeSignalsTest {

    @Test
    void emptyWindowIsReportedAsNotMeasured() {
        RuntimeSignals signals = new RuntimeSignals();

        SignalSnapshot snapshot = signals.snapshot();

        assertThat(snapshot.measured()).isFalse();
        assertThat(snapshot.attempts()).isZero();
        assertThat(snapshot.downstreamCalls()).isZero();
    }

    @Test
    void aggregatesAttemptLatencyQueueWaitAndTimeoutRate() {
        RuntimeSignals signals = new RuntimeSignals();

        signals.recordAttempt(100, 10, null, false);
        signals.recordAttempt(300, 30, ErrorCategory.TRANSIENT, false);
        signals.recordAttempt(200, 20, ErrorCategory.TIMEOUT, true);
        signals.recordAttempt(400, 40, ErrorCategory.BUSINESS_REJECTION, false);

        SignalSnapshot snapshot = signals.snapshot();

        assertThat(snapshot.measured()).isTrue();
        assertThat(snapshot.attempts()).isEqualTo(4);
        assertThat(snapshot.averageAttemptLatencyMillis()).isEqualTo(250.0);
        assertThat(snapshot.averageQueueWaitMillis()).isEqualTo(25.0);
        // TRANSIENT and TIMEOUT are downstream/execution pressure; the business rejection is not.
        assertThat(snapshot.attemptErrorRate()).isEqualTo(2 / 4.0);
        assertThat(snapshot.timeoutRate()).isEqualTo(0.25);
    }

    @Test
    void businessRejectionsAndParameterErrorsNeverLookLikeDownstreamPressure() {
        RuntimeSignals signals = new RuntimeSignals();

        signals.recordAttempt(50, 0, ErrorCategory.BUSINESS_REJECTION, false);
        signals.recordAttempt(50, 0, ErrorCategory.INVALID_PARAMETER, false);
        signals.recordAttempt(50, 0, ErrorCategory.UNKNOWN, false);

        SignalSnapshot snapshot = signals.snapshot();

        assertThat(snapshot.attemptErrorRate()).isZero();
        assertThat(snapshot.timeoutRate()).isZero();
    }

    @Test
    void aggregatesDownstreamLatencyAndErrorRate() {
        RuntimeSignals signals = new RuntimeSignals();

        signals.recordDownstreamCall(800, false);
        signals.recordDownstreamCall(1200, true);
        signals.recordDownstreamCall(1000, false);

        SignalSnapshot snapshot = signals.snapshot();

        assertThat(snapshot.downstreamCalls()).isEqualTo(3);
        assertThat(snapshot.averageDownstreamLatencyMillis()).isEqualTo(1000.0);
        assertThat(snapshot.downstreamErrorRate()).isEqualTo(1 / 3.0);
        // An attempt-free window still counts as measured: the controller saw something.
        assertThat(snapshot.measured()).isTrue();
        assertThat(snapshot.attempts()).isZero();
    }

    @Test
    void keepsOnlyTheMostRecentSamples() {
        RuntimeSignals signals = new RuntimeSignals(3);

        assertThat(signals.capacity()).isEqualTo(3);
        signals.recordDownstreamCall(100, false);
        signals.recordDownstreamCall(100, true);
        signals.recordDownstreamCall(100, true);
        signals.recordDownstreamCall(500, false);

        SignalSnapshot snapshot = signals.snapshot();

        assertThat(snapshot.downstreamCalls()).isEqualTo(3);
        assertThat(snapshot.averageDownstreamLatencyMillis()).isEqualTo(700.0 / 3.0);
        assertThat(snapshot.downstreamErrorRate()).isEqualTo(2 / 3.0);
    }

    @Test
    void resetDropsEverySample() {
        RuntimeSignals signals = new RuntimeSignals();
        signals.recordAttempt(100, 5, ErrorCategory.TRANSIENT, false);
        signals.recordDownstreamCall(100, true);

        signals.reset();

        assertThat(signals.snapshot().measured()).isFalse();
    }

    @Test
    void rejectsACapacityBelowOne() {
        try {
            new RuntimeSignals(0);
            org.assertj.core.api.Assertions.fail("expected an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertThat(expected).hasMessageContaining("capacity");
        }
    }
}
