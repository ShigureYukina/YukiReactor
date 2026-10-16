package com.yuki.reactor.adaptive;

import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.signals.RuntimeSignals;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests of the AIMD control law: increase, decrease and every safety rail of PRD 5.5.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AimdControllerTest {

    @Mock
    private TaskMapper taskMapper;

    @Mock
    private ConcurrencyDecisionRecorder recorder;

    private final MutableClock clock = new MutableClock();
    private final RuntimeSignals signals = new RuntimeSignals();
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @BeforeEach
    void stubCounters() throws Exception {
        when(taskMapper.countRunning()).thenReturn(2L);
        when(taskMapper.countDueBacklog(any(LocalDateTime.class))).thenReturn(3L);
    }

    @Test
    void increasesOneStepAtATimeUntilTheMaximum() {
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 2);
        AimdController controller = controller(properties, limit);
        recordHealthySignals();

        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(2);

        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(3);

        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(4);

        controller.sampleOnce();
        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(4);

        ArgumentCaptor<Integer> before = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> after = ArgumentCaptor.forClass(Integer.class);
        verify(recorder, times(2)).record(eq(DecisionType.CONCURRENCY_ADJUSTMENT), eq(ReasonCodes.RECOVERY_OBSERVED),
                any(), before.capture(), after.capture(), any(), any(), any(), any(), any());
        assertThat(before.getAllValues()).containsExactly(2, 3);
        assertThat(after.getAllValues()).containsExactly(3, 4);
    }

    @Test
    void decreasesMultiplicativelyAfterConsecutiveBreaches() {
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 8, 8);
        AimdController controller = controller(properties, limit);
        recordOverloadSignals();

        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(8);

        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(4);

        controller.sampleOnce();
        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(2);

        verify(recorder, times(2)).record(eq(DecisionType.CONCURRENCY_ADJUSTMENT), eq(ReasonCodes.DOWNSTREAM_OVERLOAD),
                any(), anyInt(), anyInt(), any(), any(), any(), any(), any());
    }

    @Test
    void capsTheDecreasePerAdjustment() {
        AdaptiveProperties properties = properties();
        properties.setMaxDecreaseStep(4);
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 16, 16);
        AimdController controller = controller(properties, limit);
        recordOverloadSignals();

        controller.sampleOnce();
        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(12);
    }

    @Test
    void neverDecreasesBelowTheMinimum() {
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(2, 8, 2);
        AimdController controller = controller(properties, limit);
        recordOverloadSignals();

        controller.sampleOnce();
        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(2);
        verifyNoInteractions(recorder);
    }

    @Test
    void respectsTheCooldownBetweenAdjustments() {
        AdaptiveProperties properties = properties();
        properties.setCooldown(Duration.ofHours(1));
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 16, 16);
        AimdController controller = controller(properties, limit);
        recordOverloadSignals();

        controller.sampleOnce();
        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(12);

        for (int i = 0; i < 6; i++) {
            controller.sampleOnce();
        }
        assertThat(limit.current()).isEqualTo(12);

        clock.advance(Duration.ofHours(1));
        controller.sampleOnce();
        controller.sampleOnce();
        assertThat(limit.current()).isEqualTo(8);
    }

    @Test
    void treatsTimeoutsAsOverload() {
        AdaptiveProperties properties = properties();
        properties.setTimeoutRateThreshold(0.2);
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 4);
        AimdController controller = controller(properties, limit);
        signals.recordAttempt(100, 0, ErrorCategory.TIMEOUT, true);
        signals.recordAttempt(100, 0, ErrorCategory.TIMEOUT, true);
        signals.recordAttempt(100, 0, null, false);

        controller.sampleOnce();
        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(2);
        verify(recorder).record(eq(DecisionType.CONCURRENCY_ADJUSTMENT), eq(ReasonCodes.DOWNSTREAM_OVERLOAD),
                any(), eq(4), eq(2), any(), any(), any(), any(), any());
    }

    @Test
    void fallsBackToTheConservativeConcurrencyWhenNoSamplesAreAvailable() {
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 4);
        AimdController controller = controller(properties, limit);

        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(1);
        verify(recorder).record(eq(DecisionType.CONCURRENCY_ADJUSTMENT), eq(ReasonCodes.METRICS_UNAVAILABLE), any(),
                eq(4), eq(1), eq(2L), eq(3L), eq(null), eq(null), any());
    }

    @Test
    void fallsBackToTheConservativeConcurrencyWhenCountersCannotBeRead() throws Exception {
        when(taskMapper.countRunning()).thenThrow(new IllegalStateException("database is gone"));
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 4);
        AimdController controller = controller(properties, limit);
        recordHealthySignals();

        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(1);
        ArgumentCaptor<String> details = ArgumentCaptor.forClass(String.class);
        verify(recorder).record(eq(DecisionType.CONCURRENCY_ADJUSTMENT), eq(ReasonCodes.METRICS_UNAVAILABLE), any(),
                eq(4), eq(1), eq(null), eq(null), eq(null), eq(null), details.capture());
        assertThat(details.getValue()).contains("database is gone");
    }

    @Test
    void doesNothingWhileIdleWithoutSamples() throws Exception {
        when(taskMapper.countRunning()).thenReturn(0L);
        when(taskMapper.countDueBacklog(any(LocalDateTime.class))).thenReturn(0L);
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(1, 4, 4);
        AimdController controller = controller(properties, limit);

        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(4);
        verifyNoInteractions(recorder);
    }

    @Test
    void doesNotTouchTheLimitWhenTheIncreaseIsAlreadyAtTheMaximum() {
        AdaptiveProperties properties = properties();
        ConcurrencyLimit limit = new ConcurrencyLimit(4, 4, 4);
        AimdController controller = controller(properties, limit);
        recordHealthySignals();

        controller.sampleOnce();
        controller.sampleOnce();
        controller.sampleOnce();

        assertThat(limit.current()).isEqualTo(4);
        verify(recorder, never()).record(any(), any(), any(), anyInt(), anyInt(), any(), any(), any(), any(), any());
    }

    @Test
    void rejectsInvalidPropertyBounds() {
        AdaptiveProperties properties = properties();
        properties.setMaxConcurrency(2);
        properties.setInitialConcurrency(4);

        assertThatThrownBy(properties::validate).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initial-concurrency");
    }

    private AimdController controller(AdaptiveProperties properties, ConcurrencyLimit limit) {
        return new AimdController(properties, limit, signals, taskMapper, recorder, meterRegistry, clock,
                "test-controller");
    }

    private AdaptiveProperties properties() {
        AdaptiveProperties properties = new AdaptiveProperties();
        properties.setEnabled(true);
        properties.setMinConcurrency(1);
        properties.setMaxConcurrency(16);
        properties.setInitialConcurrency(16);
        properties.setSampleInterval(Duration.ofMillis(100));
        properties.setLatencyThreshold(Duration.ofMillis(500));
        properties.setErrorRateThreshold(0.5);
        properties.setTimeoutRateThreshold(0.5);
        properties.setIncreaseStep(1);
        properties.setDecreaseFactor(0.5);
        properties.setMaxDecreaseStep(4);
        properties.setCooldown(Duration.ZERO);
        properties.setConsecutiveBreachesBeforeDecrease(2);
        properties.setRecoveryObservation(Duration.ZERO);
        properties.setConservativeConcurrency(1);
        signals.reset();
        return properties;
    }

    private void recordHealthySignals() {
        signals.recordDownstreamCall(50, false);
        signals.recordDownstreamCall(60, false);
        signals.recordAttempt(50, 0, null, false);
        signals.recordAttempt(60, 0, null, false);
    }

    private void recordOverloadSignals() {
        signals.recordDownstreamCall(900, true);
        signals.recordDownstreamCall(900, false);
        signals.recordAttempt(900, 0, ErrorCategory.TRANSIENT, false);
        signals.recordAttempt(900, 0, ErrorCategory.TRANSIENT, false);
    }

    private static final class MutableClock extends Clock {

        private Instant instant = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
