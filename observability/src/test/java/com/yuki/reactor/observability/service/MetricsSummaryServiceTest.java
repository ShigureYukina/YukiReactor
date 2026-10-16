package com.yuki.reactor.observability.service;

import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.observability.dto.MetricsSummary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * The metric definitions of PRD 10.2 are an agreement about what a number means, so they are pinned
 * here: which states count as completed, what the failure rate divides by, and what a missing window
 * bound resolves to.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MetricsSummaryServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-16T10:00:00Z");

    @Mock
    private TaskMapper taskMapper;

    @Mock
    private TaskExecutionMapper executionMapper;

    private MetricsSummaryService service() {
        return new MetricsSummaryService(taskMapper, executionMapper, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private void stubWindowCounts() {
        when(taskMapper.countCreatedBetween(any(), any())).thenReturn(10L);
        when(taskMapper.countReachedStatusBetween(eq(TaskStatus.SUCCEEDED), any(), any())).thenReturn(8L);
        when(taskMapper.countReachedStatusBetween(eq(TaskStatus.FAILED), any(), any())).thenReturn(2L);
        when(taskMapper.countReachedStatusBetween(eq(TaskStatus.CANCELLED), any(), any())).thenReturn(1L);
        when(taskMapper.countByStatus(eq(TaskStatus.RUNNING))).thenReturn(1L);
        when(taskMapper.countBacklog()).thenReturn(4L);
        when(executionMapper.countStartedBetween(any(), any())).thenReturn(12L);
        when(executionMapper.countDistinctTasksBetween(any(), any())).thenReturn(9L);
        when(executionMapper.avgQueueDelayMs(any(), any())).thenReturn(1200.0);
        when(executionMapper.avgExecutionDelayMs(any(), any())).thenReturn(800.0);
    }

    @Test
    void resolvesAMissingWindowToTheDefaultLookbackEndingNow() {
        stubWindowCounts();

        MetricsSummary summary = service().summary(null, null);

        assertThat(summary.to()).isEqualTo(LocalDateTime.of(2026, 10, 16, 10, 0, 0, 1_000_000));
        assertThat(summary.from()).isEqualTo(LocalDateTime.of(2026, 10, 16, 9, 55, 0, 1_000_000));
    }

    @Test
    void keepsTheWindowTheCallerAskedFor() {
        stubWindowCounts();
        LocalDateTime from = LocalDateTime.of(2026, 10, 16, 9, 0);
        LocalDateTime to = LocalDateTime.of(2026, 10, 16, 9, 30);

        MetricsSummary summary = service().summary(from, to);

        assertThat(summary.from()).isEqualTo(from);
        assertThat(summary.to()).isEqualTo(to);
    }

    @Test
    void countsSucceededAndFailedAsCompletedAndLeavesCancelledOutOfTheFailureRate() {
        stubWindowCounts();

        MetricsSummary summary = service().summary(null, null);

        assertThat(summary.submittedTasks()).isEqualTo(10L);
        assertThat(summary.succeededTasks()).isEqualTo(8L);
        assertThat(summary.failedTasks()).isEqualTo(2L);
        assertThat(summary.cancelledTasks()).isEqualTo(1L);
        assertThat(summary.completedTasks()).isEqualTo(10L);
        assertThat(summary.failureRate()).isCloseTo(0.2, within(1e-9));
        assertThat(summary.throughputPerSecond()).as("8 successes over the 300 s window").isCloseTo(8.0 / 300, within(1e-9));
        assertThat(summary.avgQueueDelayMs()).isEqualTo(1200.0);
        assertThat(summary.avgExecutionDelayMs()).isEqualTo(800.0);
        assertThat(summary.executionAttempts()).isEqualTo(12L);
        assertThat(summary.avgAttemptsPerTask()).isCloseTo(12.0 / 9, within(1e-9));
        assertThat(summary.runningNow()).isEqualTo(1L);
        assertThat(summary.backlogNow()).isEqualTo(4L);
    }

    @Test
    void reportsNoFailureRateWhenNothingCompleted() {
        when(taskMapper.countCreatedBetween(any(), any())).thenReturn(2L);
        when(executionMapper.countDistinctTasksBetween(any(), any())).thenReturn(0L);
        when(executionMapper.countStartedBetween(any(), any())).thenReturn(0L);
        when(executionMapper.avgQueueDelayMs(any(), any())).thenReturn(null);
        when(executionMapper.avgExecutionDelayMs(any(), any())).thenReturn(null);

        MetricsSummary summary = service().summary(null, null);

        assertThat(summary.completedTasks()).isZero();
        assertThat(summary.failureRate()).isZero();
        assertThat(summary.avgAttemptsPerTask()).as("no attempt means no ratio to report").isZero();
        assertThat(summary.avgQueueDelayMs()).as("a missing average stays unknown").isNull();
    }

    @Test
    void rejectsAnInvertedWindow() {
        LocalDateTime from = LocalDateTime.of(2026, 10, 16, 10, 0);

        assertThatThrownBy(() -> service().summary(from, from.minusMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from must be before to");
    }
}
