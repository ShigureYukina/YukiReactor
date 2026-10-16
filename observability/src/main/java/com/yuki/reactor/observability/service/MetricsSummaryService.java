package com.yuki.reactor.observability.service;

import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.observability.dto.MetricsSummary;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Aggregates the metrics of PRD 10.2 over a time window, from the stored rows.
 *
 * <p>The window is half open, {@code [from, to)}. When a bound is not given it is filled in rather
 * than left implicit: a missing {@code to} means "up to now", a missing {@code from} means
 * {@value #DEFAULT_WINDOW_MINUTES} minutes before the upper bound. The resolved window is echoed back
 * in the result so a caller can tell which numbers it actually asked for.
 *
 * <p>Peak values (for example the peak backlog) are not derivable from one aggregate, so this
 * service reports point in time readings and the experiment samples them.
 */
@Service
public class MetricsSummaryService {

    static final int DEFAULT_WINDOW_MINUTES = 5;

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final Clock clock;

    public MetricsSummaryService(TaskMapper taskMapper,
                                 TaskExecutionMapper executionMapper,
                                 Clock clock) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.clock = clock;
    }

    public MetricsSummary summary(LocalDateTime from, LocalDateTime to) {
        DecisionQueryService.requireWindow(from, to);

        // "Up to now" must include the current millisecond, hence the one millisecond nudge on the
        // exclusive upper bound.
        LocalDateTime effectiveTo = to != null ? to : LocalDateTime.now(clock).plusNanos(1_000_000);
        LocalDateTime effectiveFrom = from != null
                ? from
                : effectiveTo.minus(Duration.ofMinutes(DEFAULT_WINDOW_MINUTES));

        long submitted = taskMapper.countCreatedBetween(effectiveFrom, effectiveTo);
        long succeeded = taskMapper.countReachedStatusBetween(TaskStatus.SUCCEEDED, effectiveFrom, effectiveTo);
        long failed = taskMapper.countReachedStatusBetween(TaskStatus.FAILED, effectiveFrom, effectiveTo);
        long cancelled = taskMapper.countReachedStatusBetween(TaskStatus.CANCELLED, effectiveFrom, effectiveTo);
        long completed = succeeded + failed;

        double windowSeconds = Duration.between(effectiveFrom, effectiveTo).toMillis() / 1000.0;
        double failureRate = completed == 0 ? 0.0 : (double) failed / completed;
        double throughput = windowSeconds <= 0 ? 0.0 : succeeded / windowSeconds;

        long attempts = executionMapper.countStartedBetween(effectiveFrom, effectiveTo);
        long attemptedTasks = executionMapper.countDistinctTasksBetween(effectiveFrom, effectiveTo);

        return new MetricsSummary(
                effectiveFrom,
                effectiveTo,
                submitted,
                succeeded,
                failed,
                cancelled,
                completed,
                failureRate,
                throughput,
                executionMapper.avgQueueDelayMs(effectiveFrom, effectiveTo),
                executionMapper.avgExecutionDelayMs(effectiveFrom, effectiveTo),
                attempts,
                attemptedTasks == 0 ? 0.0 : (double) attempts / attemptedTasks,
                taskMapper.countByStatus(TaskStatus.RUNNING),
                taskMapper.countBacklog());
    }
}
