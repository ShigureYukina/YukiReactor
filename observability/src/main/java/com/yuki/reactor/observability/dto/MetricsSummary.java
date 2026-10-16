package com.yuki.reactor.observability.dto;

import java.time.LocalDateTime;

/**
 * Metric summary of a time window, computed from the {@code task} and {@code task_execution} rows
 * themselves (PRD 10.2). Nothing here is estimated: every number is an aggregate over stored rows.
 *
 * <p>Definitions are fixed here so two readings of the same window mean the same thing:
 * <ul>
 *   <li>{@code completedTasks} counts tasks that reached a terminal state inside the window
 *       ({@code SUCCEEDED} + {@code FAILED}); {@code CANCELLED} is reported separately and is not
 *       part of the failure rate.</li>
 *   <li>{@code failureRate} = {@code failedTasks} / {@code completedTasks}, or {@code 0} when the
 *       window completed nothing. The denominator is stated so the number cannot be read as
 *       "failures per submission".</li>
 *   <li>{@code throughputPerSecond} = {@code succeededTasks} / window length in seconds.</li>
 *   <li>{@code avgQueueDelayMs} = average of (attempt {@code started_at} - task {@code created_at})
 *       over attempts started in the window.</li>
 *   <li>{@code avgExecutionDelayMs} = average of (attempt {@code finished_at} - {@code started_at})
 *       over attempts finished in the window.</li>
 *   <li>{@code avgAttemptsPerTask} = attempts started in the window / distinct tasks attempted in
 *       the window; it is the "logical task to attempt" ratio of PRD 10.2.</li>
 *   <li>{@code backlogNow} and {@code runningNow} are point in time readings at query time. A peak
 *       backlog cannot be derived from one reading, so callers that need it sample this endpoint.</li>
 * </ul>
 */
public record MetricsSummary(
        LocalDateTime from,
        LocalDateTime to,
        long submittedTasks,
        long succeededTasks,
        long failedTasks,
        long cancelledTasks,
        long completedTasks,
        double failureRate,
        double throughputPerSecond,
        Double avgQueueDelayMs,
        Double avgExecutionDelayMs,
        long executionAttempts,
        double avgAttemptsPerTask,
        long runningNow,
        long backlogNow) {
}
