package com.yuki.reactor.observability.dto;

import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;

import java.util.List;

/**
 * Everything that explains one task: its current state, each attempt, each recorded decision, and a
 * single chronological event stream built from exactly those rows (PRD 3.1 item 6, 5.6).
 */
public record TaskTimeline(
        TaskView task,
        List<TaskExecutionView> executions,
        List<DecisionView> decisions,
        List<TimelineEvent> events) {
}
