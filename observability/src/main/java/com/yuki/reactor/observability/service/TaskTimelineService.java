package com.yuki.reactor.observability.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.observability.dto.DecisionView;
import com.yuki.reactor.observability.dto.TaskTimeline;
import com.yuki.reactor.observability.dto.TimelineEvent;
import com.yuki.reactor.observability.dto.TimelineEventType;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Builds the explanation of a single task (PRD 3.1 item 6, 5.6).
 *
 * <p>The timeline is assembled from exactly three sources - the task row, its execution attempts and
 * its recorded decisions - and sorted by the time each of them happened. An attempt that never
 * reported stays visible as an open start, a decision keeps the reason code that was written when it
 * was taken, and no event is produced by looking at the current status.
 */
@Service
public class TaskTimelineService {

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final SchedulerDecisionMapper decisionMapper;
    private final ObjectMapper objectMapper;

    public TaskTimelineService(TaskMapper taskMapper,
                               TaskExecutionMapper executionMapper,
                               SchedulerDecisionMapper decisionMapper,
                               ObjectMapper objectMapper) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.decisionMapper = decisionMapper;
        this.objectMapper = objectMapper;
    }

    public TaskTimeline timeline(long taskId) {
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            throw new TaskNotFoundException(taskId);
        }
        List<TaskExecution> executions = executionMapper.findByTaskId(taskId);
        List<SchedulerDecision> decisions = decisionMapper.findByTaskId(taskId);

        List<TimelineEvent> events = new ArrayList<>();
        events.add(new TimelineEvent(
                task.getCreatedAt(),
                TimelineEventType.TASK_CREATED,
                null,
                null,
                null,
                "taskType=" + task.getTaskType() + " priority=" + task.getPriority()
                        + " maxAttempts=" + task.getMaxAttempts() + " timeoutMs=" + task.getTimeoutMs()));

        for (TaskExecution execution : executions) {
            events.add(new TimelineEvent(
                    execution.getStartedAt(),
                    TimelineEventType.ATTEMPT_STARTED,
                    execution.getId(),
                    null,
                    null,
                    "attempt " + execution.getAttempt() + " claimed by " + execution.getWorkerId()
                            + " with fencingToken " + execution.getFencingToken()));
            if (execution.getFinishedAt() != null) {
                events.add(new TimelineEvent(
                        execution.getFinishedAt(),
                        TimelineEventType.ATTEMPT_FINISHED,
                        execution.getId(),
                        null,
                        null,
                        finishDetails(execution)));
            }
        }

        for (SchedulerDecision decision : decisions) {
            events.add(new TimelineEvent(
                    decision.getCreatedAt(),
                    TimelineEventType.DECISION,
                    decision.getExecutionId(),
                    decision.getDecisionType(),
                    decision.getReasonCode(),
                    decision.getObservedDetails()));
        }

        events.sort(Comparator
                .comparing(TimelineEvent::at)
                .thenComparing(event -> event.type().ordinal())
                .thenComparing(event -> event.executionId() == null ? Long.MAX_VALUE : event.executionId()));

        return new TaskTimeline(
                TaskView.from(task, objectMapper),
                executions.stream().map(TaskExecutionView::from).toList(),
                decisions.stream().map(DecisionView::from).toList(),
                List.copyOf(events));
    }

    private static String finishDetails(TaskExecution execution) {
        StringBuilder details = new StringBuilder("result=").append(execution.getResult());
        if (execution.getErrorCode() != null) {
            details.append(" errorCode=").append(execution.getErrorCode());
        }
        if (execution.getErrorCategory() != null) {
            details.append(" errorCategory=").append(execution.getErrorCategory());
        }
        if (execution.getErrorMessage() != null) {
            details.append(" message=").append(execution.getErrorMessage());
        }
        return details.toString();
    }
}
