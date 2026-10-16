package com.yuki.reactor.observability.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.observability.dto.TaskTimeline;
import com.yuki.reactor.observability.dto.TimelineEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * The timeline is the human readable form of PRD 5.6: it may only replay what was written - creation,
 * attempts, decisions - in the order it happened, and it must keep an unreported attempt visible
 * instead of pretending the task ended.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TaskTimelineServiceTest {

    private final LocalDateTime created = LocalDateTime.of(2026, 10, 16, 9, 59, 0);

    @Mock
    private TaskMapper taskMapper;

    @Mock
    private TaskExecutionMapper executionMapper;

    @Mock
    private SchedulerDecisionMapper decisionMapper;

    private TaskTimelineService service() {
        return new TaskTimelineService(taskMapper, executionMapper, decisionMapper, new ObjectMapper());
    }

    private Task task() {
        Task task = new Task();
        task.setId(1L);
        task.setTaskType("scenario");
        task.setPayload("{}");
        task.setStatus(TaskStatus.SUCCEEDED);
        task.setPriority(5);
        task.setIdempotencyScope("scope");
        task.setIdempotencyKey("key");
        task.setAttemptCount(2);
        task.setMaxAttempts(3);
        task.setTimeoutMs(30_000L);
        task.setCreatedAt(created);
        task.setUpdatedAt(created.plusMinutes(1));
        return task;
    }

    private TaskExecution execution(long id, int attempt, LocalDateTime startedAt, ExecutionResult result) {
        TaskExecution execution = new TaskExecution();
        execution.setId(id);
        execution.setTaskId(1L);
        execution.setAttempt(attempt);
        execution.setWorkerId("worker-a");
        execution.setFencingToken((long) attempt);
        execution.setStartedAt(startedAt);
        execution.setFinishedAt(result == null ? null : startedAt.plusSeconds(5));
        execution.setResult(result);
        if (result == ExecutionResult.FAILED) {
            execution.setErrorCode("EXECUTION_ERROR");
            execution.setErrorCategory(ErrorCategory.TRANSIENT);
            execution.setErrorMessage("boom");
        }
        return execution;
    }

    @Test
    void refusesToExplainATaskThatDoesNotExist() {
        when(taskMapper.selectById(404L)).thenReturn(null);

        assertThatThrownBy(() -> service().timeline(404L)).isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void replaysCreationAttemptsAndDecisionsInTheOrderTheyHappened() {
        when(taskMapper.selectById(1L)).thenReturn(task());
        LocalDateTime firstStart = created.plusSeconds(10);
        when(executionMapper.findByTaskId(1L)).thenReturn(List.of(
                execution(10L, 1, firstStart, ExecutionResult.FAILED),
                execution(11L, 2, created.plusSeconds(30), ExecutionResult.SUCCEEDED)));
        SchedulerDecision retry = new SchedulerDecision();
        retry.setId(1L);
        retry.setTaskId(1L);
        retry.setExecutionId(10L);
        retry.setDecisionType(DecisionType.RETRY_SCHEDULED);
        retry.setReasonCode(ReasonCodes.RETRY_BACKOFF);
        retry.setWorkerId("worker-a");
        retry.setNextRunAt(created.plusSeconds(30));
        retry.setObservedDetails("attempt 1/3 backed off by 2000 ms");
        retry.setCreatedAt(firstStart.plusSeconds(5));
        when(decisionMapper.findByTaskId(1L)).thenReturn(List.of(retry));

        TaskTimeline timeline = service().timeline(1L);

        assertThat(timeline.task().id()).isEqualTo(1L);
        assertThat(timeline.executions()).hasSize(2);
        assertThat(timeline.decisions()).hasSize(1);
        assertThat(timeline.events().stream().map(event -> event.type())).containsExactly(
                TimelineEventType.TASK_CREATED,
                TimelineEventType.ATTEMPT_STARTED,
                TimelineEventType.ATTEMPT_FINISHED,
                TimelineEventType.DECISION,
                TimelineEventType.ATTEMPT_STARTED,
                TimelineEventType.ATTEMPT_FINISHED);
        assertThat(timeline.events())
                .as("a reader must be able to walk the timeline forward")
                .isSortedAccordingTo(Comparator.comparing(event -> event.at()));
    }

    @Test
    void keepsTheReasonCodeOnTheDecisionThatCarriedIt() {
        when(taskMapper.selectById(1L)).thenReturn(task());
        when(executionMapper.findByTaskId(1L)).thenReturn(List.of());
        SchedulerDecision reclaimed = new SchedulerDecision();
        reclaimed.setId(2L);
        reclaimed.setTaskId(1L);
        reclaimed.setExecutionId(10L);
        reclaimed.setDecisionType(DecisionType.LEASE_RECLAIMED);
        reclaimed.setReasonCode(ReasonCodes.LEASE_EXPIRED);
        reclaimed.setWorkerId("worker-gone");
        reclaimed.setObservedDetails("lease expired, requeued for the next claim");
        reclaimed.setCreatedAt(created.plusSeconds(40));
        when(decisionMapper.findByTaskId(1L)).thenReturn(List.of(reclaimed));

        TaskTimeline timeline = service().timeline(1L);

        assertThat(timeline.events().stream().map(event -> event.type())).containsExactly(
                TimelineEventType.TASK_CREATED,
                TimelineEventType.DECISION);
        var decision = timeline.events().get(1);
        assertThat(decision.type()).isEqualTo(TimelineEventType.DECISION);
        assertThat(decision.decisionType()).isEqualTo(DecisionType.LEASE_RECLAIMED);
        assertThat(decision.reasonCode()).isEqualTo(ReasonCodes.LEASE_EXPIRED);
        assertThat(decision.executionId()).isEqualTo(10L);
        assertThat(decision.details()).contains("requeued");
    }

    @Test
    void leavesAnAttemptThatNeverReportedVisibleAsAnOpenStart() {
        when(taskMapper.selectById(1L)).thenReturn(task());
        when(executionMapper.findByTaskId(1L))
                .thenReturn(List.of(execution(10L, 1, created.plusSeconds(10), null)));
        when(decisionMapper.findByTaskId(1L)).thenReturn(List.of());

        TaskTimeline timeline = service().timeline(1L);

        assertThat(timeline.events().stream().map(event -> event.type())).containsExactly(
                TimelineEventType.TASK_CREATED,
                TimelineEventType.ATTEMPT_STARTED);
        assertThat(timeline.events().get(1).details()).contains("attempt 1").contains("worker-a");
    }
}
