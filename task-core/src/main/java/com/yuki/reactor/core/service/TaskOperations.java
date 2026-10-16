package com.yuki.reactor.core.service;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Worker facing task operations: claiming work and reporting results.
 *
 * <p>Claiming and result reporting are transactional, and both are guarded by conditional updates
 * carrying the fencing token. A result that belongs to an attempt that no longer owns the task is
 * recorded as {@link ExecutionResult#ABANDONED} and never overwrites task state (PRD 8.1).
 */
@Service
public class TaskOperations {

    private static final Logger log = LoggerFactory.getLogger(TaskOperations.class);

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;

    public TaskOperations(TaskMapper taskMapper, TaskExecutionMapper executionMapper) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
    }

    /** Candidate tasks a worker may attempt to claim, best first. */
    public List<Task> findClaimable(LocalDateTime now, int limit) {
        if (limit < 1) {
            return List.of();
        }
        return taskMapper.findClaimable(now, limit);
    }

    public Task get(long taskId) {
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            throw new TaskNotFoundException(taskId);
        }
        return task;
    }

    /**
     * Claims a task and opens its execution record in one transaction.
     *
     * <p>The claim is a conditional update; when another worker (or an operator cancelling the task)
     * won the race, this returns {@code null} and nothing is written. The attempt row is created
     * immediately so that a worker dying mid-execution still leaves a trace (PRD 7.2).
     */
    @Transactional
    public TaskClaim claimAndOpen(long taskId, String workerId, LocalDateTime now, Duration leaseTtl) {
        int claimed = taskMapper.claim(taskId, workerId, now.plus(leaseTtl), now);
        if (claimed != 1) {
            return null;
        }
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            // Cannot happen inside the same transaction that just updated the row.
            throw new TaskNotFoundException(taskId);
        }
        TaskExecution execution = new TaskExecution();
        execution.setTaskId(taskId);
        execution.setAttempt(task.getAttemptCount());
        execution.setWorkerId(workerId);
        execution.setFencingToken(task.getFencingToken());
        execution.setStartedAt(now);
        executionMapper.insert(execution);
        return new TaskClaim(task, execution);
    }

    /**
     * Reports a successful attempt.
     *
     * @return {@code true} when the task state was written, {@code false} when the result was
     *         rejected because another owner has taken over the task
     */
    @Transactional
    public boolean complete(long taskId, long executionId, long fencingToken, LocalDateTime now) {
        int updated = taskMapper.markSucceeded(taskId, fencingToken, now);
        if (updated == 1) {
            executionMapper.finish(executionId, ExecutionResult.SUCCEEDED, null, null, now);
            return true;
        }
        rejectStale(taskId, executionId, fencingToken, now);
        return false;
    }

    /**
     * Reports a failed attempt. In V1.0 phase 1 there is no retry classification yet, so a failure is
     * terminal; retry scheduling arrives in phase 2.
     *
     * @return {@code true} when the task state was written, {@code false} when the result was rejected
     */
    @Transactional
    public boolean fail(long taskId, long executionId, long fencingToken, String errorCode, String message, LocalDateTime now) {
        String safeMessage = Messages.truncate(message, Messages.MAX_ERROR_LENGTH);
        int updated = taskMapper.markFailed(taskId, fencingToken, safeMessage, now);
        if (updated == 1) {
            executionMapper.finish(executionId, ExecutionResult.FAILED, errorCode, safeMessage, now);
            return true;
        }
        rejectStale(taskId, executionId, fencingToken, now);
        return false;
    }

    private void rejectStale(long taskId, long executionId, long fencingToken, LocalDateTime now) {
        log.warn("rejected late result for task {}: attempt with fencing token {} no longer owns it",
                taskId, fencingToken);
        executionMapper.finish(executionId,
                ExecutionResult.ABANDONED,
                ErrorCodes.STALE_WRITER,
                "task is no longer owned by attempt with fencing token " + fencingToken,
                now);
    }
}
