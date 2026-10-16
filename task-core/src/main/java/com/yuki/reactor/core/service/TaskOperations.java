package com.yuki.reactor.core.service;

import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.core.error.TaskFailure;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.retry.RetryPolicy;
import com.yuki.reactor.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Worker facing task operations: claiming work, renewing leases and reporting results.
 *
 * <p>Claiming and result reporting are transactional, and both are guarded by conditional updates
 * carrying the fencing token. A result that belongs to an attempt that no longer owns the task is
 * recorded as {@link ExecutionResult#ABANDONED} and never overwrites task state (PRD 8.1).
 *
 * <p>Failure handling follows PRD 5.4: retryable categories are rescheduled with exponential
 * backoff while attempt budget remains, everything else - and any failure that used the last
 * attempt - moves the task to its terminal {@code FAILED} state.
 */
@Service
public class TaskOperations {

    private static final Logger log = LoggerFactory.getLogger(TaskOperations.class);

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final RetryPolicy retryPolicy;

    public TaskOperations(TaskMapper taskMapper, TaskExecutionMapper executionMapper, RetryPolicy retryPolicy) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.retryPolicy = retryPolicy;
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
            executionMapper.finish(executionId, ExecutionResult.SUCCEEDED, null, null, null, now);
            return true;
        }
        rejectStale(taskId, executionId, fencingToken, now);
        return false;
    }

    /**
     * Reports a failed attempt and decides what happens next.
     *
     * <p>Retryable failures with attempt budget left go back to {@code RETRY_WAIT} with a backoff
     * delay; non-retryable failures and exhausted attempt budgets end in {@code FAILED} with the
     * {@code MAX_RETRIES_EXCEEDED} code.
     *
     * @return {@code true} when the task state was written, {@code false} when the result was rejected
     */
    @Transactional
    public boolean fail(long taskId, long executionId, long fencingToken, TaskFailure failure, LocalDateTime now) {
        Task owned = taskMapper.findOwned(taskId, fencingToken);
        if (owned == null) {
            rejectStale(taskId, executionId, fencingToken, now);
            return false;
        }

        int attempts = owned.getAttemptCount();
        int maxAttempts = owned.getMaxAttempts();
        boolean exhausted = attempts >= maxAttempts;
        String code = failure.code();
        String message = Messages.truncate(failure.message(), Messages.MAX_ERROR_LENGTH);

        if (failure.retryable() && !exhausted) {
            LocalDateTime nextRunAt = retryPolicy.nextRunAt(now, attempts, failure.retryAfterHint());
            String error = Messages.describe(code, failure.category(), message);
            if (taskMapper.markRetryWait(taskId, fencingToken, nextRunAt, error, now) == 1) {
                executionMapper.finish(executionId, ExecutionResult.FAILED, code, failure.category(), message, now);
                log.debug("task {} failed as {} and is retried at {}", taskId, failure.category(), nextRunAt);
                return true;
            }
        } else {
            if (exhausted) {
                code = ErrorCodes.MAX_RETRIES_EXCEEDED;
                message = Messages.truncate(
                        message + " (max attempts reached: " + attempts + "/" + maxAttempts + ")",
                        Messages.MAX_ERROR_LENGTH);
            }
            String error = Messages.describe(code, failure.category(), message);
            if (taskMapper.markFailed(taskId, fencingToken, error, now) == 1) {
                executionMapper.finish(executionId, ExecutionResult.FAILED, code, failure.category(), message, now);
                log.debug("task {} failed terminally as {} ({})", taskId, failure.category(), code);
                return true;
            }
        }

        rejectStale(taskId, executionId, fencingToken, now);
        return false;
    }

    /**
     * Extends the lease of a running attempt (PRD 5.3).
     *
     * @return {@code false} when the attempt no longer owns the task, which tells the worker to stop
     *         working on it
     */
    public boolean renewLease(long taskId, long fencingToken, Duration leaseTtl, LocalDateTime now) {
        return taskMapper.renewLease(taskId, fencingToken, now.plus(leaseTtl), now) == 1;
    }

    private void rejectStale(long taskId, long executionId, long fencingToken, LocalDateTime now) {
        log.warn("rejected late result for task {}: attempt with fencing token {} no longer owns it",
                taskId, fencingToken);
        executionMapper.finish(executionId,
                ExecutionResult.ABANDONED,
                ErrorCodes.STALE_WRITER,
                ErrorCategory.EXECUTOR_LOST,
                "task is no longer owned by attempt with fencing token " + fencingToken,
                now);
    }
}
