package com.yuki.reactor.core.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskPage;
import com.yuki.reactor.core.dto.TaskQuery;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.exception.InvalidTaskStateException;
import com.yuki.reactor.core.exception.TaskNotFoundException;
import com.yuki.reactor.core.exception.UnknownTaskTypeException;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.statemachine.TaskStateMachine;
import com.yuki.reactor.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Client facing task operations: submission, inspection, cancellation and administrative replay.
 */
@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    static final int DEFAULT_PRIORITY = 100;
    static final int DEFAULT_MAX_ATTEMPTS = 3;
    static final long DEFAULT_TIMEOUT_MS = 60_000L;
    static final String DEFAULT_IDEMPOTENCY_SCOPE = "default";

    private static final int MIN_PRIORITY = 0;
    private static final int MAX_PRIORITY = 1000;
    private static final int MAX_MAX_ATTEMPTS = 100;
    private static final long MIN_TIMEOUT_MS = 100L;
    private static final long MAX_TIMEOUT_MS = 86_400_000L;
    private static final int MAX_TASK_TYPE_LENGTH = 64;
    private static final int MAX_SCOPE_LENGTH = 64;
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_PAGE_SIZE = 200;

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final TaskHandlerRegistry handlerRegistry;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public TaskService(TaskMapper taskMapper,
                       TaskExecutionMapper executionMapper,
                       TaskHandlerRegistry handlerRegistry,
                       ObjectMapper objectMapper,
                       Clock clock) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.handlerRegistry = handlerRegistry;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    /**
     * Persists a task, or returns the already existing task when the idempotency key was used
     * before (PRD 5.1).
     *
     * <p>Idempotency is enforced by the unique key {@code (idempotency_scope, idempotency_key)} in
     * MySQL, not by an in-memory or Redis marker. The insert is a single autocommitted statement, so
     * a duplicate key error is caught here and resolved by reading the winning row instead of
     * leaving a half-open transaction behind.
     */
    public TaskSubmission submit(SubmitTaskCommand command) {
        String taskType = requireTaskType(command.taskType());
        String payload = normalizePayload(command.payload());
        int priority = validatePriority(command.priority());
        int maxAttempts = validateMaxAttempts(command.maxAttempts());
        long timeoutMs = validateTimeout(command.timeoutMs());
        String scope = normalizeScope(command.idempotencyScope());
        String key = normalizeIdempotencyKey(command.idempotencyKey());

        LocalDateTime now = LocalDateTime.now(clock);
        Task task = new Task();
        task.setTaskType(taskType);
        task.setPayload(payload);
        task.setStatus(TaskStatus.PENDING);
        task.setPriority(priority);
        task.setIdempotencyScope(scope);
        task.setIdempotencyKey(key);
        task.setAttemptCount(0);
        task.setMaxAttempts(maxAttempts);
        task.setNextRunAt(now);
        task.setTimeoutMs(timeoutMs);
        task.setFencingToken(0L);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);

        try {
            taskMapper.insert(task);
            log.info("submitted task {} type={} priority={} maxAttempts={}", task.getId(), taskType, priority, maxAttempts);
            return new TaskSubmission(toView(task), true);
        } catch (DuplicateKeyException e) {
            if (key == null) {
                // Without a key the unique constraint cannot be the cause of this failure.
                throw e;
            }
            Task existing = taskMapper.findByIdempotencyKey(scope, key);
            if (existing == null) {
                throw e;
            }
            log.info("idempotent replay for scope={} key={} returning task {}", scope, key, existing.getId());
            return new TaskSubmission(toView(existing), false);
        }
    }

    public TaskView get(long taskId) {
        return toView(requireTask(taskId));
    }

    public TaskPage query(TaskQuery query) {
        if (query.page() < 0) {
            throw new IllegalArgumentException("page must be greater than or equal to 0");
        }
        if (query.size() < 1 || query.size() > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("size must be between 1 and " + MAX_PAGE_SIZE);
        }
        if (query.from() != null && query.to() != null && !query.from().isBefore(query.to())) {
            throw new IllegalArgumentException("from must be before to");
        }
        long total = taskMapper.countByQuery(query.status(), query.from(), query.to());
        long offset = (long) query.page() * query.size();
        List<TaskView> items = taskMapper
                .findPage(query.status(), query.from(), query.to(), query.size(), offset)
                .stream()
                .map(this::toView)
                .toList();
        return new TaskPage(items, total, query.page(), query.size());
    }

    /** Execution attempts of a task, oldest first (PRD 7.2). */
    public List<TaskExecutionView> executions(long taskId) {
        requireTask(taskId);
        return executionMapper.findByTaskId(taskId).stream().map(TaskExecutionView::from).toList();
    }

    /**
     * Cancels a task that has not started executing. Cancelling a task that is already running is
     * rejected: the state machine has no {@code RUNNING -> CANCELLED} transition, and a running task
     * may already have produced external side effects that no cancellation can undo (PRD 4.2).
     */
    public TaskView cancel(long taskId, String operator, String reason) {
        String actor = requireOperator(operator);
        String cause = requireReason(reason);
        Task task = requireTask(taskId);
        TaskStateMachine.requireTransition(task.getStatus(), TaskStatus.CANCELLED);

        String note = Messages.operatorNote("cancelled", actor, cause);
        int updated = taskMapper.markCancelled(taskId, Messages.truncate(note, Messages.MAX_ERROR_LENGTH), LocalDateTime.now(clock));
        if (updated != 1) {
            throw new InvalidTaskStateException(
                    "task " + taskId + " is no longer in a cancellable state (expected PENDING or RETRY_WAIT)");
        }
        log.info("task {} cancelled by {}: {}", taskId, actor, cause);
        return get(taskId);
    }

    /**
     * Administrative replay of a failed task: it becomes pending again with a fresh attempt budget.
     * Replay is explicitly not a way to erase history - the previous error and all execution records
     * are kept.
     */
    public TaskView replay(long taskId, String operator, String reason) {
        String actor = requireOperator(operator);
        String cause = requireReason(reason);
        Task task = requireTask(taskId);
        TaskStateMachine.requireTransition(task.getStatus(), TaskStatus.PENDING);

        int updated = taskMapper.resetForReplay(taskId, LocalDateTime.now(clock));
        if (updated != 1) {
            throw new InvalidTaskStateException("task " + taskId + " is no longer FAILED and cannot be replayed");
        }
        log.info("task {} replayed by {}: {}", taskId, actor, cause);
        return get(taskId);
    }

    private Task requireTask(long taskId) {
        Task task = taskMapper.selectById(taskId);
        if (task == null) {
            throw new TaskNotFoundException(taskId);
        }
        return task;
    }

    private String requireTaskType(String taskType) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("taskType is required");
        }
        String trimmed = taskType.trim();
        if (trimmed.length() > MAX_TASK_TYPE_LENGTH) {
            throw new IllegalArgumentException("taskType must not exceed " + MAX_TASK_TYPE_LENGTH + " characters");
        }
        if (!handlerRegistry.knows(trimmed)) {
            throw new UnknownTaskTypeException(trimmed);
        }
        return trimmed;
    }

    /**
     * Payloads are accepted as JSON objects only. They are stored as data and never interpreted as
     * a class name, SQL fragment or script (PRD 8.4).
     */
    private String normalizePayload(String payload) {
        String candidate = (payload == null || payload.isBlank()) ? "{}" : payload;
        JsonNode node;
        try {
            node = objectMapper.readTree(candidate);
        } catch (Exception e) {
            throw new IllegalArgumentException("payload must be valid JSON: " + e.getMessage());
        }
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        return node.toString();
    }

    private int validatePriority(Integer priority) {
        int value = priority == null ? DEFAULT_PRIORITY : priority;
        if (value < MIN_PRIORITY || value > MAX_PRIORITY) {
            throw new IllegalArgumentException("priority must be between " + MIN_PRIORITY + " and " + MAX_PRIORITY);
        }
        return value;
    }

    private int validateMaxAttempts(Integer maxAttempts) {
        int value = maxAttempts == null ? DEFAULT_MAX_ATTEMPTS : maxAttempts;
        if (value < 1 || value > MAX_MAX_ATTEMPTS) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and " + MAX_MAX_ATTEMPTS);
        }
        return value;
    }

    private long validateTimeout(Long timeoutMs) {
        long value = timeoutMs == null ? DEFAULT_TIMEOUT_MS : timeoutMs;
        if (value < MIN_TIMEOUT_MS || value > MAX_TIMEOUT_MS) {
            throw new IllegalArgumentException("timeoutMs must be between " + MIN_TIMEOUT_MS + " and " + MAX_TIMEOUT_MS);
        }
        return value;
    }

    private String normalizeScope(String scope) {
        if (scope == null || scope.isBlank()) {
            return DEFAULT_IDEMPOTENCY_SCOPE;
        }
        String trimmed = scope.trim();
        if (trimmed.length() > MAX_SCOPE_LENGTH) {
            throw new IllegalArgumentException("idempotencyScope must not exceed " + MAX_SCOPE_LENGTH + " characters");
        }
        return trimmed;
    }

    private String normalizeIdempotencyKey(String key) {
        if (key == null || key.isBlank()) {
            return null;
        }
        String trimmed = key.trim();
        if (trimmed.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("idempotencyKey must not exceed " + MAX_KEY_LENGTH + " characters");
        }
        return trimmed;
    }

    private String requireOperator(String operator) {
        if (operator == null || operator.isBlank()) {
            throw new IllegalArgumentException("operator is required for administrative operations");
        }
        return operator.trim();
    }

    private String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason is required for administrative operations");
        }
        return reason.trim();
    }

    private TaskView toView(Task task) {
        return TaskView.from(task, objectMapper);
    }
}
