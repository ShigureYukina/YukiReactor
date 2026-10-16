package com.yuki.reactor.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.ErrorCodes;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.service.TaskClaim;
import com.yuki.reactor.core.service.TaskOperations;
import com.yuki.reactor.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.InetAddress;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Claims and executes tasks in this process.
 *
 * <p>The engine is deliberately constructed with explicit collaborators instead of field injection,
 * so a test can start a worker with its own configuration and stop it deterministically.
 *
 * <p>Each polling round claims at most as many tasks as there is free concurrency. Claiming is a
 * conditional update in MySQL, so several worker processes may poll the same candidates and only one
 * of them wins each task (PRD 5.2).
 *
 * <p>Phase 1 scope: a claim, one execution attempt, one result. Lease renewal, execution timeouts,
 * retry classification and recovery of expired leases are implemented in phase 2.
 */
public class TaskWorkerEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TaskWorkerEngine.class);

    private static final int MAX_WORKER_ID_LENGTH = 128;
    private static final AtomicInteger INSTANCE_SEQUENCE = new AtomicInteger();

    private final TaskOperations taskOperations;
    private final TaskHandlerRegistry handlerRegistry;
    private final TaskWorkerProperties properties;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final String workerId;

    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean running;

    private ScheduledExecutorService pollScheduler;
    private ExecutorService executionPool;

    public TaskWorkerEngine(TaskOperations taskOperations,
                            TaskHandlerRegistry handlerRegistry,
                            TaskWorkerProperties properties,
                            ObjectMapper objectMapper,
                            Clock clock) {
        this(taskOperations, handlerRegistry, properties, objectMapper, clock, null);
    }

    public TaskWorkerEngine(TaskOperations taskOperations,
                            TaskHandlerRegistry handlerRegistry,
                            TaskWorkerProperties properties,
                            ObjectMapper objectMapper,
                            Clock clock,
                            String workerIdOverride) {
        this.taskOperations = taskOperations;
        this.handlerRegistry = handlerRegistry;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.clock = clock;
        validate(properties);
        this.workerId = resolveWorkerId(properties, workerIdOverride);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executionPool = Executors.newFixedThreadPool(properties.getConcurrency(), threadFactory("reactor-worker"));
        pollScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory("reactor-worker-poll"));
        long intervalMillis = Math.max(1L, properties.getPollInterval().toMillis());
        pollScheduler.scheduleWithFixedDelay(this::pollQuietly, 0L, intervalMillis, TimeUnit.MILLISECONDS);
        running = true;
        log.info("worker {} started: concurrency={} pollInterval={} batchSize={} leaseTtl={}",
                workerId,
                properties.getConcurrency(),
                properties.getPollInterval(),
                properties.getBatchSize(),
                properties.getLeaseTtl());
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (pollScheduler != null) {
            pollScheduler.shutdownNow();
            awaitTermination(pollScheduler);
            pollScheduler = null;
        }
        if (executionPool != null) {
            executionPool.shutdown();
            awaitTermination(executionPool);
            executionPool = null;
        }
        log.info("worker {} stopped", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public String getWorkerId() {
        return workerId;
    }

    /** Number of tasks this worker is executing right now. */
    public int getInFlight() {
        return inFlight.get();
    }

    /**
     * A failed polling round must never kill the scheduler, otherwise the worker would stop working
     * silently - for example while the database is briefly unavailable (PRD 5.7).
     */
    private void pollQuietly() {
        try {
            poll();
        } catch (Exception e) {
            log.warn("worker {} polling round failed: {}", workerId, Messages.describe(e));
        }
    }

    private void poll() {
        int available = properties.getConcurrency() - inFlight.get();
        if (available <= 0) {
            return;
        }
        int limit = Math.min(available, properties.getBatchSize());
        LocalDateTime now = LocalDateTime.now(clock);
        List<Task> candidates = taskOperations.findClaimable(now, limit);
        for (Task candidate : candidates) {
            if (inFlight.get() >= properties.getConcurrency()) {
                return;
            }
            TaskClaim claim = taskOperations.claimAndOpen(candidate.getId(), workerId, now, properties.getLeaseTtl());
            if (claim == null) {
                // Lost the race against another worker or an operator cancelling the task.
                continue;
            }
            inFlight.incrementAndGet();
            executionPool.execute(() -> executeClaim(claim));
        }
    }

    private void executeClaim(TaskClaim claim) {
        Task task = claim.task();
        long taskId = task.getId();
        int attempt = task.getAttemptCount();
        long executionId = claim.execution().getId();
        long fencingToken = task.getFencingToken();
        try {
            Exception failure = runHandler(task);
            if (failure == null) {
                boolean applied = taskOperations.complete(taskId, executionId, fencingToken, LocalDateTime.now(clock));
                if (!applied) {
                    log.warn("success of task {} attempt {} was rejected as stale", taskId, attempt);
                }
            } else {
                log.warn("task {} attempt {} failed: {}", taskId, attempt, Messages.describe(failure));
                boolean applied = taskOperations.fail(taskId, executionId, fencingToken,
                        ErrorCodes.EXECUTION_ERROR, Messages.describe(failure), LocalDateTime.now(clock));
                if (!applied) {
                    log.warn("failure of task {} attempt {} was rejected as stale", taskId, attempt);
                }
            }
        } catch (Exception e) {
            // Recording the outcome failed as well; the attempt row stays open on purpose so the
            // inconsistency is visible instead of being hidden.
            log.error("could not record the outcome of task {} attempt {}", taskId, attempt, e);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    /** @return the handler failure, or {@code null} when the attempt succeeded */
    private Exception runHandler(Task task) {
        try {
            TaskHandler handler = handlerRegistry.require(task.getTaskType());
            handler.execute(new TaskContext(task.getId(), task.getAttemptCount(), task.getTaskType(), readPayload(task)));
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    private JsonNode readPayload(Task task) {
        try {
            JsonNode payload = objectMapper.readTree(task.getPayload());
            return payload == null ? objectMapper.createObjectNode() : payload;
        } catch (Exception e) {
            log.warn("task {} has an unreadable payload; executing with an empty payload", task.getId());
            return objectMapper.createObjectNode();
        }
    }

    private static void validate(TaskWorkerProperties properties) {
        if (properties.getConcurrency() < 1) {
            throw new IllegalArgumentException("reactor.worker.concurrency must be at least 1");
        }
        if (properties.getBatchSize() < 1) {
            throw new IllegalArgumentException("reactor.worker.batch-size must be at least 1");
        }
        if (properties.getPollInterval() == null || properties.getPollInterval().isZero() || properties.getPollInterval().isNegative()) {
            throw new IllegalArgumentException("reactor.worker.poll-interval must be positive");
        }
        if (properties.getLeaseTtl() == null || properties.getLeaseTtl().isZero() || properties.getLeaseTtl().isNegative()) {
            throw new IllegalArgumentException("reactor.worker.lease-ttl must be positive");
        }
    }

    private static String resolveWorkerId(TaskWorkerProperties properties, String override) {
        String candidate = override != null && !override.isBlank() ? override.trim() : properties.getId().trim();
        if (candidate.isEmpty()) {
            candidate = defaultWorkerId();
        }
        if (candidate.length() > MAX_WORKER_ID_LENGTH) {
            throw new IllegalArgumentException("worker id must not exceed " + MAX_WORKER_ID_LENGTH + " characters");
        }
        return candidate;
    }

    private static String defaultWorkerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        long pid = ProcessHandle.current().pid();
        return host + "-" + pid + "-" + Integer.toHexString(INSTANCE_SEQUENCE.incrementAndGet());
    }

    private static ThreadFactory threadFactory(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }

    private void awaitTermination(ExecutorService pool) {
        long graceMillis = properties.getLeaseTtl().toMillis() + 1_000L;
        try {
            if (!pool.awaitTermination(graceMillis, TimeUnit.MILLISECONDS)) {
                log.warn("worker {} did not stop within {} ms; forcing shutdown", workerId, graceMillis);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
    }
}
