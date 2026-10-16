package com.yuki.reactor.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.error.TaskFailure;
import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.retry.RetryAdmissionLimiter;
import com.yuki.reactor.core.retry.RetryProperties;
import com.yuki.reactor.core.service.TaskClaim;
import com.yuki.reactor.core.service.TaskOperations;
import com.yuki.reactor.core.support.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Claims and executes tasks in this process.
 *
 * <p>The engine is deliberately constructed with explicit collaborators instead of field injection,
 * so a test can start a worker with its own configuration and stop it deterministically.
 *
 * <p>Each polling round claims at most as many tasks as there is free concurrency. Claiming is a
 * conditional update in MySQL, so several worker processes may poll the same candidates and only one
 * of them wins each task (PRD 5.2). Per task type caps can additionally limit how many tasks of one
 * type run in this process.
 *
 * <p>While an attempt runs, the engine renews its lease (PRD 5.3) and interrupts the handler when it
 * exceeds the timeout of the task. A handler that does not react to interruption keeps its worker
 * thread busy; the attempt is still reported as timed out, and its late result is rejected by the
 * fencing token check instead of overwriting the state produced by the next attempt.
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
    private final RetryAdmissionLimiter retryAdmission;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final Map<Long, InFlightAttempt> inFlightAttempts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inFlightByType = new ConcurrentHashMap<>();
    private volatile boolean running;

    private ScheduledExecutorService pollScheduler;
    private ScheduledExecutorService housekeepingScheduler;
    private ExecutorService executionPool;

    public TaskWorkerEngine(TaskOperations taskOperations,
                            TaskHandlerRegistry handlerRegistry,
                            TaskWorkerProperties properties,
                            RetryProperties retryProperties,
                            ObjectMapper objectMapper,
                            Clock clock) {
        this(taskOperations, handlerRegistry, properties, retryProperties, objectMapper, clock, null);
    }

    public TaskWorkerEngine(TaskOperations taskOperations,
                            TaskHandlerRegistry handlerRegistry,
                            TaskWorkerProperties properties,
                            RetryProperties retryProperties,
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
        this.retryAdmission = new RetryAdmissionLimiter(retryProperties, clock);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executionPool = Executors.newFixedThreadPool(properties.getConcurrency(), threadFactory("reactor-worker"));
        pollScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory("reactor-worker-poll"));
        // One thread renews leases, one enforces execution timeouts, so a slow database round trip
        // does not delay a timeout.
        housekeepingScheduler = Executors.newScheduledThreadPool(2, threadFactory("reactor-worker-housekeeping"));
        long intervalMillis = Math.max(1L, properties.getPollInterval().toMillis());
        pollScheduler.scheduleWithFixedDelay(this::pollQuietly, 0L, intervalMillis, TimeUnit.MILLISECONDS);
        long renewMillis = Math.max(1L, properties.effectiveLeaseRenewInterval().toMillis());
        housekeepingScheduler.scheduleWithFixedDelay(this::renewLeasesQuietly, renewMillis, renewMillis, TimeUnit.MILLISECONDS);
        running = true;
        log.info("worker {} started: concurrency={} pollInterval={} batchSize={} leaseTtl={} leaseRenewInterval={} retryAdmissionPerSecond={}",
                workerId,
                properties.getConcurrency(),
                properties.getPollInterval(),
                properties.getBatchSize(),
                properties.getLeaseTtl(),
                properties.effectiveLeaseRenewInterval(),
                retryAdmission.maxPerSecond());
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
        // Renewal stops only after the in-flight attempts are finished: as long as this worker keeps
        // its leases, no other worker may start a second execution of the same task.
        if (housekeepingScheduler != null) {
            housekeepingScheduler.shutdownNow();
            awaitTermination(housekeepingScheduler);
            housekeepingScheduler = null;
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

    /** Number of tasks of one type this worker is executing right now (normalized task type). */
    public int getInFlight(String taskType) {
        AtomicInteger counter = inFlightByType.get(normalize(taskType));
        return counter == null ? 0 : counter.get();
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
            // Retries are admitted at a limited rate so a wave of failing tasks cannot take every
            // execution slot away from fresh work (PRD 5.4).
            if (candidate.getStatus() == TaskStatus.RETRY_WAIT && !retryAdmission.tryAcquire()) {
                log.info("worker {} did not dispatch retry of task {}: retry admission limit of {}/s reached",
                        workerId, candidate.getId(), retryAdmission.maxPerSecond());
                continue;
            }
            if (typeLimitReached(candidate.getTaskType())) {
                log.info("worker {} did not dispatch task {}: task type '{}' is at its concurrency limit of {}",
                        workerId, candidate.getId(), candidate.getTaskType(), properties.limitFor(candidate.getTaskType()));
                continue;
            }
            TaskClaim claim = taskOperations.claimAndOpen(candidate.getId(), workerId, now, properties.getLeaseTtl());
            if (claim == null) {
                // Lost the race against another worker or an operator cancelling the task.
                continue;
            }
            inFlight.incrementAndGet();
            incrementType(candidate.getTaskType());
            try {
                executionPool.execute(() -> executeClaim(claim));
            } catch (RejectedExecutionException e) {
                // The engine is shutting down. The claim stays RUNNING until its lease expires and the
                // recovery scanner hands it to another worker.
                inFlight.decrementAndGet();
                decrementType(candidate.getTaskType());
                log.warn("worker {} dropped the claim of task {} while shutting down", workerId, candidate.getId());
            }
        }
    }

    private void executeClaim(TaskClaim claim) {
        Task task = claim.task();
        long taskId = task.getId();
        int attempt = task.getAttemptCount();
        long executionId = claim.execution().getId();
        long fencingToken = task.getFencingToken();
        InFlightAttempt handle = new InFlightAttempt(taskId, executionId, fencingToken);
        handle.bindThread(Thread.currentThread());
        inFlightAttempts.put(taskId, handle);
        ScheduledFuture<?> timeoutWatchdog = scheduleTimeoutWatchdog(task, handle);
        try {
            Exception failure = runHandler(task);
            // An interrupt that arrived after the handler returned must not leak into the reporting
            // calls below; whether it was a timeout is recorded on the handle instead.
            Thread.interrupted();
            if (failure == null) {
                boolean applied = taskOperations.complete(taskId, executionId, fencingToken, LocalDateTime.now(clock));
                if (!applied) {
                    log.warn("success of task {} attempt {} was rejected as stale", taskId, attempt);
                }
            } else if (handle.timedOut()) {
                reportFailure(taskId, executionId, fencingToken, attempt,
                        TaskFailure.timeout(task.getTimeoutMs() == null ? 0L : task.getTimeoutMs()));
            } else if (handle.leaseLost()) {
                // Another worker owns the task now; it decides the outcome, so nothing is reported.
                log.warn("task {} attempt {} stopped after losing its lease; its result is discarded", taskId, attempt);
            } else if (!running) {
                // Shutdown interrupted the handler: leave the execution record open and let lease
                // expiry recovery hand the task to another worker.
                log.info("task {} attempt {} was interrupted by shutdown; the outcome is left to lease recovery", taskId, attempt);
            } else {
                reportFailure(taskId, executionId, fencingToken, attempt, TaskFailure.classify(failure));
            }
        } catch (Exception e) {
            // Recording the outcome failed as well; the attempt row stays open on purpose so the
            // inconsistency is visible instead of being hidden (recovery closes it later).
            log.error("could not record the outcome of task {} attempt {}", taskId, attempt, e);
        } finally {
            if (timeoutWatchdog != null) {
                timeoutWatchdog.cancel(false);
            }
            handle.markFinished();
            inFlightAttempts.remove(taskId, handle);
            decrementType(task.getTaskType());
            inFlight.decrementAndGet();
        }
    }

    private void reportFailure(long taskId, long executionId, long fencingToken, int attempt, TaskFailure failure) {
        log.warn("task {} attempt {} failed: {} ({}) [category={}]",
                taskId, attempt, failure.message(), failure.code(), failure.category());
        boolean applied = taskOperations.fail(taskId, executionId, fencingToken, failure, LocalDateTime.now(clock));
        if (!applied) {
            log.warn("failure of task {} attempt {} was rejected as stale", taskId, attempt);
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

    private ScheduledFuture<?> scheduleTimeoutWatchdog(Task task, InFlightAttempt handle) {
        Long timeoutMs = task.getTimeoutMs();
        if (timeoutMs == null || timeoutMs <= 0) {
            return null;
        }
        return housekeepingScheduler.schedule(() -> {
            if (handle.finished() || !handle.markTimedOut()) {
                return;
            }
            log.warn("task {} attempt {} exceeded its timeout of {} ms; interrupting the handler",
                    task.getId(), task.getAttemptCount(), timeoutMs);
            handle.interruptThread();
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void renewLeasesQuietly() {
        try {
            LocalDateTime now = LocalDateTime.now(clock);
            for (InFlightAttempt handle : inFlightAttempts.values()) {
                if (handle.leaseLost()) {
                    continue;
                }
                try {
                    boolean renewed = taskOperations.renewLease(handle.taskId(), handle.fencingToken(),
                            properties.getLeaseTtl(), now);
                    if (!renewed) {
                        log.warn("worker {} lost the lease of task {} (fencing token {}); interrupting the handler",
                                workerId, handle.taskId(), handle.fencingToken());
                        handle.markLeaseLost();
                        handle.interruptThread();
                    }
                } catch (Exception e) {
                    // Database hiccup: keep the attempt alive and try again on the next heartbeat. If the
                    // outage outlasts the lease, recovery scans the task like any other expired lease.
                    log.warn("worker {} could not renew the lease of task {}: {}",
                            workerId, handle.taskId(), Messages.describe(e));
                }
            }
        } catch (Exception e) {
            log.warn("worker {} lease renewal round failed: {}", workerId, Messages.describe(e));
        }
    }

    private boolean typeLimitReached(String taskType) {
        int limit = properties.limitFor(taskType);
        return limit > 0 && getInFlight(taskType) >= limit;
    }

    private void incrementType(String taskType) {
        inFlightByType.computeIfAbsent(normalize(taskType), key -> new AtomicInteger()).incrementAndGet();
    }

    private void decrementType(String taskType) {
        inFlightByType.computeIfAbsent(normalize(taskType), key -> new AtomicInteger()).decrementAndGet();
    }

    private static String normalize(String taskType) {
        return taskType == null ? "" : taskType.trim().toLowerCase(Locale.ROOT);
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
        Duration renewInterval = properties.effectiveLeaseRenewInterval();
        if (renewInterval.isZero() || renewInterval.isNegative()) {
            throw new IllegalArgumentException("reactor.worker.lease-renew-interval must be positive");
        }
        if (renewInterval.compareTo(properties.getLeaseTtl()) >= 0) {
            throw new IllegalArgumentException("reactor.worker.lease-renew-interval must be shorter than reactor.worker.lease-ttl");
        }
        for (Map.Entry<String, Integer> entry : properties.getTaskTypeLimits().entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                throw new IllegalArgumentException("reactor.worker.task-type-limits contains a blank task type");
            }
            if (entry.getValue() == null || entry.getValue() < 1) {
                throw new IllegalArgumentException("reactor.worker.task-type-limits." + entry.getKey() + " must be at least 1");
            }
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

    /** Bookkeeping for one attempt this worker is responsible for. */
    private static final class InFlightAttempt {

        private final long taskId;
        private final long executionId;
        private final long fencingToken;
        private final AtomicBoolean timedOut = new AtomicBoolean();
        private final AtomicBoolean leaseLost = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();
        private volatile Thread thread;

        private InFlightAttempt(long taskId, long executionId, long fencingToken) {
            this.taskId = taskId;
            this.executionId = executionId;
            this.fencingToken = fencingToken;
        }

        private void bindThread(Thread thread) {
            this.thread = thread;
        }

        private long taskId() {
            return taskId;
        }

        private long fencingToken() {
            return fencingToken;
        }

        private boolean timedOut() {
            return timedOut.get();
        }

        private boolean markTimedOut() {
            return timedOut.compareAndSet(false, true);
        }

        private boolean leaseLost() {
            return leaseLost.get();
        }

        private void markLeaseLost() {
            leaseLost.set(true);
        }

        private boolean finished() {
            return finished.get();
        }

        private void markFinished() {
            finished.set(true);
        }

        private void interruptThread() {
            Thread current = thread;
            if (current != null) {
                current.interrupt();
            }
        }
    }
}
