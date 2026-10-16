package com.yuki.reactor.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.decision.SchedulerDecisionRecorder;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.retry.RetryProperties;
import com.yuki.reactor.core.service.SubmitTaskCommand;
import com.yuki.reactor.core.service.TaskOperations;
import com.yuki.reactor.core.service.TaskService;
import com.yuki.reactor.core.service.TaskSubmission;
import com.yuki.reactor.core.signals.RuntimeSignals;
import com.yuki.reactor.recovery.LeaseRecoveryScanner;
import com.yuki.reactor.recovery.LeaseRecoveryService;
import com.yuki.reactor.recovery.RecoveryProperties;
import com.yuki.reactor.recovery.RecoveryScanResult;
import com.yuki.reactor.worker.TaskWorkerEngine;
import com.yuki.reactor.worker.TaskWorkerProperties;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Base class for integration tests: real MySQL, real Flyway migrations, real conditional updates.
 *
 * <p>The Spring-managed worker and recovery scanner are disabled for every test (see
 * {@code src/test/resources/application.properties}); tests that need execution or recovery start
 * their own engine with a fast polling interval and stop it at the end of the test. That way no
 * leftover worker from another test class can pick up tasks of the current test.
 */
@SpringBootTest
@AutoConfigureMockMvc
public abstract class AbstractIntegrationTest {

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MySQLSupport::jdbcUrl);
        registry.add("spring.datasource.username", MySQLSupport::username);
        registry.add("spring.datasource.password", MySQLSupport::password);
    }

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected TaskService taskService;

    @Autowired
    protected TaskOperations taskOperations;

    @Autowired
    protected TaskHandlerRegistry taskHandlerRegistry;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected Clock clock;

    @Autowired
    protected RetryProperties retryProperties;

    @Autowired
    protected LeaseRecoveryService leaseRecoveryService;

    @Autowired
    protected RuntimeSignals runtimeSignals;

    @Autowired
    protected SchedulerDecisionRecorder schedulerDecisionRecorder;

    private final List<TaskWorkerEngine> startedWorkers = new ArrayList<>();

    private final List<LeaseRecoveryScanner> startedScanners = new ArrayList<>();

    @BeforeEach
    void resetTaskTables() {
        jdbcTemplate.execute("DELETE FROM task_execution");
        jdbcTemplate.execute("DELETE FROM task");
        jdbcTemplate.execute("DELETE FROM scheduler_decision");
        jdbcTemplate.execute("DELETE FROM experiment_run");
    }

    @AfterEach
    void stopStartedEngines() {
        startedScanners.forEach(LeaseRecoveryScanner::stop);
        startedScanners.clear();
        startedWorkers.forEach(TaskWorkerEngine::stop);
        startedWorkers.clear();
    }

    /**
     * Worker settings for tests: fast polling, short lease and a heartbeat that keeps a running task
     * alive while the test watches it.
     */
    protected TaskWorkerProperties workerProperties(int concurrency) {
        TaskWorkerProperties properties = new TaskWorkerProperties();
        properties.setConcurrency(concurrency);
        properties.setBatchSize(concurrency);
        properties.setPollInterval(Duration.ofMillis(25));
        properties.setLeaseTtl(Duration.ofSeconds(30));
        properties.setLeaseRenewInterval(Duration.ofSeconds(1));
        return properties;
    }

    /** Retry settings for tests: explicit values instead of mutating the shared configuration bean. */
    protected RetryProperties retryWith(Duration baseDelay, double jitter) {
        RetryProperties properties = new RetryProperties();
        properties.setBaseDelay(baseDelay);
        properties.setMultiplier(2.0);
        properties.setMaxDelay(Duration.ofMinutes(5));
        properties.setJitter(jitter);
        properties.setMaxDispatchesPerSecond(20);
        return properties;
    }

    /** Starts a test worker that polls quickly and is stopped after the test. */
    protected TaskWorkerEngine startWorker(int concurrency) {
        return startWorker(workerProperties(concurrency));
    }

    protected TaskWorkerEngine startWorker(TaskWorkerProperties properties) {
        return startWorker(properties, retryProperties);
    }

    protected TaskWorkerEngine startWorker(TaskWorkerProperties properties, RetryProperties retry) {
        return startWorker(properties, retry, null);
    }

    /**
     * Starts a worker with an explicit concurrency limit. When {@code limit} is null the worker
     * keeps its own fixed limit and records its observations in its private signal window; tests
     * that want the controller to see those observations pass the shared {@link #runtimeSignals}.
     */
    protected TaskWorkerEngine startWorker(TaskWorkerProperties properties, RetryProperties retry,
                                           ConcurrencyLimit limit) {
        TaskWorkerEngine engine = new TaskWorkerEngine(taskOperations, taskHandlerRegistry, properties,
                retry, runtimeSignals, limit, objectMapper, clock, null, schedulerDecisionRecorder);
        engine.start();
        startedWorkers.add(engine);
        return engine;
    }

    /** Starts the scheduled recovery scanner with a fast interval; stopped after the test. */
    protected LeaseRecoveryScanner startRecoveryScanner() {
        RecoveryProperties properties = new RecoveryProperties();
        properties.setScanInterval(Duration.ofMillis(100));
        LeaseRecoveryScanner scanner = new LeaseRecoveryScanner(leaseRecoveryService, properties);
        scanner.start();
        startedScanners.add(scanner);
        return scanner;
    }

    /** One deterministic recovery pass, without any background scheduling. */
    protected RecoveryScanResult scanOnce() {
        return leaseRecoveryService.scanOnce();
    }

    protected TaskSubmission submit(String taskType, String payloadJson) {
        return submit(taskType, payloadJson, null, null);
    }

    protected TaskSubmission submit(String taskType, String payloadJson, String scope, String key) {
        return taskService.submit(new SubmitTaskCommand(taskType, payloadJson, null, null, null, scope, key));
    }

    protected TaskSubmission submit(String taskType, String payloadJson, int maxAttempts) {
        return taskService.submit(new SubmitTaskCommand(taskType, payloadJson, null, maxAttempts, null, null, null));
    }

    protected TaskSubmission submit(String taskType, String payloadJson, int maxAttempts, long timeoutMs) {
        return taskService.submit(
                new SubmitTaskCommand(taskType, payloadJson, null, maxAttempts, timeoutMs, null, null));
    }

    protected TaskView task(long taskId) {
        return taskService.get(taskId);
    }

    protected List<TaskExecutionView> executions(long taskId) {
        return taskService.executions(taskId);
    }

    protected long countExecutions(long taskId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM task_execution WHERE task_id = ?", Long.class, taskId);
        return count == null ? 0L : count;
    }

    protected long countTasks(String scope, String key) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM task WHERE idempotency_scope = ? AND idempotency_key = ?",
                Long.class, scope, key);
        return count == null ? 0L : count;
    }

    protected void awaitStatus(long taskId, TaskStatus expected, Duration timeout) {
        awaitUntil(() -> task(taskId).status() == expected, timeout);
    }

    /** Waits for a condition with the polling interval used across the integration tests. */
    protected static void awaitUntil(Callable<Boolean> condition, Duration timeout) {
        Awaitility.await()
                .pollInterval(Duration.ofMillis(50))
                .atMost(timeout)
                .until(condition);
    }
}
