package com.yuki.reactor.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.handler.TaskHandlerRegistry;
import com.yuki.reactor.core.service.SubmitTaskCommand;
import com.yuki.reactor.core.service.TaskOperations;
import com.yuki.reactor.core.service.TaskService;
import com.yuki.reactor.core.service.TaskSubmission;
import com.yuki.reactor.worker.TaskWorkerEngine;
import com.yuki.reactor.worker.TaskWorkerProperties;
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

/**
 * Base class for integration tests: real MySQL, real Flyway migrations, real conditional updates.
 *
 * <p>The Spring-managed worker is disabled for every test (see
 * {@code src/test/resources/application.properties}); tests that need execution start their own
 * {@link TaskWorkerEngine} with a fast polling interval and stop it at the end of the test. That way
 * no leftover worker from another test class can pick up tasks of the current test.
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

    private final List<TaskWorkerEngine> startedWorkers = new ArrayList<>();

    @BeforeEach
    void resetTaskTables() {
        jdbcTemplate.execute("DELETE FROM task_execution");
        jdbcTemplate.execute("DELETE FROM task");
    }

    @AfterEach
    void stopStartedWorkers() {
        startedWorkers.forEach(TaskWorkerEngine::stop);
        startedWorkers.clear();
    }

    /** Starts a test worker that polls quickly and is stopped after the test. */
    protected TaskWorkerEngine startWorker(int concurrency) {
        TaskWorkerProperties properties = new TaskWorkerProperties();
        properties.setConcurrency(concurrency);
        properties.setBatchSize(concurrency);
        properties.setPollInterval(Duration.ofMillis(25));
        properties.setLeaseTtl(Duration.ofSeconds(30));
        TaskWorkerEngine engine = new TaskWorkerEngine(taskOperations, taskHandlerRegistry, properties, objectMapper, clock);
        engine.start();
        startedWorkers.add(engine);
        return engine;
    }

    protected TaskSubmission submit(String taskType, String payloadJson) {
        return submit(taskType, payloadJson, null, null);
    }

    protected TaskSubmission submit(String taskType, String payloadJson, String scope, String key) {
        return taskService.submit(new SubmitTaskCommand(taskType, payloadJson, null, null, null, scope, key));
    }

    protected TaskView task(long taskId) {
        return taskService.get(taskId);
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
}
