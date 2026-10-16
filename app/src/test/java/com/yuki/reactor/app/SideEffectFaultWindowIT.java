package com.yuki.reactor.app;

import com.yuki.reactor.app.task.SideEffectTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;
import com.yuki.reactor.core.service.TaskClaim;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The fault window required by PRD 9: the business side effect has already happened, but the attempt
 * died before it could write its result. Recovery replays the task, so the handler runs twice, and the
 * duplicate effect must be suppressed by the idempotency of the side effect itself - not by the
 * exactly-once claim of the engine.
 */
@Import(TestHandlersConfiguration.class)
class SideEffectFaultWindowIT extends AbstractIntegrationTest {

    @BeforeEach
    void prepareSideEffectTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS it_business_effect (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    business_key VARCHAR(128) NOT NULL,
                    note VARCHAR(128) NULL,
                    created_at DATETIME(3) NOT NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_business_effect_key (business_key)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
                """);
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS it_side_effect_attempt (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    task_id BIGINT NOT NULL,
                    attempt INT NOT NULL,
                    created_at DATETIME(3) NOT NULL,
                    PRIMARY KEY (id)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
                """);
        jdbcTemplate.update("DELETE FROM it_business_effect");
        jdbcTemplate.update("DELETE FROM it_side_effect_attempt");
    }

    @Test
    void aReplayedAttemptDoesNotDuplicateTheBusinessEffect() throws Exception {
        long taskId = submit("side-effect", "{\"businessKey\": \"order-42\", \"crashAfterSideEffect\": true}")
                .task().id();

        TaskClaim claim = taskOperations.claimAndOpen(
                taskId, "worker-crashed", LocalDateTime.now(clock), Duration.ofSeconds(1));
        assertThat(claim).isNotNull();

        TaskHandler handler = taskHandlerRegistry.require(SideEffectTaskHandler.TASK_TYPE);
        TaskContext context = new TaskContext(taskId, claim.task().getAttemptCount(),
                SideEffectTaskHandler.TASK_TYPE, objectMapper.readTree(claim.task().getPayload()));
        assertThatThrownBy(() -> handler.execute(context))
                .isInstanceOf(SideEffectTaskHandler.SideEffectCrashSimulation.class);

        assertThat(count("it_side_effect_attempt")).isEqualTo(1);
        assertThat(count("it_business_effect")).isEqualTo(1);
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING);

        Thread.sleep(1200L);
        assertThat(scanOnce().reclaimedForRetry()).isEqualTo(1);

        startWorker(1);
        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        assertThat(count("it_side_effect_attempt")).as("the handler really ran twice").isEqualTo(2);
        assertThat(count("it_business_effect")).as("but the effect stayed unique").isEqualTo(1);
        assertThat(executions(taskId)).hasSize(2);
    }

    private long count(String table) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return count == null ? 0L : count;
    }
}
