package com.yuki.reactor.app.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Test only task type that models a business side effect outside the engine's control (PRD 5.3: the
 * lease and the fencing token cannot undo what already happened).
 *
 * <p>Every invocation is recorded in {@code it_side_effect_attempt}, while the business effect lands
 * in {@code it_business_effect} behind a unique business key, so a repeated execution must not
 * produce a second effect. With {@code crashAfterSideEffect} the first attempt performs the effect
 * and then dies before it can report a result - the fault window PRD 9 asks for.
 */
public class SideEffectTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "side-effect";

    private final JdbcTemplate jdbcTemplate;

    public SideEffectTaskHandler(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        JsonNode payload = context.payload();
        String businessKey = payload.path("businessKey").asText("task-" + context.taskId());

        jdbcTemplate.update(
                "INSERT INTO it_side_effect_attempt (task_id, attempt, created_at) VALUES (?, ?, CURRENT_TIMESTAMP(3))",
                context.taskId(), context.attempt());
        jdbcTemplate.update(
                "INSERT IGNORE INTO it_business_effect (business_key, note, created_at) VALUES (?, ?, CURRENT_TIMESTAMP(3))",
                businessKey, "attempt " + context.attempt());

        if (payload.path("crashAfterSideEffect").asBoolean(false) && context.attempt() == 1) {
            throw new SideEffectCrashSimulation(
                    "executor lost after the side effect, before any result was reported");
        }

        long sleepMs = payload.path("sleepMs").asLong(0L);
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
    }

    /** Signals that the attempt stopped without reporting anything, like a killed executor. */
    public static class SideEffectCrashSimulation extends RuntimeException {

        public SideEffectCrashSimulation(String message) {
            super(message);
        }
    }
}
