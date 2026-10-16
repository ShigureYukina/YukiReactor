package com.yuki.reactor.app.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;
import org.springframework.stereotype.Component;

/**
 * Demonstration task type {@code echo}.
 *
 * <p>It exists to exercise the engine end to end and to show how a task type is added: implement
 * {@link TaskHandler} and register it as a Spring bean. Payload fields:
 *
 * <ul>
 *     <li>{@code sleepMs} - simulated work duration, default 0</li>
 *     <li>{@code fail} - when {@code true} the attempt throws, default {@code false}</li>
 * </ul>
 */
@Component
public class EchoTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "echo";

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        JsonNode payload = context.payload();
        long sleepMs = payload.path("sleepMs").asLong(0L);
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        if (payload.path("fail").asBoolean(false)) {
            throw new IllegalStateException("echo handler was asked to fail");
        }
    }
}
