package com.yuki.reactor.core.handler;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Read-only execution context handed to a {@link TaskHandler}.
 *
 * @param taskId    identifier of the logical task
 * @param attempt   attempt number of this execution, starting at 1
 * @param taskType  registered task type
 * @param payload   task parameters as a JSON object; treated as data only
 */
public record TaskContext(long taskId, int attempt, String taskType, JsonNode payload) {
}
