package com.yuki.reactor.app;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP contract of the task API (PRD 5.1): submission semantics, error codes and filters.
 */
class TaskApiIT extends AbstractIntegrationTest {

    @Test
    void createsTaskAndReturnsCreated() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"taskType":"echo","payload":{"sleepMs":0},"priority":5,
                                 "idempotencyScope":"api","idempotencyKey":"api-key-1"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.idempotentReplay").value(false))
                .andExpect(jsonPath("$.task.status").value("PENDING"))
                .andExpect(jsonPath("$.task.priority").value(5))
                .andExpect(jsonPath("$.task.payload.sleepMs").value(0))
                .andExpect(jsonPath("$.task.attemptCount").value(0))
                .andExpect(jsonPath("$.task.maxAttempts").value(3));
    }

    @Test
    void repeatedSubmissionWithTheSameKeyReturnsTheExistingTask() throws Exception {
        String body = """
                {"taskType":"echo","payload":{},"idempotencyScope":"api","idempotencyKey":"api-key-2"}
                """;
        String first = mockMvc.perform(post("/api/v1/tasks").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long firstId = taskId(first);

        mockMvc.perform(post("/api/v1/tasks").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.idempotentReplay").value(true))
                .andExpect(jsonPath("$.task.id").value(firstId));
    }

    @Test
    void rejectsUnknownTaskType() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"not-registered\",\"payload\":{}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("UNKNOWN_TASK_TYPE"));
    }

    @Test
    void rejectsPayloadThatIsNotAJsonObject() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"payload\":42}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void rejectsOutOfRangeParameters() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"payload\":{},\"priority\":5000}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"payload\":{},\"maxAttempts\":0}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returnsNotFoundForUnknownTask() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/{id}", 999_999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));
    }

    @Test
    void filtersByStatusAndCreationTime() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(get("/api/v1/tasks").param("status", "PENDING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].id").value(taskId));

        mockMvc.perform(get("/api/v1/tasks").param("status", "SUCCEEDED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0));

        String past = LocalDateTime.now(clock).minusHours(1).toString();
        String future = LocalDateTime.now(clock).plusHours(1).toString();
        mockMvc.perform(get("/api/v1/tasks").param("from", past))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1));
        mockMvc.perform(get("/api/v1/tasks").param("from", future))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(0));
        mockMvc.perform(get("/api/v1/tasks").param("from", past).param("to", future))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1));
    }

    @Test
    void rejectsInvalidFilterAndPageParameters() throws Exception {
        mockMvc.perform(get("/api/v1/tasks").param("status", "NOT_A_STATUS"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mockMvc.perform(get("/api/v1/tasks").param("page", "-1"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/tasks").param("size", "1000"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void cancelsTaskAndRejectsSecondCancellation() throws Exception {
        long taskId = submit("echo", "{}").task().id();
        String body = "{\"operator\":\"alice\",\"reason\":\"duplicate order\"}";

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.lastError").value("cancelled by alice: duplicate order"));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ILLEGAL_TASK_TRANSITION"));
    }

    @Test
    void requiresOperatorAndReason() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .contentType(APPLICATION_JSON).content("{\"reason\":\"no operator\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        mockMvc.perform(post("/api/v1/tasks/{id}/replay", taskId)
                        .contentType(APPLICATION_JSON).content("{\"operator\":\"alice\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsReplayOfATaskThatDidNotFail() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/replay", taskId)
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"retry please\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ILLEGAL_TASK_TRANSITION"));
    }

    @Test
    void exposesExecutionHistory() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(get("/api/v1/tasks/{id}/executions", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));

        mockMvc.perform(get("/api/v1/tasks/{id}/executions", 999_999L))
                .andExpect(status().isNotFound());
    }

    private long taskId(String submitResponse) throws Exception {
        JsonNode node = objectMapper.readTree(submitResponse);
        assertThat(node.path("task").path("id").isNumber()).isTrue();
        return node.path("task").path("id").asLong();
    }
}
