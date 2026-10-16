package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 5 (AGENTS 9): the negative paths of the HTTP contract in one place - malformed bodies,
 * out-of-range fields, unknown resources, illegal state transitions and invalid query windows,
 * each mapped onto the documented 400/404/409 shape.
 */
class ExceptionPathsIT extends AbstractIntegrationTest {

    @Test
    void rejectsMalformedAndIncompleteSubmissions() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON).content("{\"taskType\": \"echo\","))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        // taskType is mandatory: a missing field is a bad request, an unregistered value is
        // rejected by the handler registry (covered by TaskApiIT.rejectsUnknownTaskType).
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON).content("{\"payload\":{}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        // payload is data only: a JSON array is not an object (PRD 8.4).
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON).content("{\"taskType\":\"echo\",\"payload\":[1,2]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }

    @Test
    void rejectsOutOfRangeSubmissionFields() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"priority\":1001}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"maxAttempts\":0}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"timeoutMs\":50}"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"idempotencyScope\":\"" + "x".repeat(65) + "\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownTasksAcrossEveryReadEndpoint() throws Exception {
        long unknown = 999_999_999L;
        mockMvc.perform(get("/api/v1/tasks/{id}", unknown))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));
        mockMvc.perform(get("/api/v1/tasks/{id}/executions", unknown))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));
        mockMvc.perform(get("/api/v1/tasks/{id}/timeline", unknown))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));
    }

    @Test
    void rejectsCancelOfARunningTask() throws Exception {
        long taskId = submit("echo", "{\"sleepMs\":5000}").task().id();
        startWorker(1);
        awaitStatus(taskId, TaskStatus.RUNNING, Duration.ofSeconds(10));

        // RUNNING -> CANCELLED is not a legal automatic transition (PRD 4.2).
        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .with(httpBasic("admin", "admin-test"))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"too late\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ILLEGAL_TASK_TRANSITION"));
    }

    @Test
    void rejectsInvalidObservabilityQueries() throws Exception {
        mockMvc.perform(get("/api/v1/decisions").param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mockMvc.perform(get("/api/v1/decisions").param("size", "1000"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/decisions").param("type", "NOT_A_DECISION_TYPE"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/decisions")
                        .param("from", "2030-01-02T00:00:00")
                        .param("to", "2030-01-01T00:00:00"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/metrics/summary").param("from", "not-a-timestamp"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/metrics/summary")
                        .param("from", "2030-01-02T00:00:00")
                        .param("to", "2030-01-01T00:00:00"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/experiments").param("size", "0"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsInvalidExperimentRecords() throws Exception {
        // A recorded round must carry its identity and its JSON snapshots as objects (PRD 7.4).
        mockMvc.perform(post("/api/v1/experiments")
                        .contentType(APPLICATION_JSON)
                        .content("{\"roundIndex\":1,\"strategy\":\"FIXED\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        mockMvc.perform(post("/api/v1/experiments")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"experimentName":"exception-paths","roundIndex":0,"strategy":"FIXED",
                                 "taskCount":1,"concurrencyConfig":{},"faultConfig":{},
                                 "startedAt":"2030-01-01T00:00:00","finishedAt":"2030-01-01T00:01:00",
                                 "environment":{},"resultSummary":{}}
                                """))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/api/v1/experiments")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"experimentName":"exception-paths","roundIndex":1,"strategy":"FIXED",
                                 "taskCount":1,"concurrencyConfig":[1,2],"faultConfig":{},
                                 "startedAt":"2030-01-01T00:00:00","finishedAt":"2030-01-01T00:01:00",
                                 "environment":{},"resultSummary":{}}
                                """))
                .andExpect(status().isBadRequest());

        mockMvc.perform(post("/api/v1/experiments")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"experimentName":"exception-paths","roundIndex":1,"strategy":"FIXED",
                                 "taskCount":1,"concurrencyConfig":{},"faultConfig":{},
                                 "startedAt":"2030-01-01T00:01:00","finishedAt":"2030-01-01T00:00:00",
                                 "environment":{},"resultSummary":{}}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void recordsAValidExperimentRound() throws Exception {
        mockMvc.perform(post("/api/v1/experiments")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"experimentName":"exception-paths","roundIndex":1,"strategy":"FIXED",
                                 "taskCount":1,"concurrencyConfig":{"concurrency":4},"faultConfig":{},
                                 "startedAt":"2030-01-01T00:00:00","finishedAt":"2030-01-01T00:01:00",
                                 "environment":{"host":"test"},"resultSummary":{"succeeded":1}}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.roundIndex").value(1))
                .andExpect(jsonPath("$.strategy").value("FIXED"));
    }
}
