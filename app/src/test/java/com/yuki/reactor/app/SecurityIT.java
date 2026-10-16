package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.TaskStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 5 (PRD 8.4): the management endpoints - task cancel, task replay and fault injection -
 * require authentication and the ADMIN role. Everything else stays open so the panel, monitoring
 * and the experiment scripts keep working without credentials.
 */
class SecurityIT extends AbstractIntegrationTest {

    private static final String CANCEL_BODY = "{\"operator\":\"alice\",\"reason\":\"duplicate order\"}";

    @Test
    void rejectsManagementWritesWithoutCredentials() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .contentType(APPLICATION_JSON).content(CANCEL_BODY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));

        // The rejected write must leave the task untouched.
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.PENDING);
    }

    @Test
    void rejectsManagementWritesWithAWrongPassword() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .with(httpBasic("admin", "not-the-password"))
                        .contentType(APPLICATION_JSON).content(CANCEL_BODY))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    @Test
    void rejectsAuthenticatedUsersWithoutTheAdminRole() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .with(httpBasic("viewer", "viewer-test"))
                        .contentType(APPLICATION_JSON).content(CANCEL_BODY))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));

        mockMvc.perform(put("/api/v1/fault-lab/downstream")
                        .with(httpBasic("viewer", "viewer-test"))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\": \"viewer\", \"latencyMs\": 10}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    void letsTheAdminCancelAndReplay() throws Exception {
        long taskId = submit("echo", "{}").task().id();

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", taskId)
                        .with(httpBasic("admin", "admin-test"))
                        .contentType(APPLICATION_JSON).content(CANCEL_BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        long failedId = submit("echo", "{\"fail\":true}", 1).task().id();
        startWorker(1);
        awaitStatus(failedId, TaskStatus.FAILED, Duration.ofSeconds(30));

        mockMvc.perform(post("/api/v1/tasks/{id}/replay", failedId)
                        .with(httpBasic("admin", "admin-test"))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\":\"alice\",\"reason\":\"retry please\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void letsTheAdminInjectFaults() throws Exception {
        mockMvc.perform(put("/api/v1/fault-lab/downstream")
                        .with(httpBasic("admin", "admin-test"))
                        .contentType(APPLICATION_JSON)
                        .content("{\"operator\": \"alice\", \"latencyMs\": 321}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.latencyMs").value(321));
    }

    @Test
    void keepsReadEndpointsOpen() throws Exception {
        mockMvc.perform(get("/api/v1/tasks")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/decisions")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/metrics/summary")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/experiments")).andExpect(status().isOk());
        mockMvc.perform(get("/api/v1/fault-lab/downstream")).andExpect(status().isOk());
        mockMvc.perform(get("/panel/index.html")).andExpect(status().isOk());
    }
}
