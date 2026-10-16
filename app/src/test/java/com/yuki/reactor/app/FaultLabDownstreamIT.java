package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.error.ErrorCategory;
import com.yuki.reactor.faultlab.DownstreamFailureKind;
import com.yuki.reactor.faultlab.MockDownstream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The simulated downstream has to behave like a real one: controllable at runtime, mapped onto the
 * engine's failure classification, and recoverable without touching the tasks.
 */
class FaultLabDownstreamIT extends AbstractIntegrationTest {

    @Autowired
    private MockDownstream downstream;

    @Test
    void mapsDownstreamFailuresOntoTheRetryClassification() {
        downstream.configure(10L, 1.0, DownstreamFailureKind.OVERLOADED, null, null, null);
        startWorker(1);

        long taskId = submit("downstream", "{}", 2).task().id();
        awaitStatus(taskId, TaskStatus.FAILED, Duration.ofSeconds(30));

        List<TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts).hasSize(2);
        assertThat(attempts).extracting(TaskExecutionView::errorCategory)
                .containsExactly(ErrorCategory.OVERLOADED, ErrorCategory.OVERLOADED);
        assertThat(task(taskId).lastError()).contains("OVERLOADED");
    }

    @Test
    void honoursTheRetryAfterOfAThrottledDownstream() {
        downstream.configure(10L, 1.0, DownstreamFailureKind.THROTTLED, 3000L, null, null);
        startWorker(workerProperties(1), retryWith(Duration.ofMillis(50), 0.0));

        long taskId = submit("downstream", "{}", 3).task().id();
        awaitStatus(taskId, TaskStatus.RETRY_WAIT, Duration.ofSeconds(30));

        TaskView waiting = task(taskId);
        assertThat(waiting.nextRunAt()).isNotNull();
        assertThat(Duration.between(now(), waiting.nextRunAt())).isGreaterThan(Duration.ofSeconds(2));
        assertThat(executions(taskId)).singleElement()
                .satisfies(attempt -> assertThat(attempt.errorCategory()).isEqualTo(ErrorCategory.THROTTLED));
    }

    @Test
    void retriesSuccessfullyAfterTheDownstreamRecovers() {
        downstream.configure(10L, 1.0, DownstreamFailureKind.TRANSIENT, null, null, null);
        startWorker(workerProperties(1), retryWith(Duration.ofMillis(200), 0.0));

        long taskId = submit("downstream", "{}", 4).task().id();
        awaitStatus(taskId, TaskStatus.RETRY_WAIT, Duration.ofSeconds(30));

        downstream.configure(10L, 0.0, DownstreamFailureKind.TRANSIENT, null, null, null);
        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        List<TaskExecutionView> attempts = executions(taskId);
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0).errorCategory()).isEqualTo(ErrorCategory.TRANSIENT);
        assertThat(attempts.get(0).result()).isEqualTo(com.yuki.reactor.core.domain.ExecutionResult.FAILED);
        assertThat(attempts.get(1).result()).isEqualTo(com.yuki.reactor.core.domain.ExecutionResult.SUCCEEDED);
        assertThat(attempts.get(1).errorCategory()).isNull();
    }

    @Test
    void exposesTheDownstreamConfigurationThroughTheApi() throws Exception {
        mockMvc.perform(put("/api/v1/fault-lab/downstream")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"operator\": \"alice\", \"latencyMs\": 123, \"errorRate\": 0.25}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.latencyMs").value(123))
                .andExpect(jsonPath("$.errorRate").value(0.25));

        mockMvc.perform(put("/api/v1/fault-lab/downstream")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"latencyMs\": 50}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        mockMvc.perform(post("/api/v1/fault-lab/downstream/call"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.latencyMillis").isNumber());
    }

    private java.time.LocalDateTime now() {
        return java.time.LocalDateTime.now(clock);
    }
}
