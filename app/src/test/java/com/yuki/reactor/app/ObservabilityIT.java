package com.yuki.reactor.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.yuki.reactor.app.task.ScenarioTaskHandler;
import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.observability.domain.ExperimentStrategy;
import com.yuki.reactor.observability.dto.DecisionPage;
import com.yuki.reactor.observability.dto.DecisionQuery;
import com.yuki.reactor.observability.dto.DecisionView;
import com.yuki.reactor.observability.dto.ExperimentRounds;
import com.yuki.reactor.observability.dto.ExperimentRunPage;
import com.yuki.reactor.observability.dto.ExperimentRunRequest;
import com.yuki.reactor.observability.dto.ExperimentRunView;
import com.yuki.reactor.observability.dto.MetricsSummary;
import com.yuki.reactor.observability.dto.TaskTimeline;
import com.yuki.reactor.observability.dto.TimelineEvent;
import com.yuki.reactor.observability.dto.TimelineEventType;
import com.yuki.reactor.observability.exception.DuplicateExperimentRoundException;
import com.yuki.reactor.observability.service.DecisionQueryService;
import com.yuki.reactor.observability.service.ExperimentRunService;
import com.yuki.reactor.observability.service.MetricsSummaryService;
import com.yuki.reactor.observability.service.TaskTimelineService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The phase four deliverables against a real MySQL: a task timeline built from the task, its attempts
 * and the decisions that were really recorded for it; a decision query over the same rows; metrics
 * aggregated from the stored rows; and experiment rounds that keep every round of both strategies
 * instead of the best looking one (PRD 10.1).
 *
 * <p>The HTTP surface is exercised through MockMvc, so the panel and the JSON API are covered by the
 * same assertions a browser or the experiment script would rely on.
 */
@Import(TestHandlersConfiguration.class)
class ObservabilityIT extends AbstractIntegrationTest {

    @Autowired
    private TaskTimelineService timelineService;

    @Autowired
    private DecisionQueryService decisionQueryService;

    @Autowired
    private MetricsSummaryService metricsService;

    @Autowired
    private ExperimentRunService experimentRunService;

    @Test
    void explainsARetriedTaskAsOneChronologicalTimeline() {
        startWorker(workerProperties(1));
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        TaskTimeline timeline = timelineService.timeline(taskId);

        assertThat(timeline.task().id()).isEqualTo(taskId);
        assertThat(timeline.task().status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(timeline.executions()).hasSize(2);

        long firstAttemptId = timeline.executions().stream()
                .mapToLong(TaskExecutionView::id)
                .min()
                .orElseThrow();
        DecisionView rescheduled = timeline.decisions().stream()
                .filter(decision -> decision.decisionType() == DecisionType.RETRY_SCHEDULED)
                .findFirst()
                .orElseThrow();
        assertThat(rescheduled.reasonCode()).isEqualTo(ReasonCodes.RETRY_BACKOFF);
        assertThat(rescheduled.executionId())
                .as("the decision points at the attempt it explains")
                .isEqualTo(firstAttemptId);

        assertThat(timeline.events()).isSortedAccordingTo(Comparator.comparing(TimelineEvent::at));
        assertThat(timeline.events()).extracting(TimelineEvent::type).containsSubsequence(
                TimelineEventType.TASK_CREATED,
                TimelineEventType.ATTEMPT_STARTED,
                TimelineEventType.ATTEMPT_FINISHED,
                TimelineEventType.DECISION,
                TimelineEventType.ATTEMPT_STARTED,
                TimelineEventType.ATTEMPT_FINISHED);
        assertThat(timeline.events().get(0).type()).isEqualTo(TimelineEventType.TASK_CREATED);
        assertThat(timeline.events().get(0).details()).contains("taskType=scenario");

        TimelineEvent decisionEvent = timeline.events().stream()
                .filter(event -> event.type() == TimelineEventType.DECISION)
                .findFirst()
                .orElseThrow();
        assertThat(decisionEvent.decisionType()).isEqualTo(DecisionType.RETRY_SCHEDULED);
        assertThat(decisionEvent.reasonCode()).isEqualTo(ReasonCodes.RETRY_BACKOFF);
        assertThat(decisionEvent.executionId()).isNotNull();

        TimelineEvent finished = timeline.events().stream()
                .filter(event -> event.type() == TimelineEventType.ATTEMPT_FINISHED)
                .findFirst()
                .orElseThrow();
        assertThat(finished.reasonCode())
                .as("an attempt outcome carries no reason code; the execution error fields explain it")
                .isNull();
        assertThat(finished.details()).contains("result=FAILED").contains("TRANSIENT");
    }

    @Test
    void queriesDecisionsByTaskTypeAndReasonCode() {
        startWorker(workerProperties(1));
        long firstTaskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();
        long secondTaskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();
        awaitStatus(firstTaskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        awaitStatus(secondTaskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        DecisionPage forTask = decisionQueryService.query(
                new DecisionQuery(firstTaskId, null, null, null, null, 0, 20));
        assertThat(forTask.items()).isNotEmpty();
        assertThat(forTask.items())
                .as("the task filter returns the decisions of that task and nothing else")
                .extracting(DecisionView::taskId)
                .containsOnly(firstTaskId);

        DecisionPage rescheduled = decisionQueryService.query(
                new DecisionQuery(firstTaskId, DecisionType.RETRY_SCHEDULED, null, null, null, 0, 20));
        assertThat(rescheduled.total())
                .as("exactly one attempt of this task was rescheduled")
                .isEqualTo(1);
        assertThat(rescheduled.items().get(0).reasonCode()).isEqualTo(ReasonCodes.RETRY_BACKOFF);

        DecisionPage forType = decisionQueryService.query(
                new DecisionQuery(null, DecisionType.RETRY_SCHEDULED, null, null, null, 0, 20));
        assertThat(forType.total()).isEqualTo(2);

        DecisionPage forReason = decisionQueryService.query(
                new DecisionQuery(null, null, ReasonCodes.RETRY_BACKOFF, null, null, 0, 20));
        assertThat(forReason.total()).isEqualTo(2);

        DecisionPage unknownReason = decisionQueryService.query(
                new DecisionQuery(null, null, "NO_SUCH_REASON", null, null, 0, 20));
        assertThat(unknownReason.total()).isZero();
        assertThat(unknownReason.items()).isEmpty();
    }

    @Test
    void rejectsInvalidPagingAndInvertedWindows() {
        assertThatThrownBy(() -> decisionQueryService.query(
                new DecisionQuery(null, null, null, null, null, -1, 20)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decisionQueryService.query(
                new DecisionQuery(null, null, null, null, null, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decisionQueryService.query(
                new DecisionQuery(null, null, null, LocalDateTime.now(clock), LocalDateTime.now(clock), 0, 20)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aggregatesMetricsFromTheStoredRowsOfItsWindow() {
        LocalDateTime before = LocalDateTime.now(clock).minusSeconds(1);
        startWorker(workerProperties(2));
        long firstTaskId = submit(ScenarioTaskHandler.TASK_TYPE, "{}").task().id();
        long secondTaskId = submit("echo", "{}").task().id();
        awaitStatus(firstTaskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        awaitStatus(secondTaskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        LocalDateTime after = LocalDateTime.now(clock).plusSeconds(1);

        MetricsSummary summary = metricsService.summary(before, after);

        assertThat(summary.from()).isEqualTo(before);
        assertThat(summary.to()).isEqualTo(after);
        assertThat(summary.submittedTasks()).isEqualTo(2);
        assertThat(summary.succeededTasks()).isEqualTo(2);
        assertThat(summary.completedTasks()).isEqualTo(2);
        assertThat(summary.failedTasks()).isZero();
        assertThat(summary.cancelledTasks()).isZero();
        assertThat(summary.failureRate()).isZero();
        assertThat(summary.throughputPerSecond()).isGreaterThan(0.0);
        assertThat(summary.executionAttempts()).isEqualTo(2);
        assertThat(summary.avgAttemptsPerTask()).isEqualTo(1.0);
        assertThat(summary.avgQueueDelayMs()).isNotNull();
        assertThat(summary.avgExecutionDelayMs()).isNotNull();
        assertThat(summary.backlogNow()).isZero();
        assertThat(summary.runningNow()).isZero();
    }

    @Test
    void keepsEveryRoundOfBothStrategies() throws Exception {
        JsonNode concurrency = objectMapper.readTree("{\"concurrency\": 4}");
        JsonNode environment = objectMapper.readTree("{\"host\": \"it\", \"jdk\": \"21\"}");
        JsonNode fault = objectMapper.readTree("{\"downstreamDelayMs\": 300}");
        JsonNode result = objectMapper.readTree("{\"throughputPerSecond\": 12.5}");
        LocalDateTime startedAt = LocalDateTime.now(clock).minusMinutes(5);
        LocalDateTime finishedAt = LocalDateTime.now(clock);

        for (int round = 1; round <= 2; round++) {
            ExperimentRunView view = experimentRunService.record(request("phase4-it", round,
                    ExperimentStrategy.FIXED, 10, concurrency, fault, 42L, environment,
                    startedAt, finishedAt, result, null));
            assertThat(view.strategy()).isEqualTo("FIXED");
            assertThat(view.randomSeed()).isEqualTo(42L);
            assertThat(view.concurrencyConfig().get("concurrency").asInt()).isEqualTo(4);
            assertThat(view.resultSummary().get("throughputPerSecond").asDouble()).isEqualTo(12.5);
        }
        for (int round = 1; round <= 2; round++) {
            ExperimentRunView view = experimentRunService.record(request("phase4-it", round,
                    ExperimentStrategy.ADAPTIVE, 10, concurrency, fault, null, environment,
                    startedAt, finishedAt, null, "adaptive round " + round));
            assertThat(view.strategy()).isEqualTo("ADAPTIVE");
            assertThat(view.randomSeed()).isNull();
            assertThat(view.resultSummary())
                    .as("a round that reported no result yet stays unknown instead of being filled in")
                    .isNull();
            assertThat(view.notes()).isEqualTo("adaptive round " + round);
        }

        ExperimentRounds rounds = experimentRunService.rounds("phase4-it");
        assertThat(rounds.experimentName()).isEqualTo("phase4-it");
        assertThat(rounds.rounds()).hasSize(4);
        assertThat(rounds.rounds()).extracting(ExperimentRunView::strategy)
                .as("rounds are grouped by strategy, alphabetically, so both sides of the comparison are kept")
                .containsExactly("ADAPTIVE", "ADAPTIVE", "FIXED", "FIXED");
        assertThat(rounds.rounds()).extracting(ExperimentRunView::roundIndex)
                .containsExactly(1, 2, 1, 2);

        ExperimentRunView fixedRound1 = round(rounds, "FIXED", 1);
        assertThat(fixedRound1.faultConfig().get("downstreamDelayMs").asInt()).isEqualTo(300);
        assertThat(fixedRound1.environment().get("jdk").asText()).isEqualTo("21");
        assertThat(fixedRound1.notes())
                .as("a fixed round has no adaptive note")
                .isNull();
        assertThat(round(rounds, "ADAPTIVE", 1).resultSummary()).isNull();
        assertThat(round(rounds, "ADAPTIVE", 1).notes()).isEqualTo("adaptive round 1");

        ExperimentRunPage adaptiveOnly = experimentRunService.query(
                "phase4-it", ExperimentStrategy.ADAPTIVE, 0, 20);
        assertThat(adaptiveOnly.total()).isEqualTo(2);
        assertThat(adaptiveOnly.items()).extracting(ExperimentRunView::strategy).containsOnly("ADAPTIVE");

        assertThat(experimentRunService.rounds("no-such-experiment").rounds()).isEmpty();

        assertThatThrownBy(() -> experimentRunService.record(request("phase4-it", 1,
                ExperimentStrategy.FIXED, 10, concurrency, fault, 42L, environment,
                startedAt, finishedAt, result, null)))
                .as("the same round of the same strategy cannot be recorded twice")
                .isInstanceOf(DuplicateExperimentRoundException.class);
    }

    @Test
    void servesExperimentRoundsOverHttpAndRejectsDuplicates() throws Exception {
        JsonNode concurrency = objectMapper.readTree("{\"concurrency\": 4}");
        JsonNode environment = objectMapper.readTree("{\"host\": \"it\"}");
        String body = objectMapper.writeValueAsString(request("phase4-http", 1, ExperimentStrategy.FIXED, 5,
                concurrency, null, null, environment,
                LocalDateTime.now(clock).minusMinutes(1), LocalDateTime.now(clock), null, null));

        mockMvc.perform(post("/api/v1/experiments").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.experimentName").value("phase4-http"))
                .andExpect(jsonPath("$.strategy").value("FIXED"))
                .andExpect(jsonPath("$.roundIndex").value(1));

        mockMvc.perform(post("/api/v1/experiments").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("DUPLICATE_EXPERIMENT_ROUND"));

        mockMvc.perform(get("/api/v1/experiments").param("name", "phase4-http"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].strategy").value("FIXED"));

        mockMvc.perform(get("/api/v1/experiments/phase4-http"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rounds[0].roundIndex").value(1));
    }

    @Test
    void servesTheTimelineMetricsAndPanelOverHttp() throws Exception {
        startWorker(workerProperties(1));
        long taskId = submit(ScenarioTaskHandler.TASK_TYPE, "{\"failures\": 1}").task().id();
        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));

        mockMvc.perform(get("/api/v1/tasks/{id}/timeline", taskId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.task.id").value(taskId))
                .andExpect(jsonPath("$.executions.length()").value(2))
                .andExpect(jsonPath("$.decisions[*].reasonCode", hasItem(ReasonCodes.RETRY_BACKOFF)))
                .andExpect(jsonPath("$.events[0].type").value("TASK_CREATED"));

        mockMvc.perform(get("/api/v1/tasks/{id}/timeline", 99999999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));

        mockMvc.perform(get("/api/v1/decisions")
                        .param("taskId", String.valueOf(taskId))
                        .param("type", "RETRY_SCHEDULED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].decisionType").value("RETRY_SCHEDULED"))
                .andExpect(jsonPath("$.items[0].reasonCode").value(ReasonCodes.RETRY_BACKOFF));

        mockMvc.perform(get("/api/v1/metrics/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.submittedTasks").value(1))
                .andExpect(jsonPath("$.succeededTasks").value(1))
                .andExpect(jsonPath("$.completedTasks").value(1));

        mockMvc.perform(get("/panel/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("/api/v1/decisions")));
    }

    private static ExperimentRunView round(ExperimentRounds rounds, String strategy, int roundIndex) {
        return rounds.rounds().stream()
                .filter(view -> view.strategy().equals(strategy) && view.roundIndex() == roundIndex)
                .findFirst()
                .orElseThrow();
    }

    private static ExperimentRunRequest request(String experimentName, int roundIndex,
                                                ExperimentStrategy strategy, int taskCount,
                                                JsonNode concurrencyConfig, JsonNode faultConfig,
                                                Long randomSeed, JsonNode environment,
                                                LocalDateTime startedAt, LocalDateTime finishedAt,
                                                JsonNode resultSummary, String notes) {
        return new ExperimentRunRequest(experimentName, roundIndex, strategy, taskCount, concurrencyConfig,
                faultConfig, randomSeed, environment, startedAt, finishedAt, resultSummary, notes);
    }
}
