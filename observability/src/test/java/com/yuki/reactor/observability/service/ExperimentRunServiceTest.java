package com.yuki.reactor.observability.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.observability.domain.ExperimentRun;
import com.yuki.reactor.observability.domain.ExperimentStrategy;
import com.yuki.reactor.observability.dto.ExperimentRunPage;
import com.yuki.reactor.observability.dto.ExperimentRunRequest;
import com.yuki.reactor.observability.dto.ExperimentRunView;
import com.yuki.reactor.observability.dto.ExperimentRounds;
import com.yuki.reactor.observability.exception.DuplicateExperimentRoundException;
import com.yuki.reactor.observability.mapper.ExperimentRunMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A recorded round is evidence for PRD 10.1, so the service must store exactly what it was given,
 * reject a round that is recorded twice, and refuse an incomplete round rather than inventing the
 * missing configuration.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ExperimentRunServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-16T10:00:00Z");
    private static final LocalDateTime STARTED = LocalDateTime.of(2026, 10, 16, 9, 0);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private ExperimentRunMapper experimentRunMapper;

    private ExperimentRunService service() {
        return new ExperimentRunService(experimentRunMapper, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    /** The generated key comes back from the insert in production; the view needs it to be present. */
    @BeforeEach
    void stubInsertAssignsTheGeneratedId() {
        when(experimentRunMapper.insert(any(ExperimentRun.class))).thenAnswer(invocation -> {
            ExperimentRun run = invocation.getArgument(0);
            run.setId(11L);
            return 1;
        });
    }

    private JsonNode json(String text) {
        try {
            return objectMapper.readTree(text);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ExperimentRunRequest validRequest() {
        return new ExperimentRunRequest(
                "phase-4-comparison", 1, ExperimentStrategy.FIXED, 30,
                json("{\"concurrency\":4}"), json("{\"mode\":\"none\"}"), 20261016L,
                json("{\"host\":\"test-box\"}"), STARTED, STARTED.plusSeconds(60),
                json("{\"succeededTasks\":30}"), "round 1");
    }

    @Test
    void storesTheRoundExactlyAsItWasReported() {
        ArgumentCaptor<ExperimentRun> captor = ArgumentCaptor.forClass(ExperimentRun.class);

        ExperimentRunView view = service().record(validRequest());

        verify(experimentRunMapper).insert(captor.capture());
        ExperimentRun stored = captor.getValue();
        assertThat(stored.getExperimentName()).isEqualTo("phase-4-comparison");
        assertThat(stored.getRoundIndex()).isEqualTo(1);
        assertThat(stored.getStrategy()).isEqualTo(ExperimentStrategy.FIXED);
        assertThat(stored.getTaskCount()).isEqualTo(30);
        assertThat(stored.getConcurrencyConfig()).isEqualTo("{\"concurrency\":4}");
        assertThat(stored.getFaultConfig()).isEqualTo("{\"mode\":\"none\"}");
        assertThat(stored.getRandomSeed()).isEqualTo(20261016L);
        assertThat(stored.getEnvironment()).isEqualTo("{\"host\":\"test-box\"}");
        assertThat(stored.getStartedAt()).isEqualTo(STARTED);
        assertThat(stored.getFinishedAt()).isEqualTo(STARTED.plusSeconds(60));
        assertThat(stored.getResultSummary()).isEqualTo("{\"succeededTasks\":30}");
        assertThat(stored.getNotes()).isEqualTo("round 1");
        assertThat(stored.getCreatedAt()).isEqualTo(LocalDateTime.of(2026, 10, 16, 10, 0));
        assertThat(view.id()).isEqualTo(11L);
        assertThat(view.concurrencyConfig().get("concurrency").asInt()).isEqualTo(4);
        assertThat(view.environment().get("host").asText()).isEqualTo("test-box");
        assertThat(view.strategy()).isEqualTo("FIXED");
    }

    @Test
    void leavesOptionalSnapshotsEmptyWhenTheyWereNotGiven() {
        ArgumentCaptor<ExperimentRun> captor = ArgumentCaptor.forClass(ExperimentRun.class);
        ExperimentRunRequest request = new ExperimentRunRequest(
                "phase-4-comparison", 2, ExperimentStrategy.ADAPTIVE, 30,
                json("{\"concurrency\":4}"), null, null,
                json("{\"host\":\"test-box\"}"), STARTED, null, null, "  ");

        ExperimentRunView view = service().record(request);

        verify(experimentRunMapper).insert(captor.capture());
        assertThat(captor.getValue().getFaultConfig()).isNull();
        assertThat(captor.getValue().getRandomSeed()).isNull();
        assertThat(captor.getValue().getResultSummary()).as("a running round has no result yet").isNull();
        assertThat(captor.getValue().getFinishedAt()).isNull();
        assertThat(captor.getValue().getNotes()).as("blank notes are not a note").isNull();
        assertThat(view.faultConfig()).isNull();
        assertThat(view.resultSummary()).isNull();
    }

    @Test
    void refusesARoundWithoutTheConfigurationThatExplainsIt() {
        ExperimentRunRequest base = validRequest();

        assertThatThrownBy(() -> service().record(withName(base, "  ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("experimentName");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 0, base.strategy(), base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("roundIndex");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, null, base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strategy");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, base.strategy(), base.taskCount(), null,
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrencyConfig is required");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, base.strategy(), base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), null, base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("environment is required");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, base.strategy(), base.taskCount(), json("[1,2]"),
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrencyConfig must be a JSON object");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, base.strategy(), base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), base.environment(), null,
                base.finishedAt(), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("startedAt is required");
        assertThatThrownBy(() -> service().record(new ExperimentRunRequest(
                base.experimentName(), 1, base.strategy(), base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                STARTED.minusSeconds(1), base.resultSummary(), base.notes())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finishedAt must not be before startedAt");
    }

    @Test
    void rejectsARoundThatWasAlreadyRecordedInsteadOfOverwritingIt() {
        when(experimentRunMapper.insert(any(ExperimentRun.class)))
                .thenThrow(new DuplicateKeyException("uk_experiment_run_round"));

        assertThatThrownBy(() -> service().record(validRequest()))
                .isInstanceOf(DuplicateExperimentRoundException.class)
                .hasMessageContaining("phase-4-comparison");
    }

    @Test
    void pagesTheRecordedRounds() {
        when(experimentRunMapper.countByQuery(any(), any())).thenReturn(0L);

        ExperimentRunPage empty = service().query("phase-4-comparison", ExperimentStrategy.ADAPTIVE, 0, 20);

        assertThat(empty.total()).isZero();
        assertThat(empty.items()).isEmpty();
        verify(experimentRunMapper, never()).findPageByQuery(any(), any(), anyInt(), anyLong());

        ExperimentRun run = new ExperimentRun();
        run.setId(3L);
        run.setExperimentName("phase-4-comparison");
        run.setRoundIndex(2);
        run.setStrategy(ExperimentStrategy.ADAPTIVE);
        run.setTaskCount(30);
        run.setConcurrencyConfig("{}");
        run.setEnvironment("{}");
        run.setStartedAt(STARTED);
        run.setCreatedAt(LocalDateTime.of(2026, 10, 16, 10, 0));
        when(experimentRunMapper.countByQuery(any(), any())).thenReturn(2L);
        when(experimentRunMapper.findPageByQuery(any(), any(), anyInt(), anyLong())).thenReturn(List.of(run));

        ExperimentRunPage page = service().query("phase-4-comparison", ExperimentStrategy.ADAPTIVE, 1, 20);

        verify(experimentRunMapper).findPageByQuery("phase-4-comparison", ExperimentStrategy.ADAPTIVE, 20, 20L);
        assertThat(page.total()).isEqualTo(2L);
        assertThat(page.items()).hasSize(1);
        assertThat(page.items().get(0).roundIndex()).isEqualTo(2);
    }

    @Test
    void returnsEveryRoundOfAnExperimentAndNothingForOneThatHasNotRun() {
        ExperimentRun run = new ExperimentRun();
        run.setId(1L);
        run.setExperimentName("phase-4-comparison");
        run.setRoundIndex(1);
        run.setStrategy(ExperimentStrategy.FIXED);
        run.setConcurrencyConfig("{}");
        run.setEnvironment("{}");
        run.setStartedAt(STARTED);
        when(experimentRunMapper.findByExperimentName("phase-4-comparison")).thenReturn(List.of(run));
        when(experimentRunMapper.findByExperimentName("never-ran")).thenReturn(List.of());

        ExperimentRounds rounds = service().rounds("phase-4-comparison");

        assertThat(rounds.experimentName()).isEqualTo("phase-4-comparison");
        assertThat(rounds.rounds()).hasSize(1);
        assertThat(service().rounds("never-ran").rounds()).as("an unknown experiment is not an error").isEmpty();
    }

    private static ExperimentRunRequest withName(ExperimentRunRequest base, String name) {
        return new ExperimentRunRequest(
                name, base.roundIndex(), base.strategy(), base.taskCount(), base.concurrencyConfig(),
                base.faultConfig(), base.randomSeed(), base.environment(), base.startedAt(),
                base.finishedAt(), base.resultSummary(), base.notes());
    }
}
