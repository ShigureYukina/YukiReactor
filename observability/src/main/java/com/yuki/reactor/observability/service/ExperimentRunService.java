package com.yuki.reactor.observability.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.common.web.PageLimits;
import com.yuki.reactor.observability.domain.ExperimentRun;
import com.yuki.reactor.observability.domain.ExperimentStrategy;
import com.yuki.reactor.observability.dto.ExperimentRunPage;
import com.yuki.reactor.observability.dto.ExperimentRunRequest;
import com.yuki.reactor.observability.dto.ExperimentRunView;
import com.yuki.reactor.observability.dto.ExperimentRounds;
import com.yuki.reactor.observability.exception.DuplicateExperimentRoundException;
import com.yuki.reactor.observability.mapper.ExperimentRunMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Records and reads experiment rounds (PRD 7.4, 10.1).
 *
 * <p>A round is stored exactly as reported: configuration, seed, environment and result snapshot all
 * come from the caller that ran it. Nothing is filled in by the service, and a round that is recorded
 * twice is rejected instead of being overwritten, so the comparison keeps every round that ran.
 */
@Service
public class ExperimentRunService {

    private static final int MAX_EXPERIMENT_NAME_LENGTH = 128;
    private static final int MAX_NOTES_LENGTH = 1024;

    private final ExperimentRunMapper experimentRunMapper;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public ExperimentRunService(ExperimentRunMapper experimentRunMapper,
                                ObjectMapper objectMapper,
                                Clock clock) {
        this.experimentRunMapper = experimentRunMapper;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public ExperimentRunView record(ExperimentRunRequest request) {
        String name = requireExperimentName(request.experimentName());
        int roundIndex = requireRoundIndex(request.roundIndex());
        ExperimentStrategy strategy = requireStrategy(request.strategy());
        int taskCount = requireTaskCount(request.taskCount());
        LocalDateTime startedAt = requireStartedAt(request.startedAt());
        LocalDateTime finishedAt = requireFinishedAt(request.finishedAt(), startedAt);

        ExperimentRun run = new ExperimentRun();
        run.setExperimentName(name);
        run.setRoundIndex(roundIndex);
        run.setStrategy(strategy);
        run.setTaskCount(taskCount);
        run.setConcurrencyConfig(requireJsonObject(request.concurrencyConfig(), "concurrencyConfig", true));
        run.setFaultConfig(requireJsonObject(request.faultConfig(), "faultConfig", false));
        run.setRandomSeed(request.randomSeed());
        run.setEnvironment(requireJsonObject(request.environment(), "environment", true));
        run.setStartedAt(startedAt);
        run.setFinishedAt(finishedAt);
        run.setResultSummary(requireJsonObject(request.resultSummary(), "resultSummary", false));
        run.setNotes(requireNotes(request.notes()));
        run.setCreatedAt(LocalDateTime.now(clock));

        try {
            experimentRunMapper.insert(run);
        } catch (DuplicateKeyException e) {
            throw new DuplicateExperimentRoundException(name, strategy.name(), roundIndex);
        }
        return toView(run);
    }

    public ExperimentRunPage query(String experimentName, ExperimentStrategy strategy, int page, int size) {
        DecisionQueryService.requirePage(page, size);
        long total = experimentRunMapper.countByQuery(experimentName, strategy);
        if (total == 0) {
            return new ExperimentRunPage(List.of(), 0, page, size);
        }
        long offset = (long) page * size;
        List<ExperimentRunView> items = experimentRunMapper
                .findPageByQuery(experimentName, strategy, size, offset)
                .stream()
                .map(this::toView)
                .toList();
        return new ExperimentRunPage(items, total, page, size);
    }

    /**
     * Every recorded round of one experiment, ordered by strategy and round. An unknown experiment
     * name is an empty list, not an error: it simply has not run yet.
     */
    public ExperimentRounds rounds(String experimentName) {
        String name = requireExperimentName(experimentName);
        List<ExperimentRunView> rounds = experimentRunMapper.findByExperimentName(name)
                .stream()
                .map(this::toView)
                .toList();
        return new ExperimentRounds(name, rounds);
    }

    private ExperimentRunView toView(ExperimentRun run) {
        return new ExperimentRunView(
                run.getId(),
                run.getExperimentName(),
                run.getRoundIndex(),
                run.getStrategy() == null ? null : run.getStrategy().name(),
                run.getTaskCount(),
                parseJson(run.getConcurrencyConfig()),
                parseJson(run.getFaultConfig()),
                run.getRandomSeed(),
                parseJson(run.getEnvironment()),
                run.getStartedAt(),
                run.getFinishedAt(),
                parseJson(run.getResultSummary()),
                run.getNotes(),
                run.getCreatedAt());
    }

    private JsonNode parseJson(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            // The value was validated before it was stored; this only guards against hand edited rows.
            return objectMapper.getNodeFactory().textNode(json);
        }
    }

    private static String requireExperimentName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("experimentName is required");
        }
        String trimmed = name.trim();
        if (trimmed.length() > MAX_EXPERIMENT_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    "experimentName must not exceed " + MAX_EXPERIMENT_NAME_LENGTH + " characters");
        }
        return trimmed;
    }

    private static int requireRoundIndex(Integer roundIndex) {
        if (roundIndex == null || roundIndex < 1) {
            throw new IllegalArgumentException("roundIndex must be greater than or equal to 1");
        }
        return roundIndex;
    }

    private static ExperimentStrategy requireStrategy(ExperimentStrategy strategy) {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy is required (FIXED or ADAPTIVE)");
        }
        return strategy;
    }

    private static int requireTaskCount(Integer taskCount) {
        if (taskCount == null || taskCount < 0) {
            throw new IllegalArgumentException("taskCount must be greater than or equal to 0");
        }
        return taskCount;
    }

    private static LocalDateTime requireStartedAt(LocalDateTime startedAt) {
        if (startedAt == null) {
            throw new IllegalArgumentException("startedAt is required");
        }
        return startedAt;
    }

    private static LocalDateTime requireFinishedAt(LocalDateTime finishedAt, LocalDateTime startedAt) {
        if (finishedAt != null && finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not be before startedAt");
        }
        return finishedAt;
    }

    private static String requireNotes(String notes) {
        if (notes == null) {
            return null;
        }
        String trimmed = notes.trim();
        if (trimmed.length() > MAX_NOTES_LENGTH) {
            throw new IllegalArgumentException("notes must not exceed " + MAX_NOTES_LENGTH + " characters");
        }
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String requireJsonObject(JsonNode node, String field, boolean required) {
        if (node == null || node.isNull()) {
            if (required) {
                throw new IllegalArgumentException(field + " is required and must be a JSON object");
            }
            return null;
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException(field + " must be a JSON object");
        }
        try {
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalArgumentException(field + " could not be serialized: " + e.getMessage());
        }
    }
}
