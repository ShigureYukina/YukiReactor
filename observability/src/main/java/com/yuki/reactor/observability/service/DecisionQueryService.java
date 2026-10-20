package com.yuki.reactor.observability.service;

import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.common.web.PageLimits;
import com.yuki.reactor.observability.dto.DecisionPage;
import com.yuki.reactor.observability.dto.DecisionQuery;
import com.yuki.reactor.observability.dto.DecisionView;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Answers "why did the scheduler do that" from the recorded decisions (PRD 5.6).
 *
 * <p>This service only filters and returns rows written by the decision code. It never derives a
 * reason from the current task status, which is what makes the explanation trustworthy.
 */
@Service
public class DecisionQueryService {

    private final SchedulerDecisionMapper decisionMapper;

    public DecisionQueryService(SchedulerDecisionMapper decisionMapper) {
        this.decisionMapper = decisionMapper;
    }

    public DecisionPage query(DecisionQuery query) {
        requirePage(query.page(), query.size());
        requireWindow(query.from(), query.to());
        long total = decisionMapper.countByQuery(
                query.taskId(), query.type(), query.reasonCode(), query.from(), query.to());
        if (total == 0) {
            return new DecisionPage(List.of(), 0, query.page(), query.size());
        }
        long offset = (long) query.page() * query.size();
        List<DecisionView> items = decisionMapper
                .findPageByQuery(query.taskId(), query.type(), query.reasonCode(),
                        query.from(), query.to(), query.size(), offset)
                .stream()
                .map(DecisionView::from)
                .toList();
        return new DecisionPage(items, total, query.page(), query.size());
    }

    /** Decisions about one task, oldest first: the raw material of its timeline. */
    public List<SchedulerDecision> findByTaskId(long taskId) {
        return decisionMapper.findByTaskId(taskId);
    }

    static void requirePage(int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must be greater than or equal to 0");
        }
        if (size < 1 || size > PageLimits.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "size must be between 1 and " + PageLimits.MAX_PAGE_SIZE);
        }
    }

    static void requireWindow(LocalDateTime from, LocalDateTime to) {
        if (from != null && to != null && !from.isBefore(to)) {
            throw new IllegalArgumentException("from must be before to");
        }
    }
}
