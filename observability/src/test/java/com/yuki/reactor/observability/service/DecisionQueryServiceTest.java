package com.yuki.reactor.observability.service;

import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.domain.SchedulerDecision;
import com.yuki.reactor.core.mapper.SchedulerDecisionMapper;
import com.yuki.reactor.observability.dto.DecisionPage;
import com.yuki.reactor.observability.dto.DecisionQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.LocalDateTime;
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
 * The decision query is the read side of PRD 5.6: it filters and pages the rows the scheduler wrote,
 * and it must reject a query it cannot answer instead of silently returning something else.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DecisionQueryServiceTest {

    private final LocalDateTime from = LocalDateTime.of(2026, 10, 16, 9, 0);
    private final LocalDateTime to = LocalDateTime.of(2026, 10, 16, 10, 0);

    @Mock
    private SchedulerDecisionMapper decisionMapper;

    private DecisionQueryService service() {
        return new DecisionQueryService(decisionMapper);
    }

    @Test
    void rejectsAPageThatCannotExist() {
        assertThatThrownBy(() -> service().query(new DecisionQuery(null, null, null, null, null, -1, 20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page");
        assertThatThrownBy(() -> service().query(new DecisionQuery(null, null, null, null, null, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
        assertThatThrownBy(() -> service().query(new DecisionQuery(null, null, null, null, null, 0, 201)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
    }

    @Test
    void rejectsAWindowThatDoesNotDescribeATimeRange() {
        assertThatThrownBy(() -> service().query(new DecisionQuery(null, null, null, to, from, 0, 20)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from must be before to");
        assertThatThrownBy(() -> service().query(new DecisionQuery(null, null, null, from, from, 0, 20)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void returnsAnEmptyPageWithoutReadingRowsWhenNothingMatches() {
        when(decisionMapper.countByQuery(any(), any(), any(), any(), any())).thenReturn(0L);

        DecisionPage page = service().query(new DecisionQuery(null, null, null, null, null, 0, 20));

        assertThat(page.total()).isZero();
        assertThat(page.items()).isEmpty();
        assertThat(page.page()).isZero();
        assertThat(page.size()).isEqualTo(20);
        verify(decisionMapper, never()).findPageByQuery(any(), any(), any(), any(), any(), anyInt(), anyLong());
    }

    @Test
    void passesTheFilterWindowAndPagingThroughAndMapsEveryRow() {
        when(decisionMapper.countByQuery(any(), any(), any(), any(), any())).thenReturn(3L);
        SchedulerDecision deferred = new SchedulerDecision();
        deferred.setId(5L);
        deferred.setTaskId(7L);
        deferred.setExecutionId(null);
        deferred.setDecisionType(DecisionType.TASK_DEFERRAL);
        deferred.setReasonCode(ReasonCodes.CAPACITY_LIMIT);
        deferred.setWorkerId("worker-a");
        deferred.setConcurrencyLimit(1);
        deferred.setRunningTasks(1);
        deferred.setBacklog(4);
        deferred.setObservedDetails("attempt 0 left unclaimed");
        deferred.setCreatedAt(from.plusMinutes(1));
        when(decisionMapper.findPageByQuery(any(), any(), any(), any(), any(), anyInt(), anyLong()))
                .thenReturn(List.of(deferred));

        DecisionPage page = service().query(
                new DecisionQuery(7L, DecisionType.TASK_DEFERRAL, ReasonCodes.CAPACITY_LIMIT, from, to, 2, 20));

        verify(decisionMapper).findPageByQuery(
                7L, DecisionType.TASK_DEFERRAL, ReasonCodes.CAPACITY_LIMIT, from, to, 20, 40L);
        assertThat(page.total()).isEqualTo(3L);
        assertThat(page.page()).isEqualTo(2);
        assertThat(page.items()).hasSize(1);
        assertThat(page.items().get(0).id()).isEqualTo(5L);
        assertThat(page.items().get(0).taskId()).isEqualTo(7L);
        assertThat(page.items().get(0).executionId())
                .as("a waiting task has no execution record yet")
                .isNull();
        assertThat(page.items().get(0).decisionType()).isEqualTo(DecisionType.TASK_DEFERRAL);
        assertThat(page.items().get(0).reasonCode()).isEqualTo(ReasonCodes.CAPACITY_LIMIT);
        assertThat(page.items().get(0).concurrencyLimit()).isEqualTo(1);
        assertThat(page.items().get(0).observedDetails()).isEqualTo("attempt 0 left unclaimed");
    }

    @Test
    void readsTheDecisionsOfOneTaskOldestFirst() {
        SchedulerDecision decision = new SchedulerDecision();
        decision.setId(1L);
        when(decisionMapper.findByTaskId(42L)).thenReturn(List.of(decision));

        assertThat(service().findByTaskId(42L)).containsExactly(decision);
        verify(decisionMapper).findByTaskId(42L);
    }
}
