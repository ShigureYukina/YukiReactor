package com.yuki.reactor.core.statemachine;

import com.yuki.reactor.core.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The state machine is the contract of PRD 4.2, so every combination is asserted explicitly
 * instead of trusting a single happy-path test.
 */
class TaskStateMachineTest {

    private static final Map<TaskStatus, Set<TaskStatus>> LEGAL = Map.of(
            TaskStatus.PENDING, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELLED),
            TaskStatus.RUNNING, EnumSet.of(TaskStatus.SUCCEEDED, TaskStatus.RETRY_WAIT, TaskStatus.FAILED),
            TaskStatus.RETRY_WAIT, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELLED),
            TaskStatus.FAILED, EnumSet.of(TaskStatus.PENDING),
            TaskStatus.SUCCEEDED, EnumSet.noneOf(TaskStatus.class),
            TaskStatus.CANCELLED, EnumSet.noneOf(TaskStatus.class));

    @Test
    void everyTransitionCombinationMatchesTheDeclaredTable() {
        for (TaskStatus from : TaskStatus.values()) {
            for (TaskStatus to : TaskStatus.values()) {
                boolean expected = LEGAL.get(from).contains(to);
                assertThat(TaskStateMachine.canTransition(from, to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(expected);
            }
        }
    }

    @Test
    void runningTaskCannotBeCancelledDirectly() {
        assertThat(TaskStateMachine.canTransition(TaskStatus.RUNNING, TaskStatus.CANCELLED)).isFalse();
        assertThatThrownBy(() -> TaskStateMachine.requireTransition(TaskStatus.RUNNING, TaskStatus.CANCELLED))
                .isInstanceOf(IllegalTaskTransitionException.class)
                .hasMessageContaining("RUNNING -> CANCELLED");
    }

    @Test
    void terminalStatesHaveNoOutgoingTransitions() {
        for (TaskStatus terminal : EnumSet.of(TaskStatus.SUCCEEDED, TaskStatus.CANCELLED)) {
            assertThat(TaskStateMachine.allowedTargets(terminal)).isEmpty();
        }
    }

    @Test
    void failedTaskCanOnlyBeReplayedToPending() {
        assertThat(TaskStateMachine.allowedTargets(TaskStatus.FAILED))
                .containsExactly(TaskStatus.PENDING);
    }

    @ParameterizedTest
    @EnumSource(TaskStatus.class)
    void terminalFlagMatchesStateDefinition(TaskStatus status) {
        boolean expected = status == TaskStatus.SUCCEEDED
                || status == TaskStatus.FAILED
                || status == TaskStatus.CANCELLED;
        assertThat(status.isTerminal()).isEqualTo(expected);
    }

    @Test
    void nullArgumentsAreRejectedInsteadOfAccepted() {
        assertThat(TaskStateMachine.canTransition(null, TaskStatus.PENDING)).isFalse();
        assertThat(TaskStateMachine.canTransition(TaskStatus.PENDING, null)).isFalse();
    }
}
