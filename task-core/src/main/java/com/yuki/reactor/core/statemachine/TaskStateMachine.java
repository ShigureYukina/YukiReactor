package com.yuki.reactor.core.statemachine;

import com.yuki.reactor.core.domain.TaskStatus;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The single definition of legal task state transitions (PRD 4.2).
 *
 * <p>Every state change is validated here first and then defended again by a conditional SQL
 * update, so a concurrent writer can never skip the state machine (PRD 4.3).
 */
public final class TaskStateMachine {

    private static final Map<TaskStatus, Set<TaskStatus>> ALLOWED = allowedTransitions();

    private TaskStateMachine() {
    }

    /**
     * Legal transitions of V1.0. Extending this table is a requirement change and must be reflected
     * in {@code docs/PRD.md} first.
     */
    private static Map<TaskStatus, Set<TaskStatus>> allowedTransitions() {
        Map<TaskStatus, Set<TaskStatus>> allowed = new EnumMap<>(TaskStatus.class);
        allowed.put(TaskStatus.PENDING, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELLED));
        allowed.put(TaskStatus.RUNNING,
                EnumSet.of(TaskStatus.SUCCEEDED, TaskStatus.RETRY_WAIT, TaskStatus.FAILED));
        allowed.put(TaskStatus.RETRY_WAIT, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELLED));
        // FAILED -> PENDING only through an explicit administrative replay.
        allowed.put(TaskStatus.FAILED, EnumSet.of(TaskStatus.PENDING));
        allowed.put(TaskStatus.SUCCEEDED, EnumSet.noneOf(TaskStatus.class));
        allowed.put(TaskStatus.CANCELLED, EnumSet.noneOf(TaskStatus.class));
        return Collections.unmodifiableMap(allowed);
    }

    public static boolean canTransition(TaskStatus from, TaskStatus to) {
        if (from == null || to == null) {
            return false;
        }
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }

    /**
     * @throws IllegalTaskTransitionException when the transition is not part of the state machine
     */
    public static void requireTransition(TaskStatus from, TaskStatus to) {
        if (!canTransition(from, to)) {
            throw new IllegalTaskTransitionException(from, to);
        }
    }

    public static Set<TaskStatus> allowedTargets(TaskStatus from) {
        return ALLOWED.getOrDefault(from, Set.of());
    }
}
