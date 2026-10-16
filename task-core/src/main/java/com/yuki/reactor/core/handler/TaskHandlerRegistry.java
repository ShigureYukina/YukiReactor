package com.yuki.reactor.core.handler;

import com.yuki.reactor.core.exception.UnknownTaskTypeException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Registry of {@link TaskHandler} beans keyed by task type.
 *
 * <p>All handlers are collected at startup. A duplicate or invalid task type fails the application
 * immediately instead of silently shadowing a handler. Task types are compared
 * case-insensitively, so {@code Echo} and {@code echo} are the same type.
 */
@Component
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> discoveredHandlers) {
        Map<String, TaskHandler> registered = new TreeMap<>();
        for (TaskHandler handler : discoveredHandlers) {
            String taskType = handler.taskType();
            if (taskType == null || taskType.isBlank()) {
                throw new IllegalStateException(
                        "task handler " + handler.getClass().getName() + " declares a blank task type");
            }
            String key = normalize(taskType);
            TaskHandler previous = registered.putIfAbsent(key, handler);
            if (previous != null) {
                throw new IllegalStateException("duplicate task type '" + taskType + "' declared by "
                        + previous.getClass().getName() + " and " + handler.getClass().getName());
            }
        }
        this.handlers = Map.copyOf(registered);
    }

    public boolean knows(String taskType) {
        return taskType != null && handlers.containsKey(normalize(taskType));
    }

    /**
     * @throws UnknownTaskTypeException when no handler is registered for the task type
     */
    public TaskHandler require(String taskType) {
        TaskHandler handler = taskType == null ? null : handlers.get(normalize(taskType));
        if (handler == null) {
            throw new UnknownTaskTypeException(taskType);
        }
        return handler;
    }

    /** Registered task types, normalised to lower case. */
    public Set<String> registeredTypes() {
        return handlers.keySet();
    }

    private static String normalize(String taskType) {
        return taskType.trim().toLowerCase(Locale.ROOT);
    }
}
