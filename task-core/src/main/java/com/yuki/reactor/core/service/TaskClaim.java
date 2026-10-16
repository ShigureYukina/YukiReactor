package com.yuki.reactor.core.service;

import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;

/**
 * A claim that succeeded: the task is now RUNNING under {@code task.fencingToken}, and
 * {@code execution} is the attempt row opened for it.
 */
public record TaskClaim(Task task, TaskExecution execution) {
}
