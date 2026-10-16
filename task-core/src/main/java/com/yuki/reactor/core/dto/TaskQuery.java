package com.yuki.reactor.core.dto;

import com.yuki.reactor.core.domain.TaskStatus;

import java.time.LocalDateTime;

/**
 * Task list filter. {@code page} is zero based; {@code size} is capped by the service.
 */
public record TaskQuery(TaskStatus status, LocalDateTime from, LocalDateTime to, int page, int size) {
}
