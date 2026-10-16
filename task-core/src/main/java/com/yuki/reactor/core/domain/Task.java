package com.yuki.reactor.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Persistent task aggregate (table {@code task}, PRD 7.1).
 *
 * <p>This is the only place task state lives: MySQL is the single source of truth (PRD 6.1).
 * Every state change must go through {@link com.yuki.reactor.core.statemachine.TaskStateMachine}
 * and a conditional update in {@link com.yuki.reactor.core.mapper.TaskMapper}.
 */
@Data
@TableName("task")
public class Task {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String taskType;

    /** Raw JSON document; treated as data only, never as code (PRD 8.4). */
    private String payload;

    private TaskStatus status;

    /** Lower value is scheduled first. */
    private Integer priority;

    private String idempotencyScope;

    private String idempotencyKey;

    private Integer attemptCount;

    private Integer maxAttempts;

    private LocalDateTime nextRunAt;

    private Long timeoutMs;

    private String leaseOwner;

    private LocalDateTime leaseUntil;

    /** Incremented on every successful claim, so stale writers can be rejected. */
    private Long fencingToken;

    private String lastError;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
