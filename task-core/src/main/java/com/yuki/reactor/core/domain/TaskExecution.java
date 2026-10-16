package com.yuki.reactor.core.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One execution attempt of a logical task (table {@code task_execution}, PRD 7.2).
 *
 * <p>A row is inserted when the task is claimed, so a crashed worker still leaves a trace.
 * {@code result} and {@code finishedAt} stay {@code null} until the attempt reports back.
 */
@Data
@TableName("task_execution")
public class TaskExecution {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long taskId;

    private Integer attempt;

    private String workerId;

    private Long fencingToken;

    private LocalDateTime startedAt;

    private LocalDateTime finishedAt;

    private ExecutionResult result;

    private String errorCode;

    private String errorMessage;
}
