package com.yuki.reactor.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.error.ErrorCategory;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Persistence of execution attempts.
 *
 * <p>Rows are inserted when a task is claimed and completed when the attempt reports back, so a
 * worker that dies mid-flight leaves a row without {@code finished_at} (PRD 7.2).
 */
public interface TaskExecutionMapper extends BaseMapper<TaskExecution> {

    /**
     * Records the reported outcome of an open attempt, including how the failure was classified.
     * The {@code finished_at IS NULL} guard makes the first reported outcome final, so a second
     * report against the same attempt cannot rewrite its history.
     *
     * @return 1 when the outcome was accepted, 0 when the attempt had already reported
     */
    @Update("""
            UPDATE task_execution
               SET result = #{result},
                   error_code = #{errorCode},
                   error_category = #{errorCategory},
                   error_message = #{errorMessage},
                   finished_at = #{finishedAt}
             WHERE id = #{id}
               AND finished_at IS NULL
            """)
    int finish(@Param("id") long id,
               @Param("result") ExecutionResult result,
               @Param("errorCode") String errorCode,
               @Param("errorCategory") ErrorCategory errorCategory,
               @Param("errorMessage") String errorMessage,
               @Param("finishedAt") LocalDateTime finishedAt);

    @Select("""
            SELECT *
              FROM task_execution
             WHERE task_id = #{taskId}
             ORDER BY id ASC
            """)
    List<TaskExecution> findByTaskId(@Param("taskId") long taskId);

    /**
     * Attempts of a task that never reported a result.
     */
    @Select("""
            SELECT *
              FROM task_execution
             WHERE task_id = #{taskId}
               AND finished_at IS NULL
             ORDER BY id ASC
            """)
    List<TaskExecution> findOpenByTaskId(@Param("taskId") long taskId);

    /**
     * Open attempts whose task has moved on: its status is no longer {@code RUNNING}, or the task is
     * running under a newer fencing token, or it belongs to no task at all. Such rows can only be
     * left behind when writing the result failed, and are closed by the consistency sweep (PRD 5.4).
     */
    @Select("""
            SELECT e.*
              FROM task_execution e
              LEFT JOIN task t ON t.id = e.task_id
             WHERE e.finished_at IS NULL
               AND (t.id IS NULL
                    OR t.status <> 'RUNNING'
                    OR t.fencing_token <> e.fencing_token)
             ORDER BY e.id ASC
             LIMIT #{limit}
            """)
    List<TaskExecution> findOrphanOpenExecutions(@Param("limit") int limit);

    /** Execution attempts started in a window: the denominator of "attempts per task" (PRD 10.2). */
    @Select("""
            SELECT COUNT(*)
              FROM task_execution
             WHERE started_at >= #{from}
               AND started_at < #{to}
            """)
    long countStartedBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /** Logical tasks that were attempted at least once in the window. */
    @Select("""
            SELECT COUNT(DISTINCT task_id)
              FROM task_execution
             WHERE started_at >= #{from}
               AND started_at < #{to}
            """)
    long countDistinctTasksBetween(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /**
     * Queue delay in milliseconds: from the moment the logical task was submitted to the moment an
     * attempt actually started (PRD 10.2). Averaged over the attempts started in the window.
     */
    @Select("""
            SELECT AVG(TIMESTAMPDIFF(MICROSECOND, t.created_at, e.started_at) / 1000.0)
              FROM task_execution e
              JOIN task t ON t.id = e.task_id
             WHERE e.started_at >= #{from}
               AND e.started_at < #{to}
            """)
    Double avgQueueDelayMs(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /**
     * Execution delay in milliseconds: from the start of an attempt to the moment it reported a
     * result (PRD 10.2). Averaged over the attempts finished in the window.
     */
    @Select("""
            SELECT AVG(TIMESTAMPDIFF(MICROSECOND, e.started_at, e.finished_at) / 1000.0)
              FROM task_execution e
             WHERE e.finished_at IS NOT NULL
               AND e.finished_at >= #{from}
               AND e.finished_at < #{to}
            """)
    Double avgExecutionDelayMs(@Param("from") LocalDateTime from, @Param("to") LocalDateTime to);
}
