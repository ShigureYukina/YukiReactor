package com.yuki.reactor.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.core.domain.ExecutionResult;
import com.yuki.reactor.core.domain.TaskExecution;
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
     * Records the reported outcome of an open attempt. The {@code finished_at IS NULL} guard makes
     * the first reported outcome final, so a second report against the same attempt cannot rewrite
     * its history.
     *
     * @return 1 when the outcome was accepted, 0 when the attempt had already reported
     */
    @Update("""
            UPDATE task_execution
               SET result = #{result},
                   error_code = #{errorCode},
                   error_message = #{errorMessage},
                   finished_at = #{finishedAt}
             WHERE id = #{id}
               AND finished_at IS NULL
            """)
    int finish(@Param("id") long id,
               @Param("result") ExecutionResult result,
               @Param("errorCode") String errorCode,
               @Param("errorMessage") String errorMessage,
               @Param("finishedAt") LocalDateTime finishedAt);

    @Select("""
            SELECT *
              FROM task_execution
             WHERE task_id = #{taskId}
             ORDER BY id ASC
            """)
    List<TaskExecution> findByTaskId(@Param("taskId") long taskId);
}
