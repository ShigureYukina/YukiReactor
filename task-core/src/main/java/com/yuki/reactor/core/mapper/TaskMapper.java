package com.yuki.reactor.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskStatus;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Persistence of task state.
 *
 * <p>Every state write is a conditional update: the {@code WHERE} clause carries the state the
 * caller believed it was acting on, so a concurrent worker, an expired lease or a late writer can
 * never overwrite state it no longer owns (PRD 4.3). A return value of {@code 0} means "I lost the
 * race", never "it probably worked".
 */
public interface TaskMapper extends BaseMapper<Task> {

    /**
     * Candidate tasks ordered by priority and eligibility time. Claiming still decides who wins.
     */
    @Select("""
            SELECT *
              FROM task
             WHERE status = 'PENDING'
               AND next_run_at <= #{now}
             ORDER BY priority ASC, next_run_at ASC, id ASC
             LIMIT #{limit}
            """)
    List<Task> findClaimable(@Param("now") LocalDateTime now, @Param("limit") int limit);

    /**
     * Atomically takes ownership of a pending task: the fencing token and attempt counter advance
     * in the same statement that flips the status.
     *
     * @return 1 when this caller won the task, 0 when it was claimed or cancelled elsewhere
     */
    @Update("""
            UPDATE task
               SET status = 'RUNNING',
                   lease_owner = #{workerId},
                   lease_until = #{leaseUntil},
                   fencing_token = fencing_token + 1,
                   attempt_count = attempt_count + 1,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'PENDING'
               AND next_run_at <= #{now}
            """)
    int claim(@Param("taskId") long taskId,
              @Param("workerId") String workerId,
              @Param("leaseUntil") LocalDateTime leaseUntil,
              @Param("now") LocalDateTime now);

    /**
     * Marks the task as succeeded. Requires the task to still be RUNNING under the given fencing
     * token, so an expired attempt cannot confirm a result it no longer owns.
     *
     * @return 1 when the state was written, 0 when it was rejected
     */
    @Update("""
            UPDATE task
               SET status = 'SUCCEEDED',
                   last_error = NULL,
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int markSucceeded(@Param("taskId") long taskId,
                      @Param("fencingToken") long fencingToken,
                      @Param("now") LocalDateTime now);

    /** Marks the task as terminally failed under the given fencing token. */
    @Update("""
            UPDATE task
               SET status = 'FAILED',
                   last_error = #{error},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'RUNNING'
               AND fencing_token = #{fencingToken}
            """)
    int markFailed(@Param("taskId") long taskId,
                   @Param("fencingToken") long fencingToken,
                   @Param("error") String error,
                   @Param("now") LocalDateTime now);

    /**
     * Cancels a task that has not started executing yet. Running tasks are not cancelled here:
     * the state machine has no {@code RUNNING -> CANCELLED} transition (PRD 4.2).
     */
    @Update("""
            UPDATE task
               SET status = 'CANCELLED',
                   last_error = #{note},
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status IN ('PENDING', 'RETRY_WAIT')
            """)
    int markCancelled(@Param("taskId") long taskId,
                      @Param("note") String note,
                      @Param("now") LocalDateTime now);

    /**
     * Administrative replay: a failed task becomes pending again with a fresh attempt budget.
     * The previous error is kept so the task history stays explainable.
     */
    @Update("""
            UPDATE task
               SET status = 'PENDING',
                   attempt_count = 0,
                   next_run_at = #{now},
                   lease_owner = NULL,
                   lease_until = NULL,
                   updated_at = #{now}
             WHERE id = #{taskId}
               AND status = 'FAILED'
            """)
    int resetForReplay(@Param("taskId") long taskId, @Param("now") LocalDateTime now);

    @Select("""
            SELECT *
              FROM task
             WHERE idempotency_scope = #{scope}
               AND idempotency_key = #{key}
            """)
    Task findByIdempotencyKey(@Param("scope") String scope, @Param("key") String key);

    @Select("""
            <script>
            SELECT COUNT(*)
              FROM task
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
            </script>
            """)
    long countByQuery(@Param("status") TaskStatus status,
                      @Param("from") LocalDateTime from,
                      @Param("to") LocalDateTime to);

    @Select("""
            <script>
            SELECT *
              FROM task
            <where>
                <if test="status != null">AND status = #{status}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
             ORDER BY created_at DESC, id DESC
             LIMIT #{limit} OFFSET #{offset}
            </script>
            """)
    List<Task> findPage(@Param("status") TaskStatus status,
                        @Param("from") LocalDateTime from,
                        @Param("to") LocalDateTime to,
                        @Param("limit") int limit,
                        @Param("offset") long offset);
}
