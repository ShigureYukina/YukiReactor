package com.yuki.reactor.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.SchedulerDecision;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Writes and reads scheduling decisions (PRD 5.6, 7.3).
 *
 * <p>Writing is done by the code that actually takes the decision ({@code SchedulerDecisionRecorder},
 * {@code ConcurrencyDecisionRecorder}). Reading is what the observability module needs to explain a
 * task: "why did this one wait, retry or get taken back". The read side never infers a reason from a
 * task status, it only returns decisions that were really recorded.
 */
@Mapper
public interface SchedulerDecisionMapper extends BaseMapper<SchedulerDecision> {

    /** Decisions about one task, oldest first, so they can be replayed as a timeline. */
    @Select("""
            SELECT *
              FROM scheduler_decision
             WHERE task_id = #{taskId}
             ORDER BY id ASC
            """)
    List<SchedulerDecision> findByTaskId(@Param("taskId") long taskId);

    @Select("""
            <script>
            SELECT COUNT(*)
              FROM scheduler_decision
            <where>
                <if test="taskId != null">AND task_id = #{taskId}</if>
                <if test="type != null">AND decision_type = #{type}</if>
                <if test="reasonCode != null">AND reason_code = #{reasonCode}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
            </script>
            """)
    long countByQuery(@Param("taskId") Long taskId,
                      @Param("type") DecisionType type,
                      @Param("reasonCode") String reasonCode,
                      @Param("from") LocalDateTime from,
                      @Param("to") LocalDateTime to);

    /** Newest decisions first: the query API is used to look at what just happened. */
    @Select("""
            <script>
            SELECT *
              FROM scheduler_decision
            <where>
                <if test="taskId != null">AND task_id = #{taskId}</if>
                <if test="type != null">AND decision_type = #{type}</if>
                <if test="reasonCode != null">AND reason_code = #{reasonCode}</if>
                <if test="from != null">AND created_at &gt;= #{from}</if>
                <if test="to != null">AND created_at &lt; #{to}</if>
            </where>
             ORDER BY id DESC
             LIMIT #{limit} OFFSET #{offset}
            </script>
            """)
    List<SchedulerDecision> findPageByQuery(@Param("taskId") Long taskId,
                                           @Param("type") DecisionType type,
                                           @Param("reasonCode") String reasonCode,
                                           @Param("from") LocalDateTime from,
                                           @Param("to") LocalDateTime to,
                                           @Param("limit") int limit,
                                           @Param("offset") long offset);
}
