package com.yuki.reactor.observability.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.observability.domain.ExperimentRun;
import com.yuki.reactor.observability.domain.ExperimentStrategy;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/** Persistence and reading of experiment rounds (PRD 7.4). */
@Mapper
public interface ExperimentRunMapper extends BaseMapper<ExperimentRun> {

    @Select("""
            <script>
            SELECT COUNT(*)
              FROM experiment_run
            <where>
                <if test="experimentName != null">AND experiment_name = #{experimentName}</if>
                <if test="strategy != null">AND strategy = #{strategy}</if>
            </where>
            </script>
            """)
    long countByQuery(@Param("experimentName") String experimentName,
                      @Param("strategy") ExperimentStrategy strategy);

    /**
     * Rounds of an experiment, grouped by strategy and ordered by round: this is the order in which a
     * reader compares fixed against adaptive (PRD 10.1).
     */
    @Select("""
            <script>
            SELECT *
              FROM experiment_run
            <where>
                <if test="experimentName != null">AND experiment_name = #{experimentName}</if>
                <if test="strategy != null">AND strategy = #{strategy}</if>
            </where>
             ORDER BY experiment_name ASC, strategy ASC, round_index ASC
             LIMIT #{limit} OFFSET #{offset}
            </script>
            """)
    List<ExperimentRun> findPageByQuery(@Param("experimentName") String experimentName,
                                        @Param("strategy") ExperimentStrategy strategy,
                                        @Param("limit") int limit,
                                        @Param("offset") long offset);

    /** Every round of one experiment, grouped by strategy and ordered by round, for the comparison view. */
    @Select("""
            SELECT *
              FROM experiment_run
             WHERE experiment_name = #{experimentName}
             ORDER BY strategy ASC, round_index ASC
            """)
    List<ExperimentRun> findByExperimentName(@Param("experimentName") String experimentName);
}
