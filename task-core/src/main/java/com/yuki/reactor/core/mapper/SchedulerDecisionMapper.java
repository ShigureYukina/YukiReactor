package com.yuki.reactor.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yuki.reactor.core.domain.SchedulerDecision;
import org.apache.ibatis.annotations.Mapper;

/**
 * Writes scheduling decisions (PRD 7.3).
 *
 * <p>Reading and exposing them belongs to the observability work; until then decisions are inspected
 * through the database, which is what the fault experiments do.
 */
@Mapper
public interface SchedulerDecisionMapper extends BaseMapper<SchedulerDecision> {
}
