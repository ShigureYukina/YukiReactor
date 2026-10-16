package com.yuki.reactor.observability.dto;

import java.util.List;

/**
 * Every recorded round of one experiment, ordered by strategy and round (PRD 7.4: the same experiment
 * runs many times and the comparison keeps all of them).
 */
public record ExperimentRounds(String experimentName, List<ExperimentRunView> rounds) {
}
