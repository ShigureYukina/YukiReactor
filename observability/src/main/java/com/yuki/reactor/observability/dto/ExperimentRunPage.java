package com.yuki.reactor.observability.dto;

import java.util.List;

/** One page of experiment rounds plus the total number of rows matching the filter. */
public record ExperimentRunPage(List<ExperimentRunView> items, long total, int page, int size) {
}
