package com.yuki.reactor.observability.dto;

import java.util.List;

/** One page of decisions plus the total number of rows matching the filter. */
public record DecisionPage(List<DecisionView> items, long total, int page, int size) {
}
