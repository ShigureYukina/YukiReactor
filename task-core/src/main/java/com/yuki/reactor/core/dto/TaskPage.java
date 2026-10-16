package com.yuki.reactor.core.dto;

import java.util.List;

/** One page of tasks plus the total number of rows matching the filter. */
public record TaskPage(List<TaskView> items, long total, int page, int size) {
}
