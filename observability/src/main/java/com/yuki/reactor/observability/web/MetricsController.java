package com.yuki.reactor.observability.web;

import com.yuki.reactor.observability.dto.MetricsSummary;
import com.yuki.reactor.observability.service.MetricsSummaryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

/**
 * Metric summary API (PRD 10.2). The window is {@code [from, to)}; omitted bounds are resolved by the
 * service and echoed back in the response.
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private final MetricsSummaryService metricsSummaryService;

    public MetricsController(MetricsSummaryService metricsSummaryService) {
        this.metricsSummaryService = metricsSummaryService;
    }

    @GetMapping("/summary")
    public MetricsSummary summary(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return metricsSummaryService.summary(from, to);
    }
}
