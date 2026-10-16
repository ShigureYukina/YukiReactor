package com.yuki.reactor.observability.web;

import com.yuki.reactor.observability.dto.TaskTimeline;
import com.yuki.reactor.observability.service.TaskTimelineService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Task timeline API (PRD 3.1 item 6, 5.6): why a task waited, retried or was taken back.
 *
 * <p>It lives under the same {@code /api/v1/tasks} base as the task API, as another view of one task.
 */
@RestController
@RequestMapping("/api/v1/tasks")
public class TaskTimelineController {

    private final TaskTimelineService timelineService;

    public TaskTimelineController(TaskTimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @GetMapping("/{id}/timeline")
    public TaskTimeline timeline(@PathVariable long id) {
        return timelineService.timeline(id);
    }
}
