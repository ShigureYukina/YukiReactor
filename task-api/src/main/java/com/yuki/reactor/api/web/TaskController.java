package com.yuki.reactor.api.web;

import com.yuki.reactor.api.dto.SubmitTaskRequest;
import com.yuki.reactor.api.dto.SubmitTaskResponse;
import com.yuki.reactor.api.dto.TaskAdminRequest;
import com.yuki.reactor.core.dto.TaskExecutionView;
import com.yuki.reactor.core.dto.TaskPage;
import com.yuki.reactor.core.dto.TaskQuery;
import com.yuki.reactor.core.dto.TaskView;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.service.TaskService;
import com.yuki.reactor.core.service.TaskSubmission;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Task API (PRD 5.1).
 *
 * <p>Submitting a task means the engine accepted and persisted it - not that it ran successfully.
 * Clients follow progress through {@code GET /api/v1/tasks/{id}} and its execution history.
 */
@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    /**
     * Submits a task. Returns 201 when the task was created and 200 when an existing idempotency key
     * was matched; the body always describes the logical task the caller should track.
     */
    @PostMapping
    public ResponseEntity<SubmitTaskResponse> submit(@RequestBody SubmitTaskRequest request) {
        TaskSubmission submission = taskService.submit(request.toCommand());
        SubmitTaskResponse body = new SubmitTaskResponse(submission.task(), !submission.created());
        HttpStatus status = submission.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(body);
    }

    @GetMapping("/{id}")
    public TaskView get(@PathVariable long id) {
        return taskService.get(id);
    }

    /** Lists tasks filtered by status and creation time; timestamps are UTC ISO-8601 strings. */
    @GetMapping
    public TaskPage list(@RequestParam(required = false) TaskStatus status,
                         @RequestParam(required = false)
                         @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                         @RequestParam(required = false)
                         @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                         @RequestParam(defaultValue = "0") int page,
                         @RequestParam(defaultValue = "20") int size) {
        return taskService.query(new TaskQuery(status, from, to, page, size));
    }

    @GetMapping("/{id}/executions")
    public List<TaskExecutionView> executions(@PathVariable long id) {
        return taskService.executions(id);
    }

    /** Cancels a task that has not started yet. Fails with 409 once it is running or finished. */
    @PostMapping("/{id}/cancel")
    public TaskView cancel(@PathVariable long id, @RequestBody TaskAdminRequest request) {
        return taskService.cancel(id, request.operator(), request.reason());
    }

    /** Replays a failed task after an operator decided it is worth another try. */
    @PostMapping("/{id}/replay")
    public TaskView replay(@PathVariable long id, @RequestBody TaskAdminRequest request) {
        return taskService.replay(id, request.operator(), request.reason());
    }
}
