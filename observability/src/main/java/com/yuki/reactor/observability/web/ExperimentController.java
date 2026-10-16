package com.yuki.reactor.observability.web;

import com.yuki.reactor.observability.domain.ExperimentStrategy;
import com.yuki.reactor.observability.dto.ExperimentRunPage;
import com.yuki.reactor.observability.dto.ExperimentRunRequest;
import com.yuki.reactor.observability.dto.ExperimentRunView;
import com.yuki.reactor.observability.dto.ExperimentRounds;
import com.yuki.reactor.observability.service.ExperimentRunService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Experiment record API (PRD 7.4): the runs behind the comparison of PRD 10.1.
 *
 * <p>{@code GET /api/v1/experiments/{name}} returns every recorded round of one experiment, both
 * strategies included, which is what makes the comparison reproducible instead of anecdotal.
 */
@RestController
@RequestMapping("/api/v1/experiments")
public class ExperimentController {

    private final ExperimentRunService experimentRunService;

    public ExperimentController(ExperimentRunService experimentRunService) {
        this.experimentRunService = experimentRunService;
    }

    /** Records one finished round. A repeated round is a 409, never an overwrite. */
    @PostMapping
    public ResponseEntity<ExperimentRunView> record(@RequestBody ExperimentRunRequest request) {
        ExperimentRunView view = experimentRunService.record(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(view);
    }

    @GetMapping
    public ExperimentRunPage list(@RequestParam(required = false) String name,
                                  @RequestParam(required = false) ExperimentStrategy strategy,
                                  @RequestParam(defaultValue = "0") int page,
                                  @RequestParam(defaultValue = "20") int size) {
        return experimentRunService.query(name, strategy, page, size);
    }

    @GetMapping("/{name}")
    public ExperimentRounds rounds(@PathVariable String name) {
        return experimentRunService.rounds(name);
    }
}
