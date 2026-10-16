package com.yuki.reactor.worker;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Worker configuration (prefix {@code reactor.worker}).
 *
 * <p>Values are validated when the engine is constructed so a misconfigured worker fails at startup
 * instead of silently executing nothing. Lease renewal and timeout handling are phase 2 concerns;
 * {@code leaseTtl} is stored on the task today but not yet enforced.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "reactor.worker")
public class TaskWorkerProperties {

    /** Whether this process runs a worker at all. */
    private boolean enabled = true;

    /** Stable worker identity; when blank it is derived from host name and process id. */
    private String id = "";

    /** Number of tasks this worker executes in parallel. */
    private int concurrency = 4;

    /** Delay between polling rounds. */
    private Duration pollInterval = Duration.ofMillis(500);

    /** Maximum number of tasks claimed per polling round. */
    private int batchSize = 10;

    /** Validity period of the lease taken when claiming a task. */
    private Duration leaseTtl = Duration.ofMinutes(1);
}
