package com.yuki.reactor.worker;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Worker configuration (prefix {@code reactor.worker}).
 *
 * <p>Values are validated when the engine is constructed so a misconfigured worker fails at startup
 * instead of silently executing nothing.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "reactor.worker")
public class TaskWorkerProperties {

    /** Shortest lease renewal interval the engine accepts. */
    public static final Duration MIN_LEASE_RENEW_INTERVAL = Duration.ofMillis(100);

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

    /**
     * How often the lease of a running attempt is renewed (PRD 5.3). When left unset the engine
     * renews every third of {@link #leaseTtl}, which tolerates two lost heartbeats before a task
     * becomes recoverable. Must be shorter than {@link #leaseTtl}.
     */
    private Duration leaseRenewInterval;

    /**
     * Optional per task type concurrency caps, e.g. {@code reactor.worker.task-type-limits.echo=1}
     * (PRD 5.2). A type without an entry is only bounded by {@link #concurrency}.
     */
    private Map<String, Integer> taskTypeLimits = new LinkedHashMap<>();

    /** Renewal interval actually used, derived from the lease TTL when not configured explicitly. */
    public Duration effectiveLeaseRenewInterval() {
        if (leaseRenewInterval != null) {
            return leaseRenewInterval;
        }
        if (leaseTtl == null) {
            return MIN_LEASE_RENEW_INTERVAL;
        }
        Duration derived = leaseTtl.dividedBy(3);
        return derived.compareTo(MIN_LEASE_RENEW_INTERVAL) < 0 ? MIN_LEASE_RENEW_INTERVAL : derived;
    }

    /** @return the configured cap for the task type, or {@code 0} when the type is unlimited */
    public int limitFor(String taskType) {
        if (taskType == null || taskTypeLimits == null || taskTypeLimits.isEmpty()) {
            return 0;
        }
        for (Map.Entry<String, Integer> entry : taskTypeLimits.entrySet()) {
            if (entry.getKey() != null && entry.getKey().trim().toLowerCase(Locale.ROOT).equals(taskType.trim().toLowerCase(Locale.ROOT))) {
                Integer limit = entry.getValue();
                return limit == null ? 0 : limit;
            }
        }
        return 0;
    }
}
