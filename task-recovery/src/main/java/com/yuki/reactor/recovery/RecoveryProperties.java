package com.yuki.reactor.recovery;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration of the lease recovery scanner (PRD 5.3).
 */
@Getter
@Setter
@ConfigurationProperties("reactor.recovery")
public class RecoveryProperties {

    /** Whether the recovery scanner runs at all. */
    private boolean enabled = true;

    /** Delay between two recovery scans. */
    private Duration scanInterval = Duration.ofSeconds(5);

    /** Maximum number of expired leases and orphan executions handled per scan. */
    private int batchSize = 50;

    /** Whether attempts left open by a task that already moved on are closed by the scanner. */
    private boolean orphanSweepEnabled = true;

    public long scanIntervalMillis() {
        return Math.max(1L, scanInterval.toMillis());
    }

    public int effectiveBatchSize() {
        return Math.max(1, batchSize);
    }
}
