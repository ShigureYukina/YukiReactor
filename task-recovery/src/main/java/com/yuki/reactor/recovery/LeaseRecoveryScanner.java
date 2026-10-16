package com.yuki.reactor.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@link LeaseRecoveryService#scanOnce()} on a fixed schedule.
 *
 * <p>A failed scan is logged and the schedule keeps running: a database that is briefly unavailable
 * must not stop the recovery of already expired leases (PRD 5.3).
 */
public class LeaseRecoveryScanner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LeaseRecoveryScanner.class);

    private final LeaseRecoveryService recoveryService;
    private final RecoveryProperties properties;

    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private volatile RecoveryScanResult lastResult = RecoveryScanResult.EMPTY;

    public LeaseRecoveryScanner(LeaseRecoveryService recoveryService, RecoveryProperties properties) {
        this.recoveryService = recoveryService;
        this.properties = properties;
        validate(properties);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "reactor-recovery");
            thread.setDaemon(false);
            return thread;
        });
        running = true;
        long interval = properties.scanIntervalMillis();
        scheduler.scheduleWithFixedDelay(this::scanQuietly, 0, interval, TimeUnit.MILLISECONDS);
        log.info("lease recovery scanner started: scanInterval={}ms batchSize={} orphanSweep={}",
                interval, properties.effectiveBatchSize(), properties.isOrphanSweepEnabled());
    }

    @Override
    public void stop() {
        running = false;
        ScheduledExecutorService current = scheduler;
        if (current == null) {
            return;
        }
        current.shutdownNow();
        try {
            if (!current.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("lease recovery scanner did not stop within 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("lease recovery scanner stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** One scan, used by the schedule and by tests that want a deterministic pass. */
    public RecoveryScanResult scanOnce() {
        RecoveryScanResult result = recoveryService.scanOnce();
        lastResult = result;
        return result;
    }

    public RecoveryScanResult getLastResult() {
        return lastResult;
    }

    private void scanQuietly() {
        try {
            scanOnce();
        } catch (RuntimeException e) {
            log.warn("recovery scan failed, keeping the schedule alive: {}", e.toString());
        }
    }

    private static void validate(RecoveryProperties properties) {
        if (properties.getScanInterval() == null || properties.getScanInterval().isZero()
                || properties.getScanInterval().isNegative()) {
            throw new IllegalArgumentException("reactor.recovery.scan-interval must be positive");
        }
        if (properties.getBatchSize() < 1) {
            throw new IllegalArgumentException("reactor.recovery.batch-size must be at least 1");
        }
    }
}
