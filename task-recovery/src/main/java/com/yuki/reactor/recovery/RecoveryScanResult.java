package com.yuki.reactor.recovery;

/**
 * Counts of one recovery scan, used for logging and tests.
 */
public record RecoveryScanResult(int reclaimedForRetry, int reclaimedAsFailed, int closedOrphanExecutions) {

    public static final RecoveryScanResult EMPTY = new RecoveryScanResult(0, 0, 0);

    public int recoveredTasks() {
        return reclaimedForRetry + reclaimedAsFailed;
    }
}
