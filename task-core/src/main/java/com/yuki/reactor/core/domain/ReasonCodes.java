package com.yuki.reactor.core.domain;

/**
 * Reason codes of scheduling decisions (PRD 5.6).
 *
 * <p>A code describes a real decision taken by the scheduling or control code, never something inferred
 * from a task status afterwards. A new code is added to the PRD before it is used here.
 */
public final class ReasonCodes {

    /** Concurrency was reduced because a downstream signal stayed above its threshold. */
    public static final String DOWNSTREAM_OVERLOAD = "DOWNSTREAM_OVERLOAD";

    /** Concurrency was raised again after the observation window passed with healthy signals. */
    public static final String RECOVERY_OBSERVED = "RECOVERY_OBSERVED";

    /** Concurrency fell back to a conservative value because the metrics could not be read. */
    public static final String METRICS_UNAVAILABLE = "METRICS_UNAVAILABLE";

    /** The concurrency quota of this worker (or of the task type) was used up, so the task waited. */
    public static final String CAPACITY_LIMIT = "CAPACITY_LIMIT";

    /** The batch or priority order of the dispatch round left the task waiting although quota was free. */
    public static final String PRIORITY_WAIT = "PRIORITY_WAIT";

    /** A failed attempt was rescheduled with a backoff delay and is not ready to run yet (PRD 5.4). */
    public static final String RETRY_BACKOFF = "RETRY_BACKOFF";

    /** The lease of a running attempt expired and the task was taken back by recovery. */
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";

    /** A retryable failure could not be retried because {@code max_attempts} was reached. */
    public static final String MAX_RETRIES_EXCEEDED = "MAX_RETRIES_EXCEEDED";

    private ReasonCodes() {
    }
}
