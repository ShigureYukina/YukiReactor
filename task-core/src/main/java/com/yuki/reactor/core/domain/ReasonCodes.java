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

    private ReasonCodes() {
    }
}
