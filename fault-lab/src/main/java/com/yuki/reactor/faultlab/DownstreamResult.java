package com.yuki.reactor.faultlab;

/**
 * Outcome of one simulated downstream call. The mock never throws: the handler turns the outcome
 * into the matching task failure, which keeps the decision of "what a failure means for the task"
 * in one place.
 *
 * @param failed            whether the call is reported as a failure
 * @param latencyMillis     observed call duration
 * @param kind              failure kind, only meaningful when {@code failed} is true
 * @param detail            human readable description
 * @param retryAfterMillis  delay the downstream asks for, only meaningful for
 *                          {@link DownstreamFailureKind#THROTTLED}
 */
public record DownstreamResult(boolean failed, long latencyMillis, DownstreamFailureKind kind, String detail,
                               long retryAfterMillis) {
}
