package com.yuki.reactor.faultlab;

/**
 * The kinds of downstream trouble the mock service can inject. Each kind maps to the error
 * category the engine already understands (PRD 5.4), so the lab never invents a new failure
 * vocabulary.
 */
public enum DownstreamFailureKind {

    /** Temporary network-like trouble: retried with plain backoff. */
    TRANSIENT,

    /** The downstream is overloaded: retried, and visible to the adaptive controller. */
    OVERLOADED,

    /** The downstream asks the caller to slow down: retried after the announced delay. */
    THROTTLED
}
