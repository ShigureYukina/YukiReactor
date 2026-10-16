package com.yuki.reactor.observability;

/** Shared bounds for the observability read APIs, so every endpoint pages the same way. */
public final class ObservabilityLimits {

    public static final int DEFAULT_PAGE_SIZE = 20;
    public static final int MAX_PAGE_SIZE = 200;

    private ObservabilityLimits() {
    }
}
