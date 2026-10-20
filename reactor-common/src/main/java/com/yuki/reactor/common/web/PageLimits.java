package com.yuki.reactor.common.web;

/**
 * Shared bounds for the read APIs, so every endpoint pages the same way.
 */
public final class PageLimits {

    public static final int DEFAULT_PAGE_SIZE = 20;
    public static final int MAX_PAGE_SIZE = 200;

    private PageLimits() {
    }
}
