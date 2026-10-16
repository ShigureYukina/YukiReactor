package com.yuki.reactor.core.concurrency;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * How many tasks this process is currently allowed to run (PRD 5.5).
 *
 * <p>The value lives outside the worker so an adaptive controller can change it while the engine keeps
 * dispatching. The engine reads {@link #current()} on every polling round, so a decrease takes effect on
 * the next round and never interrupts work that is already running.
 *
 * <p>Clamping to {@code [min, max]} happens here rather than in the controller, so no caller can move the
 * limit outside the configured range.
 */
public final class ConcurrencyLimit {

    private final int min;
    private final int max;
    private final AtomicInteger current;

    public ConcurrencyLimit(int min, int max, int initial) {
        if (min < 1) {
            throw new IllegalArgumentException("minimum concurrency must be at least 1");
        }
        if (max < min) {
            throw new IllegalArgumentException("maximum concurrency must not be smaller than the minimum");
        }
        this.min = min;
        this.max = max;
        this.current = new AtomicInteger(clamp(initial));
    }

    /** A limit that never changes; used while adaptive control is disabled. */
    public static ConcurrencyLimit fixed(int value) {
        return new ConcurrencyLimit(value, value, value);
    }

    public int min() {
        return min;
    }

    public int max() {
        return max;
    }

    public int current() {
        return current.get();
    }

    /** Applies {@code next} clamped to {@code [min, max]} and returns the value now in effect. */
    public int apply(int next) {
        int clamped = clamp(next);
        current.set(clamped);
        return clamped;
    }

    public int clamp(int value) {
        return Math.max(min, Math.min(max, value));
    }
}
