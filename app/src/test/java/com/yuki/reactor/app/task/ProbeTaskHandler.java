package com.yuki.reactor.app.task;

import com.yuki.reactor.core.handler.TaskContext;
import com.yuki.reactor.core.handler.TaskHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test only task type that records the highest number of simultaneous executions, so the
 * per-task-type concurrency limit can be asserted directly instead of inferred from timings.
 */
public class ProbeTaskHandler implements TaskHandler {

    public static final String TASK_TYPE = "probe";

    private static final AtomicInteger ACTIVE = new AtomicInteger();
    private static final AtomicInteger MAX_OBSERVED = new AtomicInteger();

    public static void reset() {
        ACTIVE.set(0);
        MAX_OBSERVED.set(0);
    }

    public static int maxObservedConcurrency() {
        return MAX_OBSERVED.get();
    }

    @Override
    public String taskType() {
        return TASK_TYPE;
    }

    @Override
    public void execute(TaskContext context) throws Exception {
        int active = ACTIVE.incrementAndGet();
        MAX_OBSERVED.accumulateAndGet(active, Math::max);
        try {
            Thread.sleep(context.payload().path("sleepMs").asLong(100L));
        } finally {
            ACTIVE.decrementAndGet();
        }
    }
}
