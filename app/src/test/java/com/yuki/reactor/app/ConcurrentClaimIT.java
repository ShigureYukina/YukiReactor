package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.service.TaskClaim;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Multiple executors competing for the same queue (PRD 5.2, 9): every task is claimed exactly once,
 * and losing a race never leaves a partial write behind.
 */
class ConcurrentClaimIT extends AbstractIntegrationTest {

    @Test
    void competingClaimersTakeEachTaskExactlyOnce() throws Exception {
        int taskCount = 30;
        Set<Long> submittedIds = new LinkedHashSet<>();
        for (int i = 0; i < taskCount; i++) {
            submittedIds.add(submit("echo", "{}").task().id());
        }

        int competitors = 4;
        Set<Long> claimedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicateClaims = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(competitors);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int worker = 0; worker < competitors; worker++) {
                String workerId = "claim-worker-" + worker;
                futures.add(pool.submit(() -> {
                    startGate.await();
                    while (true) {
                        LocalDateTime now = LocalDateTime.now(clock);
                        List<Task> candidates = taskOperations.findClaimable(now, 5);
                        if (candidates.isEmpty()) {
                            return null;
                        }
                        boolean claimedSomething = false;
                        for (Task candidate : candidates) {
                            TaskClaim claim = taskOperations.claimAndOpen(
                                    candidate.getId(), workerId, now, Duration.ofSeconds(30));
                            if (claim == null) {
                                continue;
                            }
                            if (!claimedIds.add(candidate.getId())) {
                                duplicateClaims.incrementAndGet();
                            }
                            claimedSomething = true;
                        }
                        if (!claimedSomething) {
                            return null;
                        }
                    }
                }));
            }
            startGate.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(duplicateClaims).as("a task must never be claimed twice").hasValue(0);
        assertThat(claimedIds).containsExactlyInAnyOrderElementsOf(submittedIds);
        for (Long taskId : submittedIds) {
            Task task = taskOperations.get(taskId);
            assertThat(task.getStatus()).isEqualTo(TaskStatus.RUNNING);
            assertThat(task.getFencingToken()).as("one successful claim must advance the token once").isEqualTo(1L);
            assertThat(task.getAttemptCount()).isEqualTo(1);
            assertThat(countExecutions(taskId)).as("each claim opens exactly one execution row").isEqualTo(1);
        }
    }
}
