package com.yuki.reactor.app;

import com.yuki.reactor.core.service.TaskSubmission;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Idempotent submission (PRD 5.1, 9): concurrent callers sharing an idempotency key must end up with
 * one logical task, and the guarantee must come from the database unique key rather than process
 * memory.
 */
class IdempotencyIT extends AbstractIntegrationTest {

    @Test
    void concurrentSubmissionsOfTheSameKeyCreateExactlyOneTask() throws Exception {
        int submitters = 16;
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<TaskSubmission>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < submitters; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return submit("echo", "{\"sleepMs\": 0}", "checkout", "order-42");
                }));
            }
            startGate.countDown();

            Set<Long> taskIds = new HashSet<>();
            int createdCount = 0;
            for (Future<TaskSubmission> future : futures) {
                TaskSubmission submission = future.get(30, TimeUnit.SECONDS);
                taskIds.add(submission.task().id());
                if (submission.created()) {
                    createdCount++;
                }
            }

            assertThat(taskIds).as("all submitters must see the same logical task").hasSize(1);
            assertThat(createdCount).as("exactly one caller may report creation").isEqualTo(1);
            assertThat(countTasks("checkout", "order-42")).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void differentKeysCreateDifferentTasks() {
        TaskSubmission first = submit("echo", "{}", "checkout", "order-1");
        TaskSubmission second = submit("echo", "{}", "checkout", "order-2");

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isTrue();
        assertThat(first.task().id()).isNotEqualTo(second.task().id());
        assertThat(countTasks("checkout", "order-1")).isEqualTo(1);
        assertThat(countTasks("checkout", "order-2")).isEqualTo(1);
    }

    @Test
    void theSameKeyInAnotherScopeIsADifferentTask() {
        TaskSubmission checkout = submit("echo", "{}", "checkout", "shared-key");
        TaskSubmission billing = submit("echo", "{}", "billing", "shared-key");

        assertThat(checkout.task().id()).isNotEqualTo(billing.task().id());
    }

    @Test
    void submissionsWithoutAKeyAlwaysCreateNewTasks() {
        TaskSubmission first = submit("echo", "{}", null, null);
        TaskSubmission second = submit("echo", "{}", null, null);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isTrue();
        assertThat(first.task().id()).isNotEqualTo(second.task().id());
    }
}
