package com.yuki.reactor.app;

import com.yuki.reactor.app.task.TestHandlersConfiguration;
import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.core.retry.RetryProperties;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Retry admission limit (PRD 5.4): retries must not crowd out fresh work, so the number of retry
 * dispatches per second is capped. With a limit of one per second, three retries are clearly paced
 * instead of being dispatched back to back.
 */
@Import(TestHandlersConfiguration.class)
class RetryAdmissionIT extends AbstractIntegrationTest {

    @Test
    void pacesRetryDispatchWithoutStarvingTheQueue() {
        RetryProperties retry = retryWith(Duration.ofMillis(100), 0.0);
        retry.setMaxDispatchesPerSecond(1);
        startWorker(workerProperties(2), retry);

        List<Long> taskIds = IntStream.range(0, 3)
                .mapToObj(index -> submit("scenario", "{\"failures\": 1}").task().id())
                .toList();

        long startedAt = System.nanoTime();
        for (long taskId : taskIds) {
            awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(60));
            assertThat(countExecutions(taskId)).isEqualTo(2);
        }
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startedAt).toMillis();

        assertThat(elapsedMillis)
                .as("without the admission limit the three retries would be dispatched within ~300ms")
                .isGreaterThanOrEqualTo(1000L);
    }
}
