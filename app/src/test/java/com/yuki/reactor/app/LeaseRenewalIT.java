package com.yuki.reactor.app;

import com.yuki.reactor.core.domain.TaskStatus;
import com.yuki.reactor.worker.TaskWorkerProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lease heartbeat (PRD 5.3). A task that runs longer than its lease must not lose ownership: the
 * worker renews the lease while it is executing, so no other worker (and no recovery scan) takes the
 * task away and the task is executed exactly once.
 */
class LeaseRenewalIT extends AbstractIntegrationTest {

    @Test
    void heartbeatKeepsALongRunningTaskAlive() throws InterruptedException {
        TaskWorkerProperties properties = workerProperties(1);
        properties.setLeaseTtl(Duration.ofSeconds(3));
        properties.setLeaseRenewInterval(Duration.ofSeconds(1));
        startWorker(properties);

        long taskId = submit("echo", "{\"sleepMs\": 7000}").task().id();
        awaitStatus(taskId, TaskStatus.RUNNING, Duration.ofSeconds(10));

        LocalDateTime firstLease = leaseUntil(taskId);
        Thread.sleep(2000L);
        LocalDateTime secondLease = leaseUntil(taskId);

        assertThat(secondLease)
                .as("the lease must be pushed forward while the handler is still running")
                .isAfter(firstLease);
        assertThat(scanOnce().recoveredTasks()).as("no lease may look expired").isZero();
        assertThat(task(taskId).status()).isEqualTo(TaskStatus.RUNNING);

        awaitStatus(taskId, TaskStatus.SUCCEEDED, Duration.ofSeconds(30));
        assertThat(countExecutions(taskId)).as("exactly one attempt ran").isEqualTo(1);
    }

    private LocalDateTime leaseUntil(long taskId) {
        return jdbcTemplate.queryForObject("SELECT lease_until FROM task WHERE id = ?", LocalDateTime.class, taskId);
    }
}
