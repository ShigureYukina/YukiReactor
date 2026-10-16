package com.yuki.reactor.recovery;

import com.yuki.reactor.core.domain.Task;
import com.yuki.reactor.core.domain.TaskExecution;
import com.yuki.reactor.core.mapper.TaskExecutionMapper;
import com.yuki.reactor.core.mapper.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

/**
 * One pass of the recovery scanner: tasks whose executor disappeared are put back into the dispatch
 * flow, and attempts that were left open are closed.
 *
 * <p>The scanner itself is stateless and safe to run on several workers at the same time: every
 * decision is applied with a conditional update, so at most one scanner wins per task.
 */
@Service
public class LeaseRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(LeaseRecoveryService.class);

    private final TaskMapper taskMapper;
    private final TaskExecutionMapper executionMapper;
    private final ExpiredLeaseReclaimer reclaimer;
    private final RecoveryProperties properties;
    private final Clock clock;

    public LeaseRecoveryService(TaskMapper taskMapper,
                               TaskExecutionMapper executionMapper,
                               ExpiredLeaseReclaimer reclaimer,
                               RecoveryProperties properties,
                               Clock clock) {
        this.taskMapper = taskMapper;
        this.executionMapper = executionMapper;
        this.reclaimer = reclaimer;
        this.properties = properties;
        this.clock = clock;
    }

    public RecoveryScanResult scanOnce() {
        LocalDateTime now = LocalDateTime.now(clock);
        int reclaimedForRetry = 0;
        int reclaimedAsFailed = 0;

        List<Task> expired = taskMapper.findExpiredLeases(now, properties.effectiveBatchSize());
        for (Task task : expired) {
            RecoveryOutcome outcome = reclaimer.reclaimExpiredLease(task.getId(), task.getFencingToken(), now);
            switch (outcome) {
                case RECLAIMED_FOR_RETRY -> reclaimedForRetry++;
                case RECLAIMED_AS_FAILED -> reclaimedAsFailed++;
                case SKIPPED -> { }
            }
        }
        if (reclaimedForRetry + reclaimedAsFailed > 0) {
            log.info("recovery scan at {} reclaimed {} tasks ({} for retry, {} failed)",
                    now, reclaimedForRetry + reclaimedAsFailed, reclaimedForRetry, reclaimedAsFailed);
        }

        int closedOrphans = closeOrphanExecutions(now);
        return new RecoveryScanResult(reclaimedForRetry, reclaimedAsFailed, closedOrphans);
    }

    private int closeOrphanExecutions(LocalDateTime now) {
        if (!properties.isOrphanSweepEnabled()) {
            return 0;
        }
        List<TaskExecution> orphans = executionMapper.findOrphanOpenExecutions(properties.effectiveBatchSize());
        int closed = 0;
        for (TaskExecution orphan : orphans) {
            if (reclaimer.closeOrphanExecution(orphan.getId(), now)) {
                closed++;
            }
        }
        if (closed > 0) {
            log.info("recovery scan at {} closed {} attempts left open by a task that moved on", now, closed);
        }
        return closed;
    }
}
