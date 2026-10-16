-- Scheduling and control decisions, stored so a later reader can explain them (PRD 5.6, 7.3).
-- Only real decisions are written here; high frequency metrics belong to the metric system, which is why
-- the table has no sampling history.
CREATE TABLE scheduler_decision
(
    id                     BIGINT       NOT NULL AUTO_INCREMENT,
    task_id                BIGINT       NULL COMMENT 'NULL for process level decisions such as a concurrency adjustment',
    decision_type          VARCHAR(32)  NOT NULL COMMENT 'CONCURRENCY_ADJUSTMENT',
    reason_code            VARCHAR(64)  NOT NULL COMMENT 'Reason code of the decision, see PRD 5.6',
    worker_id              VARCHAR(128) NULL COMMENT 'Worker instance that took the decision',
    concurrency_before     INT          NULL,
    concurrency_after      INT          NULL,
    running_tasks          INT          NULL COMMENT 'Tasks in RUNNING state when the decision was taken',
    backlog                INT          NULL COMMENT 'Tasks due for execution when the decision was taken',
    downstream_latency_ms  DOUBLE       NULL,
    downstream_error_rate  DOUBLE       NULL,
    observed_details       VARCHAR(512) NULL,
    created_at             DATETIME(3)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_scheduler_decision_created_at (created_at),
    KEY idx_scheduler_decision_task_id (task_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
