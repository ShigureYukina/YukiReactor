-- Reactor V1.0 schema.
--
-- MySQL is the single source of truth for task state (PRD 6.1).
-- All timestamps are UTC values written by the application through an injected Clock,
-- stored with millisecond precision.

CREATE TABLE task
(
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    task_type         VARCHAR(64)  NOT NULL COMMENT 'Task type, resolved through TaskHandlerRegistry',
    payload           JSON         NOT NULL COMMENT 'Business parameters; never interpreted as code',
    status            VARCHAR(16)  NOT NULL COMMENT 'PENDING/RUNNING/RETRY_WAIT/SUCCEEDED/FAILED/CANCELLED',
    priority          INT          NOT NULL DEFAULT 100 COMMENT 'Lower value is scheduled first',
    idempotency_scope VARCHAR(64)  NOT NULL DEFAULT 'default' COMMENT 'Business scope that owns the idempotency key',
    idempotency_key   VARCHAR(128) NULL COMMENT 'Client supplied key; unique per scope when present',
    attempt_count     INT          NOT NULL DEFAULT 0,
    max_attempts      INT          NOT NULL DEFAULT 3,
    next_run_at       DATETIME(3)  NOT NULL COMMENT 'Earliest time the task may be claimed',
    timeout_ms        BIGINT       NOT NULL DEFAULT 60000 COMMENT 'Execution timeout; not enforced before phase 2',
    lease_owner       VARCHAR(128) NULL COMMENT 'Worker that currently owns the task',
    lease_until       DATETIME(3)  NULL COMMENT 'Lease expiry; lease enforcement arrives in phase 2',
    fencing_token     BIGINT       NOT NULL DEFAULT 0 COMMENT 'Incremented on every successful claim',
    last_error        VARCHAR(512) NULL COMMENT 'Most recent failure or administrative note',
    created_at        DATETIME(3)  NOT NULL,
    updated_at        DATETIME(3)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_task_idempotency (idempotency_scope, idempotency_key),
    KEY idx_task_dispatch (status, priority, next_run_at),
    KEY idx_task_created_at (created_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

CREATE TABLE task_execution
(
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    task_id       BIGINT       NOT NULL,
    attempt       INT          NOT NULL COMMENT 'Attempt number taken from task.attempt_count at claim time',
    worker_id     VARCHAR(128) NOT NULL,
    fencing_token BIGINT       NOT NULL COMMENT 'Fencing token that owned the task for this attempt',
    started_at    DATETIME(3)  NOT NULL,
    finished_at   DATETIME(3)  NULL COMMENT 'NULL while the attempt is still running',
    result        VARCHAR(16)  NULL COMMENT 'SUCCEEDED/FAILED/ABANDONED; NULL while running',
    error_code    VARCHAR(64)  NULL,
    error_message VARCHAR(512) NULL,
    PRIMARY KEY (id),
    KEY idx_task_execution_task (task_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
