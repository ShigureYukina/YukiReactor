-- Task level decisions have to name the attempt they are about and the values the decision worked
-- with, so a reader can follow "why did this task wait / retry / get taken back" without inferring
-- anything from the task status (PRD 5.6, 7.3).
--
-- Process level decisions (a concurrency adjustment) leave the new columns NULL.
ALTER TABLE scheduler_decision
    ADD COLUMN execution_id      BIGINT      NULL COMMENT 'Attempt the decision is about, NULL for process level decisions' AFTER task_id,
    ADD COLUMN concurrency_limit INT         NULL COMMENT 'Dispatch concurrency in force when the decision was taken' AFTER concurrency_after,
    ADD COLUMN next_run_at       DATETIME(3) NULL COMMENT 'Retry time chosen by the decision, set by RETRY_BACKOFF' AFTER concurrency_limit,
    ADD KEY idx_scheduler_decision_task_id_id (task_id, id);
