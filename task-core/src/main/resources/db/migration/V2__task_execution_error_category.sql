-- Phase 2: record how a failed attempt was classified so that operators can explain
-- why a task was retried or given up (PRD 5.4).

ALTER TABLE task_execution
    ADD COLUMN error_category VARCHAR(32) NULL
        COMMENT 'ErrorCategory of the failure: INVALID_PARAMETER/BUSINESS_REJECTION/TRANSIENT/THROTTLED/OVERLOADED/TIMEOUT/EXECUTOR_LOST/UNKNOWN'
        AFTER error_code;
