-- Experiment records (PRD 7.4). One row per round of one experiment, so the comparison between the
-- fixed and the adaptive strategy keeps every round it ran instead of a hand picked best one.
--
-- The same experiment may be run many times: (experiment_name, strategy, round_index) identifies a
-- round, and the round keeps its configuration, seed, environment and result snapshot as data.
CREATE TABLE experiment_run
(
    id                 BIGINT        NOT NULL AUTO_INCREMENT,
    experiment_name    VARCHAR(128)  NOT NULL COMMENT 'Experiment identity shared by every round of the comparison',
    round_index        INT           NOT NULL COMMENT '1 based round number inside the experiment',
    strategy           VARCHAR(32)   NOT NULL COMMENT 'FIXED or ADAPTIVE scheduling strategy under test',
    task_count         INT           NOT NULL COMMENT 'Number of logical tasks the round submitted',
    concurrency_config JSON          NOT NULL COMMENT 'Concurrency configuration snapshot in force during the round',
    fault_config       JSON          NULL COMMENT 'Fault injection parameters, NULL when nothing was injected',
    random_seed        BIGINT        NULL COMMENT 'Seed of the injected randomness, NULL for a deterministic round',
    environment        JSON          NOT NULL COMMENT 'Runtime the round ran on: host, os, java, app version, commit',
    started_at         DATETIME(3)   NOT NULL,
    finished_at        DATETIME(3)   NULL COMMENT 'NULL while the round is still running',
    result_summary     JSON          NULL COMMENT 'Measured metrics and final outcome of the round',
    notes              VARCHAR(1024) NULL COMMENT 'Free form remarks, for example why a round is not comparable',
    created_at         DATETIME(3)   NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_experiment_run_round (experiment_name, strategy, round_index),
    KEY idx_experiment_run_started_at (started_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
