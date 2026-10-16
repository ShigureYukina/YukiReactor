package com.yuki.reactor.observability.exception;

/**
 * Raised when a round is recorded twice for the same experiment, strategy and round number.
 *
 * <p>The uniqueness is enforced by the database key {@code uk_experiment_run_round}, so a repeated
 * submission is a conflict and never an overwrite: a stored round is evidence and must not be
 * silently replaced (PRD 7.4).
 */
public class DuplicateExperimentRoundException extends RuntimeException {

    public DuplicateExperimentRoundException(String experimentName, String strategy, int roundIndex) {
        super("experiment '" + experimentName + "' already recorded " + strategy + " round " + roundIndex);
    }
}
