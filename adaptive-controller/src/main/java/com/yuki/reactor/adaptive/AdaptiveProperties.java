package com.yuki.reactor.adaptive;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Boundaries and reaction speeds of the adaptive concurrency controller (PRD 5.5). The controller
 * only ever changes how many tasks this system runs at the same time; it never claims to repair the
 * downstream.
 */
@Getter
@Setter
@ConfigurationProperties("reactor.adaptive")
public class AdaptiveProperties {

    /**
     * Disabled by default: with no adaptive controller the worker keeps its configured fixed
     * concurrency, which is what the first two phases rely on.
     */
    private boolean enabled = false;

    private int minConcurrency = 1;

    private int maxConcurrency = 8;

    private int initialConcurrency = 4;

    /** How often the controller samples the runtime signals. */
    private Duration sampleInterval = Duration.ofSeconds(2);

    /** Average downstream latency above which the system is considered overloaded. */
    private Duration latencyThreshold = Duration.ofMillis(500);

    /** Downstream error rate (and attempt error rate) at or above which the system is overloaded. */
    private double errorRateThreshold = 0.3;

    /** Share of timed out attempts at or above which the system is overloaded. */
    private double timeoutRateThreshold = 0.2;

    /** Concurrency added per increase. */
    private int increaseStep = 1;

    /** Fraction of the current concurrency kept when decreasing. */
    private double decreaseFactor = 0.5;

    /** Hard cap on how much one decrease may take away. */
    private int maxDecreaseStep = 4;

    /** Minimum time between two adjustments. */
    private Duration cooldown = Duration.ofSeconds(10);

    /** Consecutive overloaded samples required before decreasing. */
    private int consecutiveBreachesBeforeDecrease = 2;

    /** How long the signals have to stay healthy before increasing again. */
    private Duration recoveryObservation = Duration.ofSeconds(15);

    /** Concurrency used when the signals cannot be read at all. */
    private int conservativeConcurrency = 1;

    public void validate() {
        if (minConcurrency < 1) {
            throw new IllegalArgumentException("min-concurrency must be at least 1");
        }
        if (maxConcurrency < minConcurrency) {
            throw new IllegalArgumentException("max-concurrency must not be smaller than min-concurrency");
        }
        if (initialConcurrency < minConcurrency || initialConcurrency > maxConcurrency) {
            throw new IllegalArgumentException("initial-concurrency must be between min-concurrency and max-concurrency");
        }
        if (sampleInterval == null || sampleInterval.isZero() || sampleInterval.isNegative()) {
            throw new IllegalArgumentException("sample-interval must be positive");
        }
        if (latencyThreshold == null || latencyThreshold.isNegative()) {
            throw new IllegalArgumentException("latency-threshold must not be negative");
        }
        if (errorRateThreshold <= 0 || errorRateThreshold > 1) {
            throw new IllegalArgumentException("error-rate-threshold must be in (0, 1]");
        }
        if (timeoutRateThreshold <= 0 || timeoutRateThreshold > 1) {
            throw new IllegalArgumentException("timeout-rate-threshold must be in (0, 1]");
        }
        if (increaseStep < 1) {
            throw new IllegalArgumentException("increase-step must be at least 1");
        }
        if (decreaseFactor <= 0 || decreaseFactor >= 1) {
            throw new IllegalArgumentException("decrease-factor must be in (0, 1)");
        }
        if (maxDecreaseStep < 1) {
            throw new IllegalArgumentException("max-decrease-step must be at least 1");
        }
        if (cooldown == null || cooldown.isNegative()) {
            throw new IllegalArgumentException("cooldown must not be negative");
        }
        if (consecutiveBreachesBeforeDecrease < 1) {
            throw new IllegalArgumentException("consecutive-breaches-before-decrease must be at least 1");
        }
        if (recoveryObservation == null || recoveryObservation.isNegative()) {
            throw new IllegalArgumentException("recovery-observation must not be negative");
        }
        if (conservativeConcurrency < 1) {
            throw new IllegalArgumentException("conservative-concurrency must be at least 1");
        }
    }
}
