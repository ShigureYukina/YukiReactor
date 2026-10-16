package com.yuki.reactor.adaptive;

import com.yuki.reactor.core.concurrency.ConcurrencyLimit;
import com.yuki.reactor.core.domain.DecisionType;
import com.yuki.reactor.core.domain.ReasonCodes;
import com.yuki.reactor.core.mapper.TaskMapper;
import com.yuki.reactor.core.signals.RuntimeSignals;
import com.yuki.reactor.core.signals.SignalSnapshot;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * AIMD controller: additive increase while the observed signals are healthy, multiplicative decrease
 * as soon as the downstream looks overloaded (PRD 5.5).
 *
 * <p>It only changes the execution concurrency this system controls. Every real change and every
 * fallback is written as a scheduling decision with the reason code, the concurrency before and
 * after, the backlog and the downstream signals that were visible at that moment (PRD 5.6).
 *
 * <p>Safety rails: the value is always clamped to {@code [min, max]}, a decrease needs consecutive
 * breaches, two adjustments are separated by a cooldown, one decrease is capped, and an increase
 * requires the signals to stay healthy for the whole recovery observation window. When the metrics
 * cannot be read at all, the controller falls back to the conservative concurrency instead of
 * guessing.
 */
public class AimdController implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AimdController.class);

    private static final Logger DECISION_LOG = LoggerFactory.getLogger(ConcurrencyDecisionRecorder.class);

    private final AdaptiveProperties properties;
    private final ConcurrencyLimit limit;
    private final RuntimeSignals signals;
    private final TaskMapper taskMapper;
    private final ConcurrencyDecisionRecorder recorder;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final String controllerId;

    private final Object monitor = new Object();

    /** Set while the signals have been healthy without interruption; null means "not healthy yet". */
    private LocalDateTime healthySince;

    /** When the limit was changed for the last time; null while nothing has changed yet. */
    private LocalDateTime lastAdjustmentAt;

    private int consecutiveBreaches;

    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public AimdController(AdaptiveProperties properties, ConcurrencyLimit limit, RuntimeSignals signals,
                          TaskMapper taskMapper, ConcurrencyDecisionRecorder recorder, MeterRegistry meterRegistry,
                          Clock clock) {
        this(properties, limit, signals, taskMapper, recorder, meterRegistry, clock, null);
    }

    public AimdController(AdaptiveProperties properties, ConcurrencyLimit limit, RuntimeSignals signals,
                          TaskMapper taskMapper, ConcurrencyDecisionRecorder recorder, MeterRegistry meterRegistry,
                          Clock clock, String controllerIdOverride) {
        this.properties = properties;
        properties.validate();
        this.limit = limit;
        this.signals = signals;
        this.taskMapper = taskMapper;
        this.recorder = recorder;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        this.controllerId = controllerIdOverride == null || controllerIdOverride.isBlank()
                ? defaultControllerId()
                : controllerIdOverride;
        Gauge.builder("reactor.adaptive.concurrency.limit", limit, ConcurrencyLimit::current)
                .description("Concurrency limit currently used by the worker")
                .register(meterRegistry);
        Gauge.builder("reactor.adaptive.concurrency.max", limit, ConcurrencyLimit::max)
                .description("Upper bound of the concurrency limit")
                .register(meterRegistry);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        long interval = Math.max(1L, properties.getSampleInterval().toMillis());
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "reactor-adaptive");
            thread.setDaemon(false);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::sampleQuietly, interval, interval, TimeUnit.MILLISECONDS);
        running = true;
        log.info("adaptive controller {} started: sampleInterval={} concurrencyLimit={} (min {} max {}) latencyThreshold={} "
                        + "errorRateThreshold={} timeoutRateThreshold={} increaseStep={} decreaseFactor={} "
                        + "maxDecreaseStep={} cooldown={} breachesBeforeDecrease={} recoveryObservation={} conservative={}",
                controllerId, properties.getSampleInterval(), limit.current(), limit.min(), limit.max(),
                properties.getLatencyThreshold(), properties.getErrorRateThreshold(), properties.getTimeoutRateThreshold(),
                properties.getIncreaseStep(), properties.getDecreaseFactor(), properties.getMaxDecreaseStep(),
                properties.getCooldown(), properties.getConsecutiveBreachesBeforeDecrease(),
                properties.getRecoveryObservation(), properties.getConservativeConcurrency());
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        log.info("adaptive controller {} stopped at concurrency {}", controllerId, limit.current());
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    void sampleQuietly() {
        try {
            sampleOnce();
        } catch (Exception e) {
            log.warn("adaptive controller {} could not complete a sampling round: {}", controllerId, e.toString());
        }
    }

    /**
     * Performs one control cycle. Public so tests and experiments can drive the controller without
     * waiting for the sampler.
     */
    public void sampleOnce() {
        LocalDateTime now = LocalDateTime.now(clock);
        SignalSnapshot snapshot = null;
        Long running = null;
        Long backlog = null;
        Decision decision;
        try {
            snapshot = signals.snapshot();
            running = taskMapper.countRunning();
            backlog = taskMapper.countDueBacklog(now);
            decision = decide(snapshot, running, backlog, now);
        } catch (Exception e) {
            decision = unavailable("could not read runtime signals: " + e, now);
        }
        if (decision == null) {
            return;
        }
        int applied = limit.apply(decision.target());
        if (applied == decision.before()) {
            return;
        }
        synchronized (monitor) {
            lastAdjustmentAt = now;
        }
        meterRegistry.counter("reactor.adaptive.concurrency.adjustments", "reason", decision.reasonCode()).increment();
        recorder.record(DecisionType.CONCURRENCY_ADJUSTMENT, decision.reasonCode(), controllerId, decision.before(),
                applied, decision.runningTasks(), decision.backlog(), decision.downstreamLatencyMs(),
                decision.downstreamErrorRate(), decision.details());
        if (snapshot == null) {
            DECISION_LOG.debug("adaptive decision {} applied without a usable signal snapshot", decision.reasonCode());
        }
    }

    /**
     * The whole control law. Returns {@code null} when the controller deliberately does nothing.
     */
    private Decision decide(SignalSnapshot snapshot, Long running, Long backlog, LocalDateTime now) {
        synchronized (monitor) {
            int current = limit.current();
            if (!snapshot.measured()) {
                if ((running != null && running > 0) || (backlog != null && backlog > 0)) {
                    return unavailableLocked("no execution samples while " + running + " task(s) are running and "
                            + backlog + " are due", current, running, backlog, now);
                }
                return null;
            }
            if (isOverloaded(snapshot)) {
                healthySince = null;
                consecutiveBreaches++;
                if (consecutiveBreaches < properties.getConsecutiveBreachesBeforeDecrease()) {
                    return null;
                }
                if (!cooldownElapsed(now)) {
                    return null;
                }
                int target = limit.clamp(Math.max(limit.min(),
                        Math.max(current - properties.getMaxDecreaseStep(),
                                (int) Math.floor(current * properties.getDecreaseFactor()))));
                if (target >= current) {
                    return null;
                }
                consecutiveBreaches = 0;
                return new Decision(ReasonCodes.DOWNSTREAM_OVERLOAD, current, target,
                        "downstream overload suspected, observed " + describe(snapshot), running, backlog,
                        snapshot.averageDownstreamLatencyMillis(), snapshot.downstreamErrorRate());
            }
            consecutiveBreaches = 0;
            if (healthySince == null) {
                healthySince = now;
                return null;
            }
            if (Duration.between(healthySince, now).compareTo(properties.getRecoveryObservation()) < 0) {
                return null;
            }
            if (!cooldownElapsed(now)) {
                return null;
            }
            int target = limit.clamp(Math.min(limit.max(), current + properties.getIncreaseStep()));
            if (target <= current) {
                return null;
            }
            healthySince = now;
            return new Decision(ReasonCodes.RECOVERY_OBSERVED, current, target,
                    "downstream stayed healthy for the whole observation window, observed " + describe(snapshot),
                    running, backlog, snapshot.averageDownstreamLatencyMillis(), snapshot.downstreamErrorRate());
        }
    }

    private Decision unavailable(String detail, LocalDateTime now) {
        synchronized (monitor) {
            return unavailableLocked(detail, limit.current(), null, null, now);
        }
    }

    private Decision unavailableLocked(String detail, int current, Long running, Long backlog, LocalDateTime now) {
        healthySince = null;
        int target = limit.clamp(properties.getConservativeConcurrency());
        if (target == current) {
            return null;
        }
        return new Decision(ReasonCodes.METRICS_UNAVAILABLE, current, target, detail, running, backlog, null, null);
    }

    private boolean isOverloaded(SignalSnapshot snapshot) {
        double latencyThresholdMillis = properties.getLatencyThreshold().toMillis();
        if (snapshot.downstreamCalls() > 0
                && (snapshot.averageDownstreamLatencyMillis() > latencyThresholdMillis
                || snapshot.downstreamErrorRate() >= properties.getErrorRateThreshold())) {
            return true;
        }
        return snapshot.attempts() > 0
                && (snapshot.timeoutRate() >= properties.getTimeoutRateThreshold()
                || snapshot.attemptErrorRate() >= properties.getErrorRateThreshold());
    }

    private boolean cooldownElapsed(LocalDateTime now) {
        return lastAdjustmentAt == null
                || Duration.between(lastAdjustmentAt, now).compareTo(properties.getCooldown()) >= 0;
    }

    private static String describe(SignalSnapshot snapshot) {
        return String.format("attempts=%d downstreamCalls=%d downstreamLatencyMs=%.1f downstreamErrorRate=%.3f "
                        + "attemptErrorRate=%.3f timeoutRate=%.3f queueWaitMs=%.1f",
                snapshot.attempts(), snapshot.downstreamCalls(), snapshot.averageDownstreamLatencyMillis(),
                snapshot.downstreamErrorRate(), snapshot.attemptErrorRate(), snapshot.timeoutRate(),
                snapshot.averageQueueWaitMillis());
    }

    public String getControllerId() {
        return controllerId;
    }

    private static String defaultControllerId() {
        String host = "unknown-host";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignored) {
            // hostname is only used for the audit trail
        }
        String suffix = Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000000));
        String id = host + "-" + ProcessHandle.current().pid() + "-" + suffix;
        return id.length() <= 128 ? id : id.substring(id.length() - 128);
    }

    private record Decision(String reasonCode, int before, int target, String details, Long runningTasks, Long backlog,
                            Double downstreamLatencyMs, Double downstreamErrorRate) {
    }
}
