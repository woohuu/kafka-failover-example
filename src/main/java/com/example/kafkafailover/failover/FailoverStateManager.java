package com.example.kafkafailover.failover;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Circuit-breaker-style state machine that decides which region is active.
 *
 * <pre>
 *              N consecutive failures
 *   PRIMARY ─────────────────────────────► SECONDARY
 *      ▲                                       │
 *      │   M consecutive healthy probes        │
 *      └────────── AND minDwell elapsed ◄──────┘
 * </pre>
 *
 * Hysteresis rules that prevent region flapping:
 * <ul>
 *   <li>Failover requires {@code failureThreshold} CONSECUTIVE failures —
 *       one success resets the counter.</li>
 *   <li>Failback requires {@code failbackSuccessThreshold} consecutive
 *       healthy probes of the primary — one failure resets the counter.</li>
 *   <li>Failback is not considered until {@code minDwell} has elapsed since
 *       the failover, so a primary that is bouncing up and down cannot pull
 *       traffic back and forth.</li>
 * </ul>
 *
 * All transitions are serialized on an internal lock; reads are lock-free.
 */
@Component
public class FailoverStateManager {

    private static final Logger log = LoggerFactory.getLogger(FailoverStateManager.class);

    private final FailoverProperties props;
    private final ApplicationEventPublisher events;
    private final Clock clock;

    private final AtomicReference<KafkaRegion> activeRegion = new AtomicReference<>(KafkaRegion.PRIMARY);
    private final AtomicInteger consecutivePrimaryFailures = new AtomicInteger();
    private final AtomicInteger consecutivePrimarySuccesses = new AtomicInteger();
    private volatile Instant failedOverAt;

    private final Object transitionLock = new Object();

    public FailoverStateManager(FailoverProperties props, ApplicationEventPublisher events) {
        this(props, events, Clock.systemUTC());
    }

    FailoverStateManager(FailoverProperties props, ApplicationEventPublisher events, Clock clock) {
        this.props = props;
        this.events = events;
        this.clock = clock;
    }

    /** The region producers should currently send to. */
    public KafkaRegion getActiveRegion() {
        return activeRegion.get();
    }

    public String getActiveBinder() {
        return activeRegion.get() == KafkaRegion.PRIMARY
                ? props.getPrimaryBinder()
                : props.getSecondaryBinder();
    }

    /**
     * Record a health signal for the primary region. Called by both the
     * active prober and the producer's passive send-failure feedback.
     */
    public void recordPrimaryResult(boolean healthy) {
        if (healthy) {
            consecutivePrimaryFailures.set(0);
            int successes = consecutivePrimarySuccesses.incrementAndGet();
            maybeFailback(successes);
        } else {
            consecutivePrimarySuccesses.set(0);
            int failures = consecutivePrimaryFailures.incrementAndGet();
            maybeFailover(failures);
        }
    }

    private void maybeFailover(int failures) {
        if (activeRegion.get() != KafkaRegion.PRIMARY || failures < props.getFailureThreshold()) {
            return;
        }
        synchronized (transitionLock) {
            if (activeRegion.get() != KafkaRegion.PRIMARY) {
                return;
            }
            activeRegion.set(KafkaRegion.SECONDARY);
            failedOverAt = clock.instant();
            log.warn("FAILOVER: primary region unhealthy after {} consecutive failures; "
                    + "switching producers to secondary region ({})", failures, props.getSecondaryBinder());
            events.publishEvent(new RegionSwitchEvent(this, KafkaRegion.SECONDARY));
        }
    }

    private void maybeFailback(int successes) {
        if (activeRegion.get() != KafkaRegion.SECONDARY
                || successes < props.getFailbackSuccessThreshold()
                || !dwellElapsed()) {
            return;
        }
        synchronized (transitionLock) {
            if (activeRegion.get() != KafkaRegion.SECONDARY || !dwellElapsed()) {
                return;
            }
            activeRegion.set(KafkaRegion.PRIMARY);
            failedOverAt = null;
            log.info("FAILBACK: primary region healthy for {} consecutive probes and dwell time "
                    + "elapsed; switching producers back to primary ({})", successes, props.getPrimaryBinder());
            events.publishEvent(new RegionSwitchEvent(this, KafkaRegion.PRIMARY));
        }
    }

    private boolean dwellElapsed() {
        Instant since = failedOverAt;
        return since == null
                || Duration.between(since, clock.instant()).compareTo(props.getMinDwell()) >= 0;
    }
}
