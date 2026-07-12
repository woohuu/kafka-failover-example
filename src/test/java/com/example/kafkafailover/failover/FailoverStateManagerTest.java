package com.example.kafkafailover.failover;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

class FailoverStateManagerTest {

    private FailoverProperties props;
    private MutableClock clock;
    private FailoverStateManager manager;

    /** Simple settable clock so tests can advance time deterministically. */
    static class MutableClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
        void advance(Duration d) { now.updateAndGet(i -> i.plus(d)); }
        @Override public Instant instant() { return now.get(); }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
    }

    @BeforeEach
    void setUp() {
        props = new FailoverProperties();
        props.setFailureThreshold(3);
        props.setFailbackSuccessThreshold(6);
        props.setMinDwell(Duration.ofMinutes(5));
        clock = new MutableClock();
        ApplicationEventPublisher noopPublisher = event -> { };
        manager = new FailoverStateManager(props, noopPublisher, clock);
    }

    @Test
    void staysOnPrimaryBelowFailureThreshold() {
        manager.recordPrimaryResult(false);
        manager.recordPrimaryResult(false);
        assertEquals(KafkaRegion.PRIMARY, manager.getActiveRegion());
    }

    @Test
    void singleSuccessResetsFailureCounter() {
        manager.recordPrimaryResult(false);
        manager.recordPrimaryResult(false);
        manager.recordPrimaryResult(true);
        manager.recordPrimaryResult(false);
        manager.recordPrimaryResult(false);
        assertEquals(KafkaRegion.PRIMARY, manager.getActiveRegion());
    }

    @Test
    void failsOverAtThreshold() {
        failPrimary(3);
        assertEquals(KafkaRegion.SECONDARY, manager.getActiveRegion());
    }

    @Test
    void doesNotFailBackBeforeDwellElapses() {
        failPrimary(3);
        recoverPrimary(6); // healthy long enough, but dwell not elapsed
        assertEquals(KafkaRegion.SECONDARY, manager.getActiveRegion());
    }

    @Test
    void doesNotFailBackBeforeSuccessThreshold() {
        failPrimary(3);
        clock.advance(Duration.ofMinutes(10));
        recoverPrimary(5);
        assertEquals(KafkaRegion.SECONDARY, manager.getActiveRegion());
    }

    @Test
    void failsBackAfterDwellAndConsecutiveSuccesses() {
        failPrimary(3);
        clock.advance(Duration.ofMinutes(10));
        recoverPrimary(6);
        assertEquals(KafkaRegion.PRIMARY, manager.getActiveRegion());
    }

    @Test
    void failureDuringRecoveryResetsSuccessCounter() {
        failPrimary(3);
        clock.advance(Duration.ofMinutes(10));
        recoverPrimary(5);
        manager.recordPrimaryResult(false); // flapping primary
        recoverPrimary(5);
        assertEquals(KafkaRegion.SECONDARY, manager.getActiveRegion());
        manager.recordPrimaryResult(true);  // 6th consecutive success
        assertEquals(KafkaRegion.PRIMARY, manager.getActiveRegion());
    }

    private void failPrimary(int times) {
        for (int i = 0; i < times; i++) {
            manager.recordPrimaryResult(false);
        }
    }

    private void recoverPrimary(int times) {
        for (int i = 0; i < times; i++) {
            manager.recordPrimaryResult(true);
        }
    }
}
