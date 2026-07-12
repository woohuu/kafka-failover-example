package com.example.kafkafailover.failover;

import org.springframework.context.ApplicationEvent;

/** Published whenever the active producing region changes. */
public class RegionSwitchEvent extends ApplicationEvent {

    private final KafkaRegion newRegion;

    public RegionSwitchEvent(Object source, KafkaRegion newRegion) {
        super(source);
        this.newRegion = newRegion;
    }

    public KafkaRegion getNewRegion() {
        return newRegion;
    }
}
