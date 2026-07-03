package com.example.kafkafailover.failover;

/** The two MSK regions the application can produce to. */
public enum KafkaRegion {
    PRIMARY,    // us-east-1
    SECONDARY   // us-west-2
}
