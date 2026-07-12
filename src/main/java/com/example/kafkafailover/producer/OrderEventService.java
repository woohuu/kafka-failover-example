package com.example.kafkafailover.producer;

import org.springframework.stereotype.Service;

/**
 * Example business service showing how application code publishes events.
 * It never knows which region is active — routing is fully encapsulated
 * inside {@link ResilientKafkaProducer}.
 */
@Service
public class OrderEventService {

    private final ResilientKafkaProducer producer;

    public OrderEventService(ResilientKafkaProducer producer) {
        this.producer = producer;
    }

    public boolean publishOrderCreated(String orderId, String orderJson) {
        return producer.send("orderEvents-out-0", orderId, orderJson);
    }
}
