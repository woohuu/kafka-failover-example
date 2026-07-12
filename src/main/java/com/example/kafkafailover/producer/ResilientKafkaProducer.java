package com.example.kafkafailover.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.example.kafkafailover.failover.FailoverProperties;
import com.example.kafkafailover.failover.FailoverStateManager;
import com.example.kafkafailover.failover.KafkaRegion;

/**
 * Region-aware producer facade.
 *
 * Every send is routed through {@link StreamBridge} to whichever binder the
 * {@link FailoverStateManager} says is active. Send outcomes against the
 * primary are fed back into the state manager as passive health signals, so
 * an outage is detected from real traffic even between health probes.
 *
 * If a send fails while the primary is still active, the message is
 * immediately retried once against the secondary region so the individual
 * message is not lost while the circuit is still counting failures.
 */
@Component
public class ResilientKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(ResilientKafkaProducer.class);

    private final StreamBridge streamBridge;
    private final FailoverStateManager stateManager;
    private final FailoverProperties props;

    public ResilientKafkaProducer(StreamBridge streamBridge,
                                  FailoverStateManager stateManager,
                                  FailoverProperties props) {
        this.streamBridge = streamBridge;
        this.stateManager = stateManager;
        this.props = props;
    }

    /**
     * Send a payload to the given binding (e.g. "orders-out-0"), keyed for
     * partition affinity, via the currently active region.
     *
     * @return true if the message was accepted by a broker in either region
     */
    public <T> boolean send(String bindingName, String key, T payload) {
        Message<T> message = MessageBuilder.withPayload(payload)
                .setHeader("kafka_messageKey", key)
                .build();
        return send(bindingName, message);
    }

    public boolean send(String bindingName, Message<?> message) {
        KafkaRegion region = stateManager.getActiveRegion();
        String binder = stateManager.getActiveBinder();

        boolean sent = trySend(binder, bindingName, message);

        if (region == KafkaRegion.PRIMARY) {
            // passive health signal from real traffic
            stateManager.recordPrimaryResult(sent);

            if (!sent) {
                // per-message fallback so this message survives even though
                // the circuit may not have tripped yet
                log.warn("Send to primary failed for binding {}; retrying against secondary", bindingName);
                sent = trySend(props.getSecondaryBinder(), bindingName, message);
            }
        }

        if (!sent) {
            log.error("Send failed in both regions for binding {}", bindingName);
        }
        return sent;
    }

    private boolean trySend(String binder, String bindingName, Message<?> message) {
        try {
            return streamBridge.send(bindingName, binder, message);
        } catch (Exception e) {
            log.warn("Send via binder {} failed: {}", binder, e.getMessage());
            return false;
        }
    }
}
