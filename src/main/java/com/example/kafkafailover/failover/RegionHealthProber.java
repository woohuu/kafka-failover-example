package com.example.kafkafailover.failover;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Actively probes the primary MSK cluster with a lightweight
 * {@code describeCluster} call and feeds the result into the
 * {@link FailoverStateManager}.
 *
 * Active probing is what allows FAILBACK: once traffic has moved to the
 * secondary region, no sends touch the primary anymore, so passive
 * send-failure signals alone could never detect recovery.
 */
@Component
public class RegionHealthProber {

    private static final Logger log = LoggerFactory.getLogger(RegionHealthProber.class);

    private final FailoverProperties props;
    private final FailoverStateManager stateManager;

    private AdminClient primaryAdmin;

    public RegionHealthProber(FailoverProperties props, FailoverStateManager stateManager) {
        this.props = props;
        this.stateManager = stateManager;
    }

    @PostConstruct
    void init() {
        Map<String, Object> config = new HashMap<>(props.getProbeClientConfig());
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getPrimaryBootstrapServers());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) props.getProbeTimeout().toMillis());
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) props.getProbeTimeout().toMillis());
        // fail fast instead of retrying internally — the state machine owns retry policy
        config.put(AdminClientConfig.RETRIES_CONFIG, 0);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "region-health-prober-primary");
        this.primaryAdmin = AdminClient.create(config);
    }

    @Scheduled(fixedDelayString = "${app.kafka.failover.probe-interval:10s}")
    public void probePrimary() {
        boolean healthy = isClusterReachable(primaryAdmin);
        if (!healthy) {
            log.warn("Primary MSK cluster health probe FAILED");
        } else if (log.isDebugEnabled()) {
            log.debug("Primary MSK cluster health probe OK");
        }
        stateManager.recordPrimaryResult(healthy);
    }

    private boolean isClusterReachable(AdminClient admin) {
        try {
            var result = admin.describeCluster(
                    new DescribeClusterOptions().timeoutMs((int) props.getProbeTimeout().toMillis()));
            var nodes = result.nodes().get(props.getProbeTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return nodes != null && !nodes.isEmpty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    @PreDestroy
    void shutdown() {
        if (primaryAdmin != null) {
            primaryAdmin.close();
        }
    }
}
