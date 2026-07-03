package com.example.kafkafailover.failover;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tuning knobs for multi-region failover behavior.
 *
 * The defaults implement the industry-standard "circuit breaker with
 * hysteresis" pattern:
 *   - failover only after {@code failureThreshold} CONSECUTIVE failures
 *     (avoids reacting to a single transient blip)
 *   - failback only after the primary has been healthy for
 *     {@code failbackSuccessThreshold} consecutive probes AND we have spent
 *     at least {@code minDwell} on the secondary (prevents region flapping)
 */
@ConfigurationProperties(prefix = "app.kafka.failover")
public class FailoverProperties {

    /** Binder name for the primary region (us-east-1). */
    private String primaryBinder = "kafka-primary";

    /** Binder name for the secondary region (us-west-2). */
    private String secondaryBinder = "kafka-secondary";

    /** Bootstrap servers of the primary MSK cluster, for health probes. */
    private String primaryBootstrapServers;

    /** Bootstrap servers of the secondary MSK cluster, for health probes. */
    private String secondaryBootstrapServers;

    /** How often the active health probe runs. */
    private Duration probeInterval = Duration.ofSeconds(10);

    /** Timeout for a single AdminClient describeCluster probe. */
    private Duration probeTimeout = Duration.ofSeconds(5);

    /** Consecutive failures (probe or send) before failing over. */
    private int failureThreshold = 3;

    /** Consecutive successful primary probes required before failing back. */
    private int failbackSuccessThreshold = 6;

    /**
     * Minimum time to stay on the secondary region after a failover before
     * a failback is even considered (anti-flapping dwell time).
     */
    private Duration minDwell = Duration.ofMinutes(5);

    /**
     * Extra Kafka client properties for the health-probe AdminClient
     * (security.protocol, sasl.* for MSK IAM auth, etc.). Must match the
     * security settings of the binders, otherwise probes will always fail.
     */
    private java.util.Map<String, String> probeClientConfig = new java.util.HashMap<>();

    public String getPrimaryBinder() { return primaryBinder; }
    public void setPrimaryBinder(String primaryBinder) { this.primaryBinder = primaryBinder; }

    public String getSecondaryBinder() { return secondaryBinder; }
    public void setSecondaryBinder(String secondaryBinder) { this.secondaryBinder = secondaryBinder; }

    public String getPrimaryBootstrapServers() { return primaryBootstrapServers; }
    public void setPrimaryBootstrapServers(String v) { this.primaryBootstrapServers = v; }

    public String getSecondaryBootstrapServers() { return secondaryBootstrapServers; }
    public void setSecondaryBootstrapServers(String v) { this.secondaryBootstrapServers = v; }

    public Duration getProbeInterval() { return probeInterval; }
    public void setProbeInterval(Duration probeInterval) { this.probeInterval = probeInterval; }

    public Duration getProbeTimeout() { return probeTimeout; }
    public void setProbeTimeout(Duration probeTimeout) { this.probeTimeout = probeTimeout; }

    public int getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(int failureThreshold) { this.failureThreshold = failureThreshold; }

    public int getFailbackSuccessThreshold() { return failbackSuccessThreshold; }
    public void setFailbackSuccessThreshold(int v) { this.failbackSuccessThreshold = v; }

    public Duration getMinDwell() { return minDwell; }
    public void setMinDwell(Duration minDwell) { this.minDwell = minDwell; }

    public java.util.Map<String, String> getProbeClientConfig() { return probeClientConfig; }
    public void setProbeClientConfig(java.util.Map<String, String> v) { this.probeClientConfig = v; }
}
