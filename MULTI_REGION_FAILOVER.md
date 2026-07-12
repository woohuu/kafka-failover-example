# Spring Cloud Stream Kafka — Multi-Region MSK Failover

Producer-side resiliency for AWS MSK across two regions:

- **Primary**: us-east-1 (all traffic under normal operation)
- **Secondary**: us-west-2 (automatic failover target)

## Architecture

```
                 ┌────────────────────────────────────────────┐
                 │              Application                   │
                 │                                            │
  business code ─► ResilientKafkaProducer ──► StreamBridge    │
                 │        │                     │        │    │
                 │        │ passive send    binder   binder   │
                 │        │ health signal  kafka-   kafka-    │
                 │        ▼                primary  secondary │
                 │  FailoverStateManager      │        │      │
                 │        ▲                   ▼        ▼      │
                 │        │ probe result   MSK        MSK     │
                 │  RegionHealthProber    us-east-1  us-west-2│
                 └────────────────────────────────────────────┘
```

| Component | Responsibility |
|---|---|
| `FailoverStateManager` | Circuit-breaker state machine deciding the active region |
| `RegionHealthProber` | Scheduled `AdminClient.describeCluster()` probe of the primary — enables failback detection |
| `ResilientKafkaProducer` | Routes every send to the active binder via `StreamBridge`; feeds real send outcomes back as passive health signals; per-message fallback to secondary |
| Two Kafka binders | Independent connections to each MSK cluster, always configured |

## Failover / failback semantics (anti-flapping hysteresis)

- **Failover trigger**: `failure-threshold` (default 3) *consecutive* failures — from health probes **or** real sends. A single success resets the counter, so transient blips don't trip it.
- **Failback trigger**: primary must be healthy for `failback-success-threshold` (default 6) *consecutive* probes **and** at least `min-dwell` (default 5m) must have elapsed since the failover. A bouncing primary cannot pull traffic back and forth.
- **Per-message safety**: while the circuit is still counting failures, a message that fails against the primary is immediately retried against the secondary, so nothing is dropped during detection.
- `sync: true` + bounded `delivery.timeout.ms` on the producer binding are **required** — otherwise sends are fire-and-forget and outages are invisible to the passive signal path.

## Configuration

Set the environment variables:

```
MSK_PRIMARY_BROKERS=b-1.mycluster.xxxx.c2.kafka.us-east-1.amazonaws.com:9098,...
MSK_SECONDARY_BROKERS=b-1.mycluster.yyyy.c2.kafka.us-west-2.amazonaws.com:9098,...
```

Tune thresholds under `app.kafka.failover.*` in `application.yml`.

## Usage

```java
@Service
public class OrderEventService {
    private final ResilientKafkaProducer producer;
    ...
    public boolean publishOrderCreated(String orderId, String json) {
        return producer.send("orderEvents-out-0", orderId, json);
    }
}
```

## Notes / operational caveats

- Topics must exist in **both** regions (use MirrorMaker 2, MSK Replicator, or IaC to keep topic configs in sync).
- Messages produced to the secondary during an outage are **not** automatically replayed to the primary on failback — use MSK Replicator / MM2 bidirectional replication if consumers only read one region.
- Consumers need their own failover story (offset translation); this project covers the producer side.
- The `RegionSwitchEvent` Spring event is published on every switch — hook it for alerting/metrics.
