# kafka-failover-example

A Spring Boot / Spring Cloud Stream example demonstrating producer-side
resiliency for AWS MSK across two regions (us-east-1 primary, us-west-2
secondary), with automatic circuit-breaker-style failover and failback.

See [MULTI_REGION_FAILOVER.md](MULTI_REGION_FAILOVER.md) for the full
architecture write-up.

## CI

Every push and pull request to `master` runs the build and test suite via
GitHub Actions (`.github/workflows/ci.yml`).
