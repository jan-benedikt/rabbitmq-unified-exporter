# Architecture

## Overview

`rabbitmq-unified-exporter` is a central multi-target exporter that:

1. Proxies and sanitizes native RabbitMQ Prometheus exposition.
2. Adapts Management HTTP API JSON to the same metric names (where fidelity allows).
3. Runs active TCP/AMQP/health checks without Blackbox Exporter.
4. Serves a hybrid scrape model for near-realtime values.

```text
Prometheus / VictoriaMetrics
        |  GET /metrics  or  /metrics/{group}  or  /probe?target=
        v
+---------------------------+
| Metrics HTTP Server       |
| Snapshot store (fresh)    |
+-------------+-------------+
              |
+-------------v-------------+
| Scheduler (per target)    |
| jitter, semaphore,        |
| singleflight, no-overlap  |
+------+------+------+------+
       |      |      |
   Prometheus JSON  Probes
   collector  adapter AMQP/TCP
       |      |      |
       v      v      v
   RabbitMQ brokers (N instances)
```

## Components

| Package | Responsibility |
|---|---|
| `internal/config` | YAML load, inheritance, validation, sharding, secrets `*_file` |
| `internal/scheduler` | Independent loops, singleflight for `/probe` |
| `internal/promsrc` | Fetch + tolerant sanitizer |
| `internal/mgmt` | Management API client, pagination, capability discovery |
| `internal/mapping` | Versioned JSON→Prometheus rules |
| `internal/health` | Cluster/quorum/vhost/application metrics |
| `internal/probes` | AMQP TCP/auth/delivery/TLS expiry |
| `internal/collector` | Orchestrates sources per target |
| `internal/model` | Families, merge, labels, snapshot store |
| `internal/server` | HTTP endpoints |
| `internal/selfmetrics` | `rabbitmq_unified_exporter_*` |

## Realtime model

- Background scheduler updates snapshots on `scrape_interval` (min practical ~5s; RabbitMQ stats interval is typically 5s).
- `/metrics` and `/metrics/{group}` return the last fresh snapshot (no synchronous fan-out).
- `/probe?target=` triggers an immediate singleflight collection for one broker.
- Stale snapshots older than `max_staleness` (default 2× interval) are omitted so Prometheus marks series stale.

## Failure semantics

- One target failure never blocks others.
- **Prometheus and Management run in parallel with independent `scrape_timeout` deadlines** — a slow Management API does not cancel `/metrics` scrapes.
- Within Prometheus, each configured endpoint (`/metrics`, `/metrics/detailed`, …) is scraped in parallel with its own timeout.
- Within Management, overview runs first; nodes/queues/vhosts/federation/shovels and health checks run in parallel afterwards.
- Failed required sources set `collection_success=0`. The last **successful** snapshot remains on `/metrics` until `max_staleness` expires (same idea as a failed direct scrape: Prometheus keeps the previous samples).
- `/readyz` depends only on loaded config + running scheduler (not broker health).
- `/healthz` is process liveness only.

## Labels

Priority: `defaults.labels` < `group.labels` < `target.labels` < exporter `instance` < intrinsic data labels (`vhost`, `queue`, …).

Intrinsic labels cannot be set in config. Scrape Prometheus with `honor_labels: true` / ServiceMonitor `honorLabels: true`.
