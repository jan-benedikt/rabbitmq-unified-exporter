# rabbitmq-unified-exporter

[![CI](https://github.com/jan-benedikt/rabbitmq-unified-exporter/actions/workflows/ci.yaml/badge.svg)](https://github.com/jan-benedikt/rabbitmq-unified-exporter/actions/workflows/ci.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Multi-target RabbitMQ exporter for Prometheus.

## Capabilities

1. **Prometheus proxy + sanitization** — scrapes RabbitMQ `/metrics`, `/metrics/detailed`, optional `/metrics/per-object` and `/metrics/memory-breakdown`; rewrites `rabbitmq_detailed_*` to classic `rabbitmq_*` names (plus legacy raft aliases for dashboards); drops invalid sample values such as `unknown` without replacing them with `0`. Exposes split scrape paths (`/metrics/self`, `/metrics/targets/{group}/{name}/{prometheus|management|probes}`) so Prometheus/VictoriaMetrics can scrape targets in parallel; Helm ServiceMonitor endpoints are auto-generated from `config.groups`.
2. **JSON adapter** — maps Management HTTP API fields to the **same** Prometheus metric names with an explicit fidelity matrix (`exact` / `approximate` / `unsupported`).
3. **Active health checks** — cluster size, partitions, quorum queues, vhosts, AMQP TCP/auth, optional end-to-end delivery test (no Blackbox Exporter).
4. **Multi-target** — one exporter scrapes many brokers with group/target label inheritance.
5. **Hybrid realtime** — `/metrics` serves the last fresh snapshot; `/metrics/{group}` and `/probe?target=` enable tighter scrape loops.

## Quick start

```bash
go build -o bin/rabbitmq-unified-exporter ./cmd/rabbitmq-unified-exporter
./bin/rabbitmq-unified-exporter -config config.yaml
curl -s localhost:9419/metrics | head
curl -s localhost:9419/targets | jq .
curl -s localhost:9419/capabilities | jq .
```

Edit [config.yaml](config.yaml) with your broker URLs and Secret file mounts.

## Container image

Multi-arch images (`linux/amd64`, `linux/arm64`) are published to GHCR on version tags:

```bash
docker pull ghcr.io/jan-benedikt/rabbitmq-unified-exporter:1.0.0
```

## HTTP endpoints

| Path | Description |
|---|---|
| `GET /metrics` | Aggregated fresh snapshots + exporter self-metrics |
| `GET /metrics/self` | Exporter self-metrics only |
| `GET /metrics/{group}` | Snapshots for one config group |
| `GET /metrics/targets/{group}/{name}` | Single target (all sources) |
| `GET /metrics/targets/{group}/{name}/{source}` | Single target source (`prometheus` / `management` / `probes`) |
| `GET /probe?target=` | Synchronous single-target collection |
| `GET /healthz` / `GET /-/healthy` | Process liveness |
| `GET /readyz` / `GET /-/ready` | Config loaded + scheduler running |
| `GET /targets` | JSON diagnostics |
| `GET /capabilities` | Supported metrics / mapping matrix |

## Kubernetes / Helm

Install from the published Helm repository (GitHub Pages):

```bash
helm repo add rabbitmq-unified-exporter https://jan-benedikt.github.io/rabbitmq-unified-exporter
helm repo update
helm upgrade --install rabbitmq-unified-exporter rabbitmq-unified-exporter/rabbitmq-unified-exporter \
  --set serviceMonitor.enabled=true \
  --set-file config=config.yaml
```

Or install from a local checkout:

```bash
helm upgrade --install rabbitmq-unified-exporter deploy/helm/rabbitmq-unified-exporter \
  --set serviceMonitor.enabled=true \
  --set-file config=config.yaml
```

Enable `honorLabels: true` on ServiceMonitor / scrape config so `job` / `instance` labels from the exporter are preserved.

Default image: `ghcr.io/jan-benedikt/rabbitmq-unified-exporter`.

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Metric compatibility](docs/metric-compatibility.md)
- [Security](docs/security.md)
- [Troubleshooting](docs/troubleshooting.md)
- [PromQL examples](docs/promql-examples.md)

## Alerts

Original Grafana export: [`alerts.yaml`](alerts.yaml).

Migrated rules (central exporter semantics):

- [`alerts/rabbitmq_alerts.migrated.yaml`](alerts/rabbitmq_alerts.migrated.yaml)
- [`alerts/rabbitmq-availability-alerts.migrated.yml`](alerts/rabbitmq-availability-alerts.migrated.yml)

Validate coverage:

```bash
make alertlint
```

## Tests

```bash
make race
make helm
# optional live RabbitMQ:
make integration
```

## Design guarantees

- Never convert `unknown` → `0`.
- Never synthesize health success on collection failure.
- Never invent unsupported JSON mappings.
- One broker failure does not affect others.
- Delivery test never consumes production queues (default off).

## License

[MIT](LICENSE)
