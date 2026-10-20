# Configuration

See [config.yaml](../config.yaml) for a five-broker reference.

## Top-level keys

| Key | Description |
|---|---|
| `server.listen_address` | Default `:9419` (same default as kbudde/rabbitmq_exporter — avoid co-locating both on one host without remapping) |
| `server.metrics_path` | Default `/metrics` |
| `server.basic_auth` | Optional Basic auth for metrics/probe/diagnostics (`username`/`password` or `*_file`) |
| `server.tls_cert_file` / `server.tls_key_file` | Optional HTTPS for the metrics listener (both required together) |
| `server.enable_probe` | Default `false` (opt-in `/probe`) |
| `server.enable_diagnostics` | Default `false` (opt-in `/targets` and `/capabilities`) |
| `server.read_timeout` / `write_timeout` / `idle_timeout` | HTTP server timeouts (defaults 30s / 60s / 120s) |
| `defaults.*` | Inherited scrape/sanitize/limits/labels |
| `sharding` | Optional hash sharding across replicas |
| `groups[]` | Named groups with targets |

## Metrics source

- `metrics_source: prometheus` — scrape RabbitMQ Prometheus plugin (`metrics_url` required).
- `metrics_source: json` — map Management API (`management_url` required; enables management).

Management can also be enabled alongside Prometheus (`management.enabled: true`) for cluster/quorum/partition health.

Cluster-wide Management collectors (`queues`, `vhosts`, `federation`, `shovels`, `nodes` list) run only on the **cluster observer** — the lexicographically first target in the group, or `cluster_observer: true` on one target. Other targets still fetch overview + node-local health checks.

`management.allow_unpaginated_queues` (default `false`) must be set to accept a full `/api/queues` dump when pagination fails.

## Job / label migration

Previous direct ScrapeConfigs often used separate jobs `rabbitmq-*-metrics` and `rabbitmq-*-queue-metrics`. The unified exporter serves both classic and detailed series under the group `job` label (e.g. `rabbitmq-hv-prd-metrics`). Update dashboards/alerts that hard-code `*-queue-metrics` job names.

## Credentials

Prefer file mounts from Kubernetes Secrets:

```yaml
credentials:
  username_file: /secrets/prod/username
  password_file: /secrets/prod/password
```

Inline `username`/`password` are supported for local testing only.

## Prometheus endpoints

```yaml
prometheus:
  endpoints:
    - path: /metrics
    - path: /metrics/detailed
      params:
        family: [queue_coarse_metrics, ra_metrics]
```

Rules:

- `/metrics/detailed` **requires** `params.family` (validated).
- Families from `/metrics/detailed` are rewritten to classic `rabbitmq_*` names (strip `rabbitmq_detailed_`) and raft legacy aliases are emitted so dashboards scrapes of the exporter `/metrics` match classic naming.
- Avoid scraping both `/metrics` (with per-object enabled) and `/metrics/per-object` — duplicates are deduped but waste bandwidth.
- `/metrics/per-object` lacks `rabbitmq_alarms_*`.
- `/metrics/memory-breakdown` is optional; HTTP 404 is treated as unsupported.

## Probes

```yaml
probes:
  amqp_tcp: { enabled: true, interval: 15s, timeout: 5s }
  amqp_auth: { enabled: true, interval: 30s, vhosts: ["/"] }
  delivery_test: { enabled: false, interval: 60s, vhost: monitoring, max_concurrent_tests: 2 }
  emit_probe_success_alias: false
```

Delivery test is **off by default**. It uses an exclusive auto-delete server-named queue and never consumes application queues.

Probe `interval` (when set) reuses the last result until the interval elapses, so AMQP checks need not run on every scrape tick.

## Hot reload

Send `SIGHUP` to reload the config file. The HTTP server swaps the scheduler under a lock, purges snapshots for removed targets, and keeps `/probe` / `/readyz` consistent. Secret files are re-read on each collection via `*_file`.

CLI: `-version`, `-log-level=debug|info|warn|error`.

YAML parsing uses strict known fields — unknown keys are rejected.

## Prometheus scrape tips

```yaml
scrape_configs:
  - job_name: rabbitmq-unified
    honor_labels: true
    metrics_path: /metrics
    static_configs:
      - targets: ["rabbitmq-unified-exporter:9419"]
  # Near-realtime for one group:
  - job_name: rabbitmq-prod
    honor_labels: true
    metrics_path: /metrics/prod
    scrape_interval: 10s
```

Do **not** set `k8s_cluster_name` / `prometheus` on the exporter; add them via Prometheus/VM `external_labels`.
