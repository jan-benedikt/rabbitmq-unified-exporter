# Troubleshooting

## `unknown` disk metric

Symptom: Prometheus scrape of RabbitMQ `/metrics` fails or drops the family.

Fix: Exporter sanitizer drops only the invalid sample and keeps other metrics. Check:

```promql
rabbitmq_unified_exporter_sanitized_samples_total{reason="unknown_value"}
```

JSON mode can supply `rabbitmq_disk_space_available_bytes` from `disk_free` when management is enabled.

## Alerts on `up` fire incorrectly

With a central exporter, `up` describes the exporter pod, not each broker.

Use:

```promql
rabbitmq_unified_exporter_target_up{source="prometheus"}
# or
rabbitmq_unified_exporter_collection_success
```

Migrated alert rules are under `alerts/`.

## Duplicate series / cardinality explosion

- Do not scrape `/metrics` and `/metrics/per-object` together when per-object is already enabled on the broker.
- Cap with `limits.max_series_per_target` / `max_queues`.
- Check `rabbitmq_unified_exporter_dropped_series_total{reason="cardinality"}`.

## Stale data

```promql
rabbitmq_unified_exporter_data_age_seconds
```

If age grows, the broker or network is slow, or `scrape_timeout` is too low. Increase timeout or reduce endpoint set.

## No RabbitMQ metrics / Targets total = 0 while logs show dial timeouts

This is usually **not** caused by parallel endpoint scraping. Logs like:

`dial tcp 10.0.0.5:15692: i/o timeout` / `context deadline exceeded`

mean the exporter pod cannot reach broker Prometheus ports. Parallel scrapes only make each `/metrics` + `/metrics/detailed` attempt independent; they do not invent connectivity.

What you see on the dashboard:

1. After `max_staleness`, last-good broker series expire → Queues/Channels flatline.
2. If Grafana `$job` / `$instance` were taken only from `rabbitmq_identity_info`, those variables go empty when identity metrics disappear → **Targets total/up/down all show 0** even though `rabbitmq_unified_exporter_targets` exists.
3. Overview variables now use `rabbitmq_unified_exporter_targets` so availability stats keep working during broker outages.

Check from the exporter pod:

```bash
curl -m 5 http://rmq-prod-02.example.com:15692/metrics
curl -m 5 http://rmq-prod-02.example.com:15672/api/overview   # management
curl -s localhost:9419/metrics | grep rabbitmq_unified_exporter_targets
```

```promql
rabbitmq_unified_exporter_targets
rabbitmq_unified_exporter_target_up{source="prometheus"}
rabbitmq_unified_exporter_target_up{source="management"}
rabbitmq_unified_exporter_data_age_seconds
```

Classic ScrapeConfig often ran on a network path that already reached `:15692`; the unified exporter must have the same L3/firewall access.

## Grafana gaps / dotted lines (classic scrape was continuous)

Typical causes with this exporter:

1. **Failed collection used to wipe `/metrics`** — a timeout replaced the last good snapshot with an empty failure, so Prometheus scraped the exporter successfully but without series → multi-minute gaps. Fixed by retaining last successful snapshot until `max_staleness`.
2. **Collection slower than interval** — scraping `/metrics` + two `/metrics/detailed` + Management per broker often needs >10s. Use `scrape_interval: 30s` / `scrape_timeout: 20s` (timeout must stay `<` interval).
3. **ServiceMonitor scraping `/metrics` and `/metrics/{group}` together** — same payload rendered 3×; raise `scrapeTimeout` (e.g. 30s) and scrape a single path.
4. Check:
   ```promql
   rabbitmq_unified_exporter_collection_success
   rabbitmq_unified_exporter_collection_duration_seconds
   rabbitmq_unified_exporter_data_age_seconds
   ```

## Quorum availability is missing

If Management API does not return `members`/`online`/`leader`, the exporter emits an unsupported note and **does not** set `rabbitmq_quorum_queue_available=1`.

Check:

```promql
rabbitmq_quorum_queue_available
rabbitmq_unified_exporter_collection_success{source="management"}
```

Quorum metrics require `management.enabled: true` and collector `queues`. They only appear for queues with `type=quorum` (or non-empty `members`).

## Raft metrics missing (`rabbitmq_raft_*`)

Per-queue raft series come from RabbitMQ `/metrics/detailed?family=ra_metrics` (as `rabbitmq_detailed_raft_*`).
The exporter **rewrites** them to classic names on its `/metrics`:

- `rabbitmq_detailed_raft_*` → `rabbitmq_raft_*`
- modern names also aliased to dashboard legacy names (`rabbitmq_raft_entry_commit_latency_seconds`, `rabbitmq_raft_log_*`, `rabbitmq_raft_term_total`)

Aggregated raft / `rabbitmq_federation_links` also appear on broker `/metrics` and are scraped with that endpoint.

1. Broker check: `curl -s 'http://BROKER:15692/metrics/detailed?family=ra_metrics' | grep raft`
2. Exporter check: `curl -s http://EXPORTER:9419/metrics | grep rabbitmq_raft_entry_commit_latency`
3. Partial scrape: `rabbitmq_unified_exporter_collection_errors_total{source="prometheus",reason="endpoint_partial"}`

## Federation / shovel metrics missing

- Native counter: `rabbitmq_federation_links` (broker `/metrics`, when federation plugin is enabled).
- Dashboard panels that filter `queue=~"federation:.*"` need detailed `queue_metrics` (rewritten to classic `rabbitmq_queue_*` on the exporter).
- Link/shovel status from Management: `rabbitmq_federation_link_running`, `rabbitmq_shovel_running` (collectors `federation` / `shovels`).

## JSON mode missing raft / unroutable metrics

Expected — see [metric-compatibility.md](metric-compatibility.md). Prefer Prometheus mode for those alert families.

## Diagnostics endpoints

| Path | Use |
|---|---|
| `/targets` | Per-target last snapshot status |
| `/capabilities` | Supported metrics and mapping matrix |
| `/readyz` | Exporter ready (config + scheduler) |
| `/healthz` | Process alive |

## Example PromQL

```promql
# Per-broker collection health
rabbitmq_unified_exporter_collection_success

# Freshness
rabbitmq_unified_exporter_data_age_seconds > 60

# AMQP TCP
rabbitmq_amqp_tcp_success == 0

# Cluster majority
rabbitmq_cluster_nodes_running / rabbitmq_cluster_nodes < 0.5

# Exporter process
up{job="rabbitmq-unified-exporter"}
```
