# Metric Compatibility Matrix

RabbitMQ Management API and the Prometheus plugin do **not** share identical semantics.
This matrix documents JSON-mode fidelity for metrics required by Grafana alerts and the implementation plan.

Legend:

- **exact** — same name, type, unit and comparable value (node-filtered where required)
- **approximate** — same name but aggregation, reset semantics or scope differ
- **unsupported** — no equivalent Management API field; never synthesized
- **native** — available from RabbitMQ Prometheus plugin endpoints

Version coverage: **3.9.x – 4.x** (schemas in `mappings/`). The `4.x` JSON schema matches `3.13` field paths; prefer the native Prometheus plugin in production.

| Metric | Native | JSON fidelity | JSON source | Notes | Fixture |
|---|---|---|---|---|---|
| `rabbitmq_queue_messages` | yes | exact | `/api/queues` `messages` | Filtered to self node/leader | `testdata/golden/3.9.15/queues.json` |
| `rabbitmq_queue_messages_ready` | yes | exact | `messages_ready` | Also as `rabbitmq_detailed_*` via `/metrics/detailed` | same |
| `rabbitmq_queue_messages_unacked` | yes | exact | `messages_unacknowledged` | | same |
| `rabbitmq_queue_consumers` | yes | exact | `consumers` | | same |
| `rabbitmq_queue_process_memory_bytes` | yes | exact | `memory` | | same |
| `rabbitmq_queue_messages_ram_bytes` | yes | exact | `message_bytes_ram` | | same |
| `rabbitmq_process_resident_memory_bytes` | yes | exact | `/api/nodes/{self}` `mem_used` | | `nodes.json` |
| `rabbitmq_resident_memory_limit_bytes` | yes | exact | `mem_limit` | | same |
| `rabbitmq_disk_space_available_bytes` | yes | exact | `disk_free` | Native may emit `unknown` — sanitized (dropped) | `testdata/golden/3.9.15/prometheus/metrics_unknown.txt` |
| `rabbitmq_disk_space_available_limit_bytes` | yes | exact | `disk_free_limit` | | same |
| `rabbitmq_process_open_fds` | yes | exact | `fd_used` | | same |
| `rabbitmq_process_max_fds` | yes | exact | `fd_total` | | same |
| `rabbitmq_alarms_memory_used_watermark` | yes (`/metrics` only) | exact | `mem_alarm` | Missing from `/metrics/per-object` | same |
| `rabbitmq_alarms_free_disk_space_watermark` | yes | exact | `disk_free_alarm` | | same |
| `rabbitmq_alarms_file_descriptor_limit` | yes | approximate | health `local-alarms` | Prefer native Prometheus | — |
| `rabbitmq_global_messages_delivered_total` | yes | approximate | sum `message_stats.deliver_get` | Stats DB reset; node-local sum | — |
| `rabbitmq_global_messages_redelivered_total` | yes | approximate | sum `message_stats.redeliver` | | — |
| `rabbitmq_channel_messages_unroutable_dropped_total` | yes | **unsupported** | — | No per-exchange Management equivalent | — |
| `rabbitmq_channel_messages_unroutable_returned_total` | yes | **unsupported** | — | | — |
| `rabbitmq_raft_entry_commit_latency_seconds` | yes (`ra_metrics` → classic alias) | **unsupported** | — | detailed `rabbitmq_detailed_raft_commit_latency_seconds` normalized | — |
| `rabbitmq_raft_log_commit_index` | yes (alias of `raft_commit_index`) | **unsupported** | — | | — |
| `rabbitmq_raft_log_last_written_index` | yes | **unsupported** | — | | — |
| `rabbitmq_raft_log_last_applied_index` | yes | **unsupported** | — | | — |
| `rabbitmq_raft_term_total` | yes (alias of `raft_term`) | **unsupported** | — | | — |
| `rabbitmq_erlang_uptime_seconds` | yes | approximate | `uptime` / 1000 | Management unit is ms | — |

## Additional health metrics (exporter-owned)

| Metric | Source | Notes |
|---|---|---|
| `rabbitmq_cluster_nodes` | config `expected_cluster_nodes` | Stable expected size |
| `rabbitmq_cluster_nodes_running` | `/api/nodes` `running` | Observed state |
| `rabbitmq_cluster_partitions` | `/api/nodes` `partitions` | |
| `rabbitmq_cluster_version_skew` | `/api/nodes` versions | |
| `rabbitmq_application_running` | node + health check + optional AMQP auth | Never synthetic success on scrape failure |
| `rabbitmq_quorum_queue_available` | queue `leader`/`members`/`online` | Unknown if metadata missing |
| `rabbitmq_federation_link` / `_running` | `/api/federation-links` | Needs `federation` collector + federation management plugin |
| `rabbitmq_shovel` / `_running` | `/api/shovels` | Needs `shovels` collector + shovel management plugin |
| `rabbitmq_vhost_running` | `/api/vhosts` `cluster_state` | |
| `rabbitmq_expected_target` | config | Always emitted |
| `rabbitmq_amqp_tcp_success` | TCP dial | |
| `rabbitmq_amqp_connection_success` | AMQP handshake | |
| `rabbitmq_message_delivery_test_success` | exclusive auto-delete queue | Default off |

## Rules

1. Never convert rate fields (`*_details.rate`) into `*_total` counters.
2. Never invent unsupported metrics to make tests pass.
3. Never replace `unknown` / `null` with `0`.
4. JSON queue metrics are filtered to the observing node (`node` or `leader == self`).
