# PromQL examples

## Per-broker health

```promql
rabbitmq_unified_exporter_collection_success
rabbitmq_unified_exporter_target_up
rabbitmq_unified_exporter_data_age_seconds
rabbitmq_amqp_tcp_success
rabbitmq_application_running
```

## Cluster

```promql
rabbitmq_cluster_nodes_running / rabbitmq_cluster_nodes
rabbitmq_cluster_partitions > 0
rabbitmq_unified_exporter_cluster_identity_mismatch == 1
```

## Queues

```promql
rabbitmq_queue_messages_ready{queue!=""}
rabbitmq_quorum_queue_available{queue!=""}
rabbitmq_queue_state{state=~"crashed|minority|flow"}
```

## Sanitization / exporter self

```promql
rate(rabbitmq_unified_exporter_sanitized_samples_total[5m])
rabbitmq_unified_exporter_dropped_series_total
up{job=~"rabbitmq-unified.*"}
```

## Scrape honor_labels

Always scrape with `honor_labels: true` so exporter-emitted `job`/`instance` are preserved.
