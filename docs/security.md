# Security

## Container hardening

- Distroless nonroot image (`65532`)
- `readOnlyRootFilesystem: true`
- Drop all capabilities
- No privilege escalation
- Optional NetworkPolicy limiting egress to broker ports + DNS

## Credentials

- Prefer Kubernetes Secrets / External Secrets mounted as files (`username_file`, `password_file`).
- Credentials are never logged; error strings are redacted before logs and `/targets`.
- Files are re-read each collection via `Resolve()` (supports rotation without restart).

## HTTP surface

- Set `server.basic_auth` (or `username_file` / `password_file`) to require Basic auth on all endpoints except `/healthz` and `/readyz`.
- Optional HTTPS: `server.tls_cert_file` + `server.tls_key_file`.
- `/probe` and `/targets`/`/capabilities` are **off by default**; enable only when needed (`enable_probe` / `enable_diagnostics`).
- Server sets `ReadTimeout` / `WriteTimeout` / `IdleTimeout` / `MaxHeaderBytes` (overridable in YAML).
- Prefer NetworkPolicy with `ingressFrom` (scrape namespace) and `brokerCIDRs` for egress.

## Monitoring user permissions

Recommended least privilege for Management + probes:

```text
tag: monitoring
```

For delivery test (dedicated vhost `monitoring` recommended):

```text
configure: ^rmq-ue-probe\.|^$
write:     .*
read:      .*
```

Or grant configure/write/read only on vhost `monitoring`.

AMQP connections set `connection_name=rabbitmq-unified-exporter` for log/dashboard filtering.

## What the exporter will NOT do

- Never call `/api/aliveness-test` (declares a queue on the broker).
- Never consume from application queues.
- Never change RabbitMQ configuration as part of normal monitoring.
- Never invent health success when collection fails.

## TLS

Configure `tls.enabled`, `ca_file`, `cert_file`, `key_file`, `server_name`.
`insecure_skip_verify` is available for labs only.
Exporter can emit `rabbitmq_endpoint_tls_cert_expiry_timestamp_seconds` for HTTPS endpoints.
