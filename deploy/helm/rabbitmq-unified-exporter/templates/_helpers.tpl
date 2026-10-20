{{- define "rabbitmq-unified-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rabbitmq-unified-exporter.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "rabbitmq-unified-exporter.labels" -}}
app.kubernetes.io/name: {{ include "rabbitmq-unified-exporter.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "rabbitmq-unified-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rabbitmq-unified-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rabbitmq-unified-exporter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "rabbitmq-unified-exporter.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Fail at template time when replicaCount > 1. Sharding is config-driven; use one
Helm release per shard with unique config.sharding.shard_index, or replicaCount=1.
*/}}
{{- define "rabbitmq-unified-exporter.validateValues" -}}
{{- if gt (int .Values.replicaCount) 1 -}}
{{- fail "replicaCount must be 1: use separate Helm releases for sharding, each with a unique config.sharding.shard_index in the exporter YAML (or keep replicaCount=1)" -}}
{{- end -}}
{{- end -}}

{{/*
Common Prometheus ServiceMonitor endpoint fields (interval, honor*, limits, TLS, auth).
Expects dict $s matching serviceMonitor / vmServiceScrape values.
*/}}
{{- define "rabbitmq-unified-exporter.prometheusScrapeOpts" -}}
{{- $s := . -}}
interval: {{ $s.interval }}
scrapeTimeout: {{ $s.scrapeTimeout }}
honorLabels: {{ $s.honorLabels }}
honorTimestamps: {{ $s.honorTimestamps }}
{{- if $s.scrapeClass }}
scrapeClass: {{ $s.scrapeClass | quote }}
{{- end }}
{{- if $s.fallbackScrapeProtocol }}
fallbackScrapeProtocol: {{ $s.fallbackScrapeProtocol | quote }}
{{- end }}
{{- if not (eq $s.sampleLimit nil) }}
sampleLimit: {{ $s.sampleLimit }}
{{- end }}
{{- if not (eq $s.labelLimit nil) }}
labelLimit: {{ $s.labelLimit }}
{{- end }}
{{- with $s.tlsConfig }}
tlsConfig:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $s.basicAuth }}
basicAuth:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $s.metricRelabelings }}
metricRelabelings:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
VictoriaMetrics VMServiceScrape endpoint fields (same knobs, VM CR field names).
*/}}
{{- define "rabbitmq-unified-exporter.vmScrapeOpts" -}}
{{- $s := . -}}
interval: {{ $s.interval }}
scrapeTimeout: {{ $s.scrapeTimeout }}
honorLabels: {{ $s.honorLabels }}
honorTimestamps: {{ $s.honorTimestamps }}
{{- if $s.scrapeClass }}
scrapeClass: {{ $s.scrapeClass | quote }}
{{- end }}
{{- if $s.fallbackScrapeProtocol }}
fallbackScrapeProtocol: {{ $s.fallbackScrapeProtocol | quote }}
{{- end }}
{{- if not (eq $s.sampleLimit nil) }}
sampleLimit: {{ $s.sampleLimit }}
{{- end }}
{{- if not (eq $s.labelLimit nil) }}
labelLimit: {{ $s.labelLimit }}
{{- end }}
{{- with $s.tlsConfig }}
tlsConfig:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $s.basicAuth }}
basicAuth:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $s.metricRelabelings }}
metricRelabelConfigs:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
Simple ServiceMonitor endpoint (path only, optional relabelings + extra relabelings from values).
Dict: root, path, scrape ($s), relabelings (optional list).
*/}}
{{- define "rabbitmq-unified-exporter.smSimpleEndpoint" -}}
- port: http
  path: {{ .path }}
  {{- include "rabbitmq-unified-exporter.prometheusScrapeOpts" .scrape | nindent 2 }}
  {{- if or .relabelings .scrape.relabelings }}
  relabelings:
    {{- with .relabelings }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
    {{- with .scrape.relabelings }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- end }}
{{- end -}}

{{/*
One ServiceMonitor endpoint for a split metrics path.
Expects dict: root, path, source, labels (map), scrape ($s).
*/}}
{{- define "rabbitmq-unified-exporter.scrapeEndpoint" -}}
- port: http
  path: {{ .path }}
  {{- include "rabbitmq-unified-exporter.prometheusScrapeOpts" .scrape | nindent 2 }}
  relabelings:
    - targetLabel: exporter_source
      replacement: {{ .source | quote }}
      action: replace
    - sourceLabels: [__metrics_path__]
      targetLabel: exporter_metrics_path
      action: replace
    {{- range $k, $v := .labels }}
    - targetLabel: {{ $k | quote }}
      replacement: {{ $v | quote }}
      action: replace
    {{- end }}
    {{- with .scrape.relabelings }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}
