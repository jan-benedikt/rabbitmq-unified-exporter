// Package selfmetrics exposes rabbitmq_unified_exporter_* Prometheus metrics.
package selfmetrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "rabbitmq_unified_exporter"

// TargetMeta identifies a monitored broker for self-metrics labels.
// Job/cluster must match labels on RabbitMQ series so dashboards can filter one exporter scope.
type TargetMeta struct {
	Target   string
	Group    string
	Instance string
	Job      string
	Cluster  string
}

// Registry holds exporter self-monitoring metrics.
type Registry struct {
	reg *prometheus.Registry

	targetUp             *prometheus.GaugeVec
	collectionSuccess    *prometheus.GaugeVec
	collectionDuration   *prometheus.HistogramVec
	collectionErrors     *prometheus.CounterVec
	sanitizedSamples     *prometheus.CounterVec
	lastSuccessTimestamp *prometheus.GaugeVec
	collectedSamples     *prometheus.GaugeVec
	mappingErrors        *prometheus.CounterVec
	probeSuccess         *prometheus.GaugeVec
	dataAge              *prometheus.GaugeVec
	snapshotSeries       *prometheus.GaugeVec
	droppedSeries        *prometheus.CounterVec
	configReloadSuccess  prometheus.Gauge
	duplicateSeries      *prometheus.CounterVec
	typeConflicts        *prometheus.CounterVec
	unsupportedMetric    *prometheus.GaugeVec
	skippedCollections   *prometheus.CounterVec
	buildInfo            *prometheus.GaugeVec
	configuredTargets    *prometheus.GaugeVec // 1 per configured broker target

	mu                  sync.Mutex
	lastSuccessByTarget map[string]time.Time
	metaByTarget        map[string]TargetMeta
}

var targetLabelNames = []string{"job", "cluster", "target", "group", "instance"}

// New creates and registers self-metrics on a dedicated registry.
func New(version string) *Registry {
	r := prometheus.NewRegistry()
	m := &Registry{
		reg:                 r,
		lastSuccessByTarget: map[string]time.Time{},
		metaByTarget:        map[string]TargetMeta{},
		targetUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "target_up", Help: "1 if last collection for target source succeeded",
		}, append(append([]string{}, targetLabelNames...), "source")),
		collectionSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "collection_success", Help: "1 if last collection succeeded",
		}, targetLabelNames),
		collectionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "collection_duration_seconds", Help: "Collection duration",
			Buckets: prometheus.DefBuckets,
		}, targetLabelNames),
		collectionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "collection_errors_total", Help: "Collection errors",
		}, append(append([]string{}, targetLabelNames...), "source", "reason")),
		sanitizedSamples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "sanitized_samples_total", Help: "Samples dropped by sanitizer",
		}, []string{"job", "cluster", "instance", "reason"}),
		lastSuccessTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "last_success_timestamp_seconds", Help: "Unix time of last successful collection",
		}, targetLabelNames),
		collectedSamples: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "collected_samples", Help: "Samples in last successful snapshot",
		}, targetLabelNames),
		mappingErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "mapping_errors_total", Help: "JSON mapping errors",
		}, []string{"job", "cluster", "target", "instance", "reason"}),
		probeSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "probe_success", Help: "1 if last probe succeeded",
		}, []string{"job", "cluster", "target", "instance", "probe"}),
		dataAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "data_age_seconds", Help: "Age of last successful snapshot",
		}, targetLabelNames),
		snapshotSeries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "snapshot_series", Help: "Series count in last successful snapshot per target",
		}, targetLabelNames),
		droppedSeries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "dropped_series_total", Help: "Series dropped",
		}, []string{"job", "cluster", "reason"}),
		configReloadSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "config_reload_success", Help: "1 if last config reload succeeded",
		}),
		duplicateSeries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "duplicate_series_total", Help: "Duplicate series detected at merge",
		}, []string{"job", "cluster", "metric"}),
		typeConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "type_conflict_total", Help: "Metric TYPE conflicts detected at merge",
		}, []string{"job", "cluster", "metric"}),
		unsupportedMetric: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "unsupported_metric", Help: "1 if metric is unsupported for source",
		}, []string{"job", "metric", "source"}),
		skippedCollections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "skipped_collections_total", Help: "Collections skipped due to overlap or backoff",
		}, targetLabelNames),
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "build_info", Help: "Build information",
		}, []string{"version"}),
		configuredTargets: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "targets",
			Help: "1 for each configured monitored RabbitMQ server (sum/count = total targets)",
		}, targetLabelNames),
	}

	r.MustRegister(
		m.targetUp, m.collectionSuccess, m.collectionDuration, m.collectionErrors,
		m.sanitizedSamples, m.lastSuccessTimestamp, m.collectedSamples, m.mappingErrors,
		m.probeSuccess, m.dataAge, m.snapshotSeries, m.droppedSeries, m.configReloadSuccess,
		m.duplicateSeries, m.typeConflicts, m.unsupportedMetric, m.skippedCollections, m.buildInfo,
		m.configuredTargets,
	)
	m.buildInfo.WithLabelValues(version).Set(1)
	m.configReloadSuccess.Set(1)
	return m
}

// Gatherer returns the Prometheus gatherer.
func (m *Registry) Gatherer() prometheus.Gatherer { return m.reg }

// Registerer returns the Prometheus registerer.
func (m *Registry) Registerer() prometheus.Registerer { return m.reg }

// RememberTarget stores label meta used by scheduler callbacks (skip/observe).
func (m *Registry) RememberTarget(meta TargetMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metaByTarget[meta.Target] = meta
}

// SetConfiguredTargets replaces the set of configured broker targets (value 1 each).
// Call after config load/reload so dashboards can count monitored servers even when scrapes fail.
func (m *Registry) SetConfiguredTargets(metas []TargetMeta) {
	m.configuredTargets.Reset()
	m.mu.Lock()
	m.metaByTarget = make(map[string]TargetMeta, len(metas))
	m.mu.Unlock()
	for _, meta := range metas {
		m.RememberTarget(meta)
		m.configuredTargets.WithLabelValues(m.values(meta)...).Set(1)
	}
}

func (m *Registry) meta(targetID string) TargetMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if meta, ok := m.metaByTarget[targetID]; ok {
		return meta
	}
	group, instance := splitTarget(targetID)
	return TargetMeta{Target: targetID, Group: group, Instance: instance}
}

func (m *Registry) values(meta TargetMeta) []string {
	return []string{meta.Job, meta.Cluster, meta.Target, meta.Group, meta.Instance}
}

// IncSkipped implements scheduler.Metrics.
func (m *Registry) IncSkipped(targetID string) {
	meta := m.meta(targetID)
	m.skippedCollections.WithLabelValues(m.values(meta)...).Inc()
}

// ObserveCollection implements scheduler.Metrics.
func (m *Registry) ObserveCollection(targetID string, success bool, duration time.Duration) {
	meta := m.meta(targetID)
	vals := m.values(meta)
	m.collectionDuration.WithLabelValues(vals...).Observe(duration.Seconds())
	if success {
		m.collectionSuccess.WithLabelValues(vals...).Set(1)
		m.lastSuccessTimestamp.WithLabelValues(vals...).Set(float64(time.Now().Unix()))
		m.mu.Lock()
		m.lastSuccessByTarget[targetID] = time.Now()
		m.mu.Unlock()
	} else {
		m.collectionSuccess.WithLabelValues(vals...).Set(0)
	}
}

// SetCollectionResult records a full collection outcome.
func (m *Registry) SetCollectionResult(meta TargetMeta, success bool, samples int, sources map[string]bool) {
	m.RememberTarget(meta)
	vals := m.values(meta)
	v := 0.0
	if success {
		v = 1
	}
	m.collectionSuccess.WithLabelValues(vals...).Set(v)
	m.collectedSamples.WithLabelValues(vals...).Set(float64(samples))
	m.snapshotSeries.WithLabelValues(vals...).Set(float64(samples))
	for src, ok := range sources {
		sv := 0.0
		if ok {
			sv = 1
		}
		m.targetUp.WithLabelValues(append(vals, src)...).Set(sv)
	}
	if success {
		now := time.Now()
		m.lastSuccessTimestamp.WithLabelValues(vals...).Set(float64(now.Unix()))
		m.mu.Lock()
		m.lastSuccessByTarget[meta.Target] = now
		m.mu.Unlock()
	}
}

// AddSanitized increments sanitized sample counters.
func (m *Registry) AddSanitized(meta TargetMeta, reason string, n int) {
	m.RememberTarget(meta)
	m.sanitizedSamples.WithLabelValues(meta.Job, meta.Cluster, meta.Instance, reason).Add(float64(n))
}

// IncMappingError increments mapping errors.
func (m *Registry) IncMappingError(meta TargetMeta, reason string) {
	m.RememberTarget(meta)
	m.mappingErrors.WithLabelValues(meta.Job, meta.Cluster, meta.Target, meta.Instance, reason).Inc()
}

// SetProbe records probe outcome.
func (m *Registry) SetProbe(meta TargetMeta, probe string, success bool) {
	m.RememberTarget(meta)
	v := 0.0
	if success {
		v = 1
	}
	m.probeSuccess.WithLabelValues(meta.Job, meta.Cluster, meta.Target, meta.Instance, probe).Set(v)
}

// SetDataAge sets data age for a target.
func (m *Registry) SetDataAge(meta TargetMeta, age time.Duration) {
	m.RememberTarget(meta)
	m.dataAge.WithLabelValues(m.values(meta)...).Set(age.Seconds())
}

// LastSuccessAge returns time since last successful collection, or 0 if never succeeded.
func (m *Registry) LastSuccessAge(targetID string, now time.Time) (age time.Duration, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, exists := m.lastSuccessByTarget[targetID]
	if !exists || t.IsZero() {
		return 0, false
	}
	return now.Sub(t), true
}

// SetSnapshotSeries is kept for compatibility; prefer per-target via SetCollectionResult.
func (m *Registry) SetSnapshotSeries(n int) {
	_ = n
}

// IncDropped increments dropped series.
func (m *Registry) IncDropped(meta TargetMeta, reason string, n int) {
	m.RememberTarget(meta)
	m.droppedSeries.WithLabelValues(meta.Job, meta.Cluster, reason).Add(float64(n))
}

// IncDuplicate increments duplicate series counter.
func (m *Registry) IncDuplicate(meta TargetMeta, metric string) {
	m.RememberTarget(meta)
	m.duplicateSeries.WithLabelValues(meta.Job, meta.Cluster, metric).Inc()
}

func (m *Registry) IncTypeConflict(meta TargetMeta, metric string) {
	m.RememberTarget(meta)
	m.typeConflicts.WithLabelValues(meta.Job, meta.Cluster, metric).Inc()
}

// SetUnsupported marks a metric as unsupported.
func (m *Registry) SetUnsupported(meta TargetMeta, metric, source string) {
	m.unsupportedMetric.WithLabelValues(meta.Job, metric, source).Set(1)
}

// SetConfigReload records reload success.
func (m *Registry) SetConfigReload(ok bool) {
	if ok {
		m.configReloadSuccess.Set(1)
	} else {
		m.configReloadSuccess.Set(0)
	}
}

// IncCollectionError increments error counter.
func (m *Registry) IncCollectionError(meta TargetMeta, source, reason string) {
	m.RememberTarget(meta)
	m.collectionErrors.WithLabelValues(append(m.values(meta), source, reason)...).Inc()
}

// MetaFromLabels builds TargetMeta from exporter target identity labels.
func MetaFromLabels(targetID, group, instance string, labels map[string]string) TargetMeta {
	if instance == "" {
		instance = labels["instance"]
	}
	if instance == "" {
		_, instance = splitTarget(targetID)
	}
	return TargetMeta{
		Target:   targetID,
		Group:    group,
		Instance: instance,
		Job:      labels["job"],
		Cluster:  labels["cluster"],
	}
}

func splitTarget(id string) (group, instance string) {
	for i := 0; i < len(id); i++ {
		if id[i] == '/' {
			return id[:i], id[i+1:]
		}
	}
	return "", id
}
