// Package server provides the HTTP metrics and diagnostics endpoints.
package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mapping"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/safeerr"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/scheduler"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/selfmetrics"
)

const (
	defaultProbeTimeout = 10 * time.Second
	maxProbeTimeout     = 60 * time.Second
)

// Server is the HTTP API.
type Server struct {
	mu      sync.RWMutex
	cfg     *config.Config
	targets []config.ResolvedTarget
	store   *model.SnapshotStore
	sched   *scheduler.Scheduler
	metrics *selfmetrics.Registry
	schema  *mapping.Schema
	log     *slog.Logger
	ready   func() bool
	mux     *http.ServeMux
	httpSrv *http.Server
	handler http.Handler

	expoMu    sync.Mutex
	expoCache map[string]expoCacheEntry
}

type expoCacheEntry struct {
	plain       []byte
	gzipped     []byte
	contentType string
	fingerprint string
}

// New creates the HTTP server.
func New(
	cfg *config.Config,
	targets []config.ResolvedTarget,
	store *model.SnapshotStore,
	sched *scheduler.Scheduler,
	metrics *selfmetrics.Registry,
	schema *mapping.Schema,
	ready func() bool,
	log *slog.Logger,
) *Server {
	s := &Server{
		cfg:       cfg,
		targets:   targets,
		store:     store,
		sched:     sched,
		metrics:   metrics,
		schema:    schema,
		ready:     ready,
		log:       log,
		mux:       http.NewServeMux(),
		expoCache: map[string]expoCacheEntry{},
	}
	s.routes()
	s.handler = s.authMiddleware(s.mux)
	return s
}

func (s *Server) routes() {
	path := s.metricsPathLocked()
	s.mux.HandleFunc("/", s.handleLanding)
	s.mux.HandleFunc(path, s.handleMetrics)
	s.mux.HandleFunc(path+"/", s.handleMetricsSubpath)
	s.mux.HandleFunc("/probe", s.handleProbe)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/readyz", s.handleReadyz)
	s.mux.HandleFunc("/-/healthy", s.handleHealthz)
	s.mux.HandleFunc("/-/ready", s.handleReadyz)
	s.mux.HandleFunc("/targets", s.handleTargets)
	s.mux.HandleFunc("/capabilities", s.handleCapabilities)
}

func (s *Server) handleLanding(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, `<html><head><title>rabbitmq-unified-exporter</title></head><body>
<h1>rabbitmq-unified-exporter</h1>
<p><a href="`+s.metricsPath()+`">Metrics</a></p>
<p><a href="/metrics/self">Self metrics</a></p>
<p><a href="/healthz">Health</a> · <a href="/readyz">Ready</a></p>
</body></html>`)
}

func (s *Server) metricsPath() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metricsPathLocked()
}

func (s *Server) metricsPathLocked() string {
	path := s.cfg.Server.MetricsPath
	if path == "" {
		return "/metrics"
	}
	return path
}

// Handler returns the HTTP handler (with optional basic auth).
func (s *Server) Handler() http.Handler { return s.handler }

// ListenAndServe starts the HTTP server with production timeouts.
func (s *Server) ListenAndServe() error {
	s.mu.RLock()
	cfg := s.cfg
	s.mu.RUnlock()

	readTO := cfg.Server.ReadTimeout.Duration()
	writeTO := cfg.Server.WriteTimeout.Duration()
	idleTO := cfg.Server.IdleTimeout.Duration()
	if readTO <= 0 {
		readTO = 30 * time.Second
	}
	if writeTO <= 0 {
		writeTO = 60 * time.Second
	}
	if idleTO <= 0 {
		idleTO = 120 * time.Second
	}

	s.httpSrv = &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       readTO,
		WriteTimeout:      writeTO,
		IdleTimeout:       idleTO,
		MaxHeaderBytes:    64 << 10,
	}
	cert, key := cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile
	if cert != "" && key != "" {
		s.log.Info("listening", "addr", cfg.Server.ListenAddress, "tls", true)
		return s.httpSrv.ListenAndServeTLS(cert, key)
	}
	s.log.Info("listening", "addr", cfg.Server.ListenAddress, "tls", false)
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Liveness/readiness stay unauthenticated for kubelet probes.
		switch r.URL.Path {
		case "/healthz", "/readyz", "/-/healthy", "/-/ready":
			next.ServeHTTP(w, r)
			return
		}
		s.mu.RLock()
		auth := s.cfg.Server.BasicAuth
		s.mu.RUnlock()
		user, pass, err := auth.Resolve()
		if err != nil {
			s.log.Error("basic_auth resolve failed", "err", safeerr.Err(err))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if user == "" && pass == "" {
			next.ServeHTTP(w, r)
			return
		}
		u, p, ok := r.BasicAuth()
		if !ok || !constantTimeEqual(u, user) || !constantTimeEqual(p, pass) {
			w.Header().Set("WWW-Authenticate", `Basic realm="rabbitmq-unified-exporter"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func constantTimeEqual(a, b string) bool {
	ha := sha256.Sum256([]byte(a))
	hb := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.writeSnapshots(w, r, s.store.AllFresh(time.Now(), ""), true)
}

// handleMetricsSubpath serves:
//
//	/metrics/self
//	/metrics/targets/{group}/{name}
//	/metrics/targets/{group}/{name}/{source}   source=prometheus|management|probes
//	/metrics/{group}                           legacy group filter
func (s *Server) handleMetricsSubpath(w http.ResponseWriter, r *http.Request) {
	base := s.metricsPath()
	rest := strings.TrimPrefix(r.URL.Path, base+"/")
	rest = strings.Trim(rest, "/")
	if rest == "" {
		s.handleMetrics(w, r)
		return
	}
	parts := strings.Split(rest, "/")

	switch {
	case parts[0] == "self":
		s.writeSelfOnly(w, r)
		return
	case parts[0] == "targets":
		if len(parts) < 3 {
			http.Error(w, "usage: /metrics/targets/{group}/{name}[/{source}]", http.StatusBadRequest)
			return
		}
		group, name := parts[1], parts[2]
		source := ""
		if len(parts) >= 4 {
			source = parts[3]
		}
		if len(parts) > 4 {
			http.Error(w, "too many path segments", http.StatusBadRequest)
			return
		}
		s.writeTarget(w, r, group, name, source)
		return
	default:
		// Legacy: /metrics/{group}
		if len(parts) != 1 {
			http.Error(w, "unknown metrics path", http.StatusNotFound)
			return
		}
		s.writeSnapshots(w, r, s.store.AllFresh(time.Now(), parts[0]), true)
	}
}

func (s *Server) writeTarget(w http.ResponseWriter, r *http.Request, group, name, source string) {
	id := group + "/" + name
	s.mu.RLock()
	var tgt *config.ResolvedTarget
	for i := range s.targets {
		if s.targets[i].ID == id || (s.targets[i].Group == group && s.targets[i].Name == name) {
			cp := s.targets[i]
			tgt = &cp
			id = cp.ID
			break
		}
	}
	s.mu.RUnlock()
	if tgt == nil {
		http.Error(w, "unknown target", http.StatusNotFound)
		return
	}

	now := time.Now()
	snap := s.store.Fresh(id, now)
	var families []model.Family
	if snap != nil {
		age := now.Sub(snap.CollectedAt)
		if !snap.OldestSourceAt.IsZero() {
			age = now.Sub(snap.OldestSourceAt)
		}
		instance := tgt.Labels["instance"]
		if instance == "" {
			instance = tgt.Name
		}
		meta := selfmetrics.MetaFromLabels(tgt.ID, tgt.Group, instance, tgt.Labels)
		s.metrics.SetDataAge(meta, age)

		switch source {
		case "":
			families = snap.Families
		case "prometheus", "management", "probes":
			if snap.FamiliesBySource != nil {
				families = snap.FamiliesBySource[source]
			}
		default:
			http.Error(w, "source must be prometheus, management, or probes", http.StatusBadRequest)
			return
		}
	}
	// Per-target scrapes omit self-metrics (scraped once via /metrics/self).
	s.writeFamilies(w, r, families, false)
}

func (s *Server) writeSnapshots(w http.ResponseWriter, r *http.Request, snaps []*model.Snapshot, includeSelf bool) {
	now := time.Now()
	parts := make([][]model.Family, 0, len(snaps))
	s.mu.RLock()
	targets := append([]config.ResolvedTarget(nil), s.targets...)
	s.mu.RUnlock()
	for _, snap := range snaps {
		parts = append(parts, snap.Families)
		age := now.Sub(snap.CollectedAt)
		if !snap.OldestSourceAt.IsZero() {
			age = now.Sub(snap.OldestSourceAt)
		}
		for _, t := range targets {
			if t.ID == snap.TargetID {
				instance := t.Labels["instance"]
				if instance == "" {
					instance = t.Name
				}
				meta := selfmetrics.MetaFromLabels(t.ID, t.Group, instance, t.Labels)
				s.metrics.SetDataAge(meta, age)
				break
			}
		}
	}
	merged := model.MergeFamilies(parts, model.MergeOptions{
		DedupeIdentical: true,
		OnDuplicateConflict: func(name string, _, _ model.Sample) {
			s.metrics.IncDuplicate(selfmetrics.TargetMeta{}, name)
		},
	})
	s.metrics.SetSnapshotSeries(model.CountSeries(merged))
	s.writeFamilies(w, r, merged, includeSelf)
}

func (s *Server) writeSelfOnly(w http.ResponseWriter, r *http.Request) {
	s.writeFamilies(w, r, nil, true)
}

func (s *Server) writeFamilies(w http.ResponseWriter, r *http.Request, families []model.Family, includeSelf bool) {
	openMetrics := r != nil && prefersOpenMetrics(r.Header.Get("Accept"))
	wantGzip := r != nil && acceptsGzip(r.Header.Get("Accept-Encoding"))

	var buf bytes.Buffer
	opts := model.ExpositionOptions{OpenMetrics: openMetrics}
	if err := model.WriteExpositionOpts(&buf, families, opts); err != nil {
		s.log.Warn("metrics write failed", "err", err)
	}
	if includeSelf {
		selfBody, err := gatherSelf(s.metrics.Gatherer())
		if err == nil && selfBody != "" {
			_, _ = buf.WriteString(selfBody)
		}
	}
	if openMetrics {
		_, _ = buf.WriteString("# EOF\n")
	}

	fp := fingerprintBytes(buf.Bytes())
	cacheKey := r.URL.Path
	if openMetrics {
		cacheKey += "|om"
	}

	ct := "text/plain; version=0.0.4; charset=utf-8"
	if openMetrics {
		ct = "application/openmetrics-text; version=1.0.0; charset=utf-8"
	}

	plain := buf.Bytes()
	var gzipped []byte
	s.expoMu.Lock()
	if ent, ok := s.expoCache[cacheKey]; ok && ent.fingerprint == fp {
		plain = ent.plain
		gzipped = ent.gzipped
		ct = ent.contentType
	} else {
		if wantGzip {
			gzipped = s.gzipBytes(plain)
		}
		s.expoCache[cacheKey] = expoCacheEntry{plain: append([]byte(nil), plain...), gzipped: gzipped, contentType: ct, fingerprint: fp}
		// Bound cache size roughly.
		if len(s.expoCache) > 64 {
			s.expoCache = map[string]expoCacheEntry{cacheKey: s.expoCache[cacheKey]}
		}
	}
	s.expoMu.Unlock()

	w.Header().Set("Content-Type", ct)
	w.Header().Set("Vary", "Accept, Accept-Encoding")
	if wantGzip {
		if gzipped == nil {
			gzipped = s.gzipBytes(plain)
		}
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzipped)
		return
	}
	_, _ = w.Write(plain)
}

func (s *Server) gzipBytes(plain []byte) []byte {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, _ = zw.Write(plain)
	_ = zw.Close()
	return b.Bytes()
}

func fingerprintBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:8])
}

func prefersOpenMetrics(accept string) bool {
	return strings.Contains(strings.ToLower(accept), "application/openmetrics-text")
}

func acceptsGzip(ae string) bool {
	for _, part := range strings.Split(ae, ",") {
		coding := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if strings.EqualFold(coding, "gzip") {
			return true
		}
	}
	return false
}

func gatherSelf(g prometheus.Gatherer) (string, error) {
	mfs, err := g.Gather()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&b, mf); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	enabled := s.cfg.ProbeEnabled()
	sched := s.sched
	s.mu.RUnlock()
	if !enabled {
		http.Error(w, "probe endpoint disabled", http.StatusNotFound)
		return
	}
	if sched == nil {
		http.Error(w, "scheduler not ready", http.StatusServiceUnavailable)
		return
	}
	target := r.URL.Query().Get("target")
	if target == "" {
		http.Error(w, "target query parameter required (group/name)", http.StatusBadRequest)
		return
	}
	timeout := defaultProbeTimeout
	if h := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"); h != "" {
		if sec, err := strconv.ParseFloat(h, 64); err == nil && sec > 0.5 {
			timeout = time.Duration((sec - 0.5) * float64(time.Second))
		}
	}
	if timeout > maxProbeTimeout {
		timeout = maxProbeTimeout
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	snap, err := sched.CollectNow(ctx, target)
	if err != nil && !errors.Is(err, scheduler.ErrUnknownTarget) {
		s.log.Warn("probe collection error", "target", target, "err", safeerr.Err(err))
	}
	if errors.Is(err, scheduler.ErrUnknownTarget) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	success := snap != nil && snap.Success
	var families []model.Family
	if snap != nil && snap.Success {
		families = snap.Families
	}
	// Always emit a blackbox-style success gauge so scrapers can alert on failure.
	v := 0.0
	if success {
		v = 1
	}
	families = append(families, model.Family{
		Name: "rabbitmq_unified_probe_success",
		Help: "1 if /probe collection succeeded for the requested target",
		Type: model.TypeGauge,
		Samples: []model.Sample{{
			Labels: []model.LabelPair{{Name: "target", Value: target}},
			Value:  v,
		}},
	})
	s.writeFamilies(w, r, families, false)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready != nil && !s.ready() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Server) handleTargets(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	if !s.cfg.DiagnosticsEnabled() {
		s.mu.RUnlock()
		http.Error(w, "diagnostics disabled", http.StatusNotFound)
		return
	}
	targets := append([]config.ResolvedTarget(nil), s.targets...)
	metricsPath := s.metricsPathLocked()
	s.mu.RUnlock()

	type scrapePath struct {
		Path   string `json:"path"`
		Source string `json:"source,omitempty"`
	}
	type sourceInfo struct {
		Name     string  `json:"name"`
		Success  bool    `json:"success"`
		Error    string  `json:"error,omitempty"`
		Duration float64 `json:"duration_seconds,omitempty"`
		Samples  int     `json:"samples,omitempty"`
	}
	type snapInfo struct {
		CollectedAt time.Time `json:"collected_at"`
		Success     bool      `json:"success"`
		Error       string    `json:"error,omitempty"`
		SeriesCount int       `json:"series_count"`
	}
	type targetInfo struct {
		ID            string       `json:"id"`
		Name          string       `json:"name"`
		Group         string       `json:"group"`
		Labels        map[string]string `json:"labels"`
		MetricsSource string       `json:"metrics_source"`
		ScrapePaths   []scrapePath `json:"scrape_paths"`
		Snapshot      *snapInfo    `json:"snapshot,omitempty"`
		Sources       []sourceInfo `json:"sources,omitempty"`
	}
	out := make([]targetInfo, 0, len(targets))
	for _, t := range targets {
		info := targetInfo{
			ID:            t.ID,
			Name:          t.Name,
			Group:         t.Group,
			Labels:        t.Labels,
			MetricsSource: t.MetricsSource,
			ScrapePaths: []scrapePath{
				{Path: fmt.Sprintf("%s/targets/%s/%s", metricsPath, t.Group, t.Name)},
				{Path: fmt.Sprintf("%s/targets/%s/%s/prometheus", metricsPath, t.Group, t.Name), Source: "prometheus"},
				{Path: fmt.Sprintf("%s/targets/%s/%s/management", metricsPath, t.Group, t.Name), Source: "management"},
			},
		}
		if t.Probes.AMQPTCP.Enabled || t.Probes.AMQPAuth.Enabled || t.Probes.DeliveryTest.Enabled {
			info.ScrapePaths = append(info.ScrapePaths, scrapePath{
				Path:   fmt.Sprintf("%s/targets/%s/%s/probes", metricsPath, t.Group, t.Name),
				Source: "probes",
			})
		}
		if snap := s.store.Get(t.ID); snap != nil {
			info.Snapshot = &snapInfo{
				CollectedAt: snap.CollectedAt,
				Success:     snap.Success,
				Error:       safeerr.SanitizeSourceError(snap.Error),
				SeriesCount: snap.SeriesCount,
			}
			for name, st := range snap.Sources {
				info.Sources = append(info.Sources, sourceInfo{
					Name:     name,
					Success:  st.Success,
					Error:    safeerr.SanitizeSourceError(st.Error),
					Duration: st.Duration.Seconds(),
					Samples:  st.Samples,
				})
			}
		}
		out = append(out, info)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	if !s.cfg.DiagnosticsEnabled() {
		s.mu.RUnlock()
		http.Error(w, "diagnostics disabled", http.StatusNotFound)
		return
	}
	schema := s.schema
	s.mu.RUnlock()

	type caps struct {
		MetricsSource []string            `json:"metrics_sources"`
		Probes        []string            `json:"probes"`
		Health        []string            `json:"health_metrics"`
		Mapping       []map[string]string `json:"json_mapping,omitempty"`
		Endpoints     []string            `json:"http_endpoints"`
	}
	c := caps{
		MetricsSource: []string{"prometheus", "json"},
		Probes:        []string{"amqp_tcp", "amqp_auth", "delivery_test", "tls_cert_expiry"},
		Health: []string{
			"rabbitmq_cluster_nodes", "rabbitmq_cluster_nodes_running", "rabbitmq_cluster_partitions",
			"rabbitmq_cluster_version_skew", "rabbitmq_application_running",
			"rabbitmq_quorum_queue_available", "rabbitmq_vhost_running", "rabbitmq_expected_target",
			"rabbitmq_health_check_status", "rabbitmq_queue_state",
		},
		Endpoints: []string{
			"/metrics",
			"/metrics/self",
			"/metrics/{group}",
			"/metrics/targets/{group}/{name}",
			"/metrics/targets/{group}/{name}/{source}",
			"/probe", "/healthz", "/readyz", "/targets", "/capabilities",
		},
	}
	if schema != nil {
		c.Mapping = schema.CompatibilityMatrix()
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
	}
}

// UpdateRuntime swaps config, targets and scheduler after a successful reload.
func (s *Server) UpdateRuntime(cfg *config.Config, targets []config.ResolvedTarget, sched *scheduler.Scheduler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	s.targets = targets
	s.sched = sched
}

// SchedulerRunning reports whether the current scheduler loops are active.
func (s *Server) SchedulerRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sched != nil && s.sched.Running()
}

// TargetCount returns the number of configured targets.
func (s *Server) TargetCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.targets)
}
