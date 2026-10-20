// Package collector orchestrates Prometheus, JSON, health and probe collection per target.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/health"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mapping"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/probes"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/promsrc"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/safeerr"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/selfmetrics"
)

// Collector implements scheduler.Collector.
type Collector struct {
	metrics *selfmetrics.Registry
	probes  *probes.Runner
	log     *slog.Logger
	schema  *mapping.Schema // default 3.9 schema; per-version loaded at runtime
	http    *httpPool
}

// New creates an orchestrating collector.
func New(metrics *selfmetrics.Registry, log *slog.Logger) *Collector {
	schema, err := mapping.LoadEmbedded("3.9.15")
	if err != nil {
		log.Warn("failed to load embedded mapping schema", "err", err)
	}
	return &Collector{
		metrics: metrics,
		probes:  probes.NewRunner(0), // per-target max_concurrent_tests applied at run time
		log:     log,
		schema:  schema,
		http:    newHTTPPool(),
	}
}

// ResetHTTPClients drops pooled HTTP clients (call after config reload).
func (c *Collector) ResetHTTPClients() {
	if c.http != nil {
		c.http.Reset()
	}
}

type collectResult struct {
	families []model.Family
	status   model.SourceStatus
	ok       bool
	required bool
	// management side-products for probes re-eval
	client     *mgmt.Client
	overview   *mgmt.Overview
	nodes      []mgmt.NodeInfo
	selfNode   *mgmt.NodeInfo
	queues     []mgmt.Queue
	vhosts     []mgmt.VHost
	federation []mgmt.FederationLink
	shovels    []mgmt.Shovel
	selfName   string
}

// Collect runs all enabled sources for a target.
// Prometheus and Management use independent timeouts and run concurrently so a
// slow Management API never cancels classic /metrics scrapes (and vice versa).
func (c *Collector) Collect(ctx context.Context, t config.ResolvedTarget) (*model.Snapshot, error) {
	start := time.Now()
	snap := &model.Snapshot{
		TargetID:    t.ID,
		Group:       t.Group,
		CollectedAt: start,
		Sources:     map[string]model.SourceStatus{},
	}
	instance := t.Labels["instance"]
	if instance == "" {
		instance = t.Name
	}
	meta := selfmetrics.MetaFromLabels(t.ID, t.Group, instance, t.Labels)
	c.metrics.RememberTarget(meta)

	needMgmt := t.MetricsSource == "json" || t.Management.Enabled
	needProm := t.MetricsSource == "prometheus"

	var (
		promRes collectResult
		mgmtRes collectResult
		wg      sync.WaitGroup
	)

	if needProm {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Independent deadline: not shared with management.
			pctx, cancel := context.WithTimeout(ctx, t.ScrapeTimeout)
			defer cancel()
			promRes = c.collectPrometheus(pctx, t, meta)
		}()
	}

	if needMgmt {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mctx, cancel := context.WithTimeout(ctx, t.ScrapeTimeout)
			defer cancel()
			mgmtRes = c.collectManagement(mctx, t, meta)
		}()
	}

	wg.Wait()

	var parts [][]model.Family
	bySource := map[string][]model.Family{}
	sourcesOK := map[string]bool{}
	anyRequiredFail := false

	if needProm {
		snap.Sources["prometheus"] = promRes.status
		sourcesOK["prometheus"] = promRes.ok
		if len(promRes.families) > 0 {
			parts = append(parts, promRes.families)
			bySource["prometheus"] = promRes.families
		}
		if promRes.required && !promRes.ok {
			anyRequiredFail = true
		}
	}

	if needMgmt {
		snap.Sources["management"] = mgmtRes.status
		sourcesOK["management"] = mgmtRes.ok
		if len(mgmtRes.families) > 0 {
			parts = append(parts, mgmtRes.families)
			bySource["management"] = mgmtRes.families
		}
		if mgmtRes.required && !mgmtRes.ok {
			anyRequiredFail = true
		}
		if needProm && !promRes.ok && mgmtRes.ok {
			c.log.Warn("prometheus failed; publishing management metrics only",
				"target", t.ID, "prometheus_err", promRes.status.Error)
		}
	} else {
		hf, _ := health.Evaluate(ctx, health.Input{Target: t})
		if len(hf) > 0 {
			parts = append(parts, hf)
			bySource["management"] = hf
		}
	}

	probeEnabled := t.Probes.AMQPTCP.Enabled || t.Probes.AMQPAuth.Enabled || t.Probes.DeliveryTest.Enabled
	if probeEnabled {
		st := model.SourceStatus{Name: "probes"}
		srcStart := time.Now()
		pctx, cancel := context.WithTimeout(ctx, t.ScrapeTimeout)
		pres := c.probes.Run(pctx, t)
		cancel()
		st.Duration = time.Since(srcStart)
		st.Success = true
		if t.Probes.AMQPTCP.Enabled && !pres.TCPOK {
			st.Success = false
		}
		if pres.AuthOK != nil {
			c.metrics.SetProbe(meta, "amqp_auth", *pres.AuthOK)
		}
		if t.Probes.AMQPTCP.Enabled {
			c.metrics.SetProbe(meta, "amqp_tcp", pres.TCPOK)
		}
		if pres.Delivery != nil {
			c.metrics.SetProbe(meta, "delivery_test", *pres.Delivery)
		}
		var probeParts []model.Family
		if needMgmt && mgmtRes.overview != nil {
			hf, _ := health.Evaluate(ctx, health.Input{
				Target:             t,
				SelfNode:           mgmtRes.selfName,
				Self:               mgmtRes.selfNode,
				Overview:           mgmtRes.overview,
				Nodes:              mgmtRes.nodes,
				Queues:             mgmtRes.queues,
				VHosts:             mgmtRes.vhosts,
				FederationLinks:    mgmtRes.federation,
				Shovels:            mgmtRes.shovels,
				Client:             mgmtRes.client,
				AMQPAuthSuccess:    pres.AuthOK,
				EmitAsLeaderOnly:   !t.ClusterObserver,
				EmitClusterMetrics: t.ClusterObserver,
				RunHealthChecks:    false,
			})
			if len(hf) > 0 {
				parts = append(parts, hf)
				probeParts = append(probeParts, hf...)
			}
		}
		if len(pres.Families) > 0 {
			parts = append(parts, pres.Families)
			probeParts = append(probeParts, pres.Families...)
		}
		if len(probeParts) > 0 {
			bySource["probes"] = probeParts
		}
		st.Samples = model.CountSeries(pres.Families)
		snap.Sources["probes"] = st
		sourcesOK["probes"] = st.Success
	}

	merged := model.MergeFamilies(parts, model.MergeOptions{
		MaxSeries:       t.Limits.MaxSeriesPerTarget,
		DedupeIdentical: true,
		OnDuplicateConflict: func(name string, _, _ model.Sample) {
			c.metrics.IncDuplicate(meta, name)
		},
		OnDropped: func(reason string, count int) {
			c.metrics.IncDropped(meta, reason, count)
		},
		OnTypeConflict: func(name string, _, _ model.MetricType) {
			c.metrics.IncTypeConflict(meta, name)
		},
	})

	snap.Families = merged
	snap.FamiliesBySource = bySource
	snap.SourceCollectedAt = map[string]time.Time{}
	now := time.Now()
	for src, ok := range sourcesOK {
		if ok {
			snap.SourceCollectedAt[src] = now
		}
	}
	snap.SeriesCount = model.CountSeries(merged)
	snap.Success = !anyRequiredFail
	if anyRequiredFail {
		snap.Error = "one or more required sources failed"
	}

	c.metrics.SetCollectionResult(meta, snap.Success, snap.SeriesCount, sourcesOK)
	// data_age must reflect last *successful* scrape — not reset to 0 on failure,
	// otherwise last-good RabbitMQ series look fresh while collection_success=0.
	if age, ok := c.metrics.LastSuccessAge(t.ID, time.Now()); ok {
		c.metrics.SetDataAge(meta, age)
	} else if snap.Success {
		c.metrics.SetDataAge(meta, 0)
	}

	if !snap.Success {
		return snap, fmt.Errorf("%s", snap.Error)
	}
	return snap, nil
}

func (c *Collector) collectPrometheus(ctx context.Context, t config.ResolvedTarget, meta selfmetrics.TargetMeta) collectResult {
	res := collectResult{required: true, status: model.SourceStatus{Name: "prometheus"}}
	srcStart := time.Now()
	tlsCfg, err := promsrc.BuildTLSConfig(t.Prometheus.TLS)
	if err != nil {
		res.status.Error = safeerr.Err(err)
		c.metrics.IncCollectionError(meta, "prometheus", "tls")
		c.log.Warn("prometheus tls config failed", "target", t.ID, "err", safeerr.Err(err))
		return res
	}
	hc := c.http.Client(tlsPoolKey("prom", t.ID, t.ScrapeTimeout, t.Prometheus.TLS), t.ScrapeTimeout, tlsCfg)
	client := promsrc.NewClientWithHTTP(hc, t.MaxResponseBytes, t.Sanitize)
	out, err := client.Collect(ctx, t.MetricsURL, t.Prometheus.Endpoints, t.Prometheus.Auth)
	res.status.Duration = time.Since(srcStart)
	if err != nil {
		res.status.Success = false
		res.status.Error = safeerr.Err(err)
		c.metrics.IncCollectionError(meta, "prometheus", "fetch")
		c.log.Warn("prometheus collect failed", "target", t.ID, "err", safeerr.Err(err))
		return res
	}
	res.ok = true
	res.status.Success = true
	res.status.Samples = model.CountSeries(out.Families)
	res.status.Sanitized = out.Stats.DroppedSamples
	for reason, n := range out.Stats.Reasons {
		c.metrics.AddSanitized(meta, reason, n)
	}
	for _, e := range out.Errors {
		c.metrics.IncCollectionError(meta, "prometheus", "endpoint_partial")
		c.log.Warn("prometheus endpoint failed (partial)", "target", t.ID, "path", e.Path, "err", e.Error)
	}
	res.families = model.ApplyTargetLabels(out.Families, t.Labels)
	return res
}

func (c *Collector) collectManagement(ctx context.Context, t config.ResolvedTarget, meta selfmetrics.TargetMeta) collectResult {
	res := collectResult{
		required: t.MetricsSource == "json",
		status:   model.SourceStatus{Name: "management"},
	}
	srcStart := time.Now()
	defer func() { res.status.Duration = time.Since(srcStart) }()

	tlsCfg, err := promsrc.BuildTLSConfig(t.Management.TLS)
	if err != nil {
		res.status.Error = safeerr.Err(err)
		c.metrics.IncCollectionError(meta, "management", "tls")
		c.log.Warn("management tls config failed", "target", t.ID, "err", safeerr.Err(err))
		return res
	}
	hc := c.http.Client(tlsPoolKey("mgmt", t.ID, t.ScrapeTimeout, t.Management.TLS), t.ScrapeTimeout, tlsCfg)
	client, err := mgmt.NewWithHTTPClient(t.ManagementURL, t.Management.Credentials, hc, t.MaxResponseBytes, t.Limits.PageSize)
	if err != nil {
		res.status.Error = safeerr.Err(err)
		c.metrics.IncCollectionError(meta, "management", "client")
		c.log.Warn("management client failed", "target", t.ID, "err", safeerr.Err(err))
		return res
	}
	res.client = client

	// Overview first — needed for self node name; short-circuit if it fails.
	overview, err := client.Overview(ctx)
	if err != nil {
		res.status.Error = safeerr.Err(err)
		c.metrics.IncCollectionError(meta, "management", "overview")
		c.log.Warn("management overview failed", "target", t.ID, "url", safeerr.RedactURLUserinfo(t.ManagementURL), "err", safeerr.Err(err))
		return res
	}
	res.overview = overview
	res.selfName = overview.Node

	collectors := map[string]bool{}
	for _, name := range t.Management.Collectors {
		collectors[name] = true
	}
	observer := t.ClusterObserver
	needNodesList := observer && (collectors["nodes"] || collectors["overview"] || t.MetricsSource == "json")
	needSelfNode := collectors["nodes"] || collectors["overview"] || t.MetricsSource == "json" || collectors["health_checks"]
	needQueues := observer && (collectors["queues"] || t.MetricsSource == "json")
	needVHosts := observer && collectors["vhosts"]
	needFed := observer && (collectors["federation"] || collectors["federation_links"])
	needShovels := observer && collectors["shovels"]
	needHealth := collectors["health_checks"]

	var (
		mu       sync.Mutex
		errs     []string
		nodes    []mgmt.NodeInfo
		selfNode *mgmt.NodeInfo
		queues   []mgmt.Queue
		vhosts   []mgmt.VHost
		fed      []mgmt.FederationLink
		shovels  []mgmt.Shovel
		wg       sync.WaitGroup
	)
	addErr := func(msg string) {
		mu.Lock()
		errs = append(errs, safeerr.String(msg))
		mu.Unlock()
	}

	if needNodesList || needSelfNode {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n []mgmt.NodeInfo
			if needNodesList {
				var err error
				n, err = client.Nodes(ctx)
				if err != nil {
					addErr("nodes: " + safeerr.Err(err))
					return
				}
			}
			var self *mgmt.NodeInfo
			if res.selfName != "" && needSelfNode {
				self, _ = client.Node(ctx, res.selfName)
			}
			mu.Lock()
			nodes = n
			selfNode = self
			mu.Unlock()
		}()
	}
	if needQueues {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qr, err := client.Queues(ctx, t.Limits.MaxQueues, t.Management.AllowUnpaginatedQueues)
			if err != nil {
				addErr("queues: " + safeerr.Err(err))
				return
			}
			if qr.Dropped > 0 {
				c.metrics.IncDropped(meta, "max_queues", qr.Dropped)
				c.log.Warn("queues truncated by max_queues",
					"target", t.ID, "kept", len(qr.Queues), "dropped", qr.Dropped, "total_hint", qr.TotalHint)
			}
			mu.Lock()
			queues = qr.Queues
			mu.Unlock()
		}()
	}
	if needVHosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := client.VHosts(ctx)
			if err != nil {
				addErr("vhosts: " + safeerr.Err(err))
				return
			}
			mu.Lock()
			vhosts = v
			mu.Unlock()
		}()
	}
	if needFed {
		wg.Add(1)
		go func() {
			defer wg.Done()
			links, err := client.FederationLinks(ctx)
			if err != nil {
				addErr("federation: " + safeerr.Err(err))
				c.log.Warn("federation-links fetch failed", "target", t.ID, "err", safeerr.Err(err))
				return
			}
			mu.Lock()
			fed = links
			mu.Unlock()
		}()
	}
	if needShovels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := client.Shovels(ctx)
			if err != nil {
				addErr("shovels: " + safeerr.Err(err))
				c.log.Warn("shovels fetch failed", "target", t.ID, "err", safeerr.Err(err))
				return
			}
			mu.Lock()
			shovels = s
			mu.Unlock()
		}()
	}
	wg.Wait()

	res.nodes = nodes
	res.selfNode = selfNode
	res.queues = queues
	res.vhosts = vhosts
	res.federation = fed
	res.shovels = shovels

	var families []model.Family

	if t.MetricsSource == "json" && c.schema != nil {
		schema := c.schema
		if overview.RabbitMQVersion != "" {
			if s2, err := mapping.LoadEmbedded(overview.RabbitMQVersion); err == nil {
				schema = s2
			}
		}
		mapped := mapping.Apply(schema, mapping.Input{
			SelfNode: res.selfName,
			Overview: overview,
			Nodes:    nodes,
			Self:     selfNode,
			Queues:   queues,
			VHosts:   vhosts,
		})
		for _, u := range mapped.Unsupported {
			c.metrics.SetUnsupported(meta, u, "json")
		}
		for range mapped.Errors {
			c.metrics.IncMappingError(meta, "field")
		}
		families = append(families, model.ApplyTargetLabels(mapped.Families, t.Labels)...)
		res.status.Samples += model.CountSeries(mapped.Families)
	}

	healthFamilies, notes := health.Evaluate(ctx, health.Input{
		Target:             t,
		SelfNode:           res.selfName,
		Self:               selfNode,
		Overview:           overview,
		Nodes:              nodes,
		Queues:             queues,
		VHosts:             vhosts,
		FederationLinks:    fed,
		Shovels:            shovels,
		Client:             client,
		EmitAsLeaderOnly:   !observer,
		RunHealthChecks:    needHealth,
		EmitClusterMetrics: observer,
	})
	for _, n := range notes {
		c.log.Debug("health note", "target", t.ID, "note", n)
	}
	families = append(families, healthFamilies...)
	res.status.Samples += model.CountSeries(healthFamilies)
	res.families = families

	if len(errs) > 0 {
		res.status.Error = safeerr.String(errs[0])
		if len(errs) > 1 {
			res.status.Error = fmt.Sprintf("%s (+%d more)", errs[0], len(errs)-1)
		}
		// Overview succeeded — treat as partial success for hybrid mode.
		res.ok = true
		res.status.Success = true
		return res
	}
	res.ok = true
	res.status.Success = true
	return res
}
