// Package config loads and validates the exporter YAML configuration.
package config

import (
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"gopkg.in/yaml.v3"
)

// Config is the root configuration document.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Defaults DefaultsConfig `yaml:"defaults"`
	Sharding ShardingConfig `yaml:"sharding"`
	Groups   []GroupConfig  `yaml:"groups"`
}

// ServerConfig controls the HTTP metrics server.
type ServerConfig struct {
	ListenAddress string `yaml:"listen_address"`
	MetricsPath   string `yaml:"metrics_path"`
	// TLSCertFile / TLSKeyFile enable HTTPS on the metrics listener (PEM paths).
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`
	// BasicAuth protects all endpoints except /healthz and /readyz when set.
	BasicAuth BasicAuthConfig `yaml:"basic_auth"`
	// EnableProbe exposes GET /probe (default false — opt-in).
	EnableProbe *bool `yaml:"enable_probe"`
	// EnableDiagnostics exposes /targets and /capabilities (default false — opt-in).
	EnableDiagnostics *bool `yaml:"enable_diagnostics"`
	ReadTimeout       Duration `yaml:"read_timeout"`
	WriteTimeout      Duration `yaml:"write_timeout"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
}

// DefaultsConfig holds global defaults inherited by groups/targets.
type DefaultsConfig struct {
	ScrapeInterval   Duration       `yaml:"scrape_interval"`
	ScrapeTimeout    Duration       `yaml:"scrape_timeout"`
	MaxConcurrency   int            `yaml:"max_concurrency"`
	MaxResponseBytes int64          `yaml:"max_response_bytes"`
	MaxStaleness     Duration       `yaml:"max_staleness"`
	JitterRatio      float64        `yaml:"jitter_ratio"`
	Labels           map[string]string `yaml:"labels"`
	Sanitize         SanitizeConfig `yaml:"sanitize"`
	Limits           LimitsConfig   `yaml:"limits"`
}

// ShardingConfig enables deterministic target sharding across replicas.
type ShardingConfig struct {
	Enabled    bool `yaml:"enabled"`
	ShardIndex int  `yaml:"shard_index"`
	ShardCount int  `yaml:"shard_count"`
}

// SanitizeConfig controls Prometheus exposition sanitization.
type SanitizeConfig struct {
	Enabled           bool     `yaml:"enabled"`
	InvalidValues     []string `yaml:"invalid_values"`
	Strategy          string   `yaml:"strategy"` // drop_sample
	MaxInvalidSamples int      `yaml:"max_invalid_samples"`
}

// LimitsConfig controls cardinality and API page sizes.
type LimitsConfig struct {
	MaxSeriesPerTarget int `yaml:"max_series_per_target"`
	MaxQueues          int `yaml:"max_queues"`
	PageSize           int `yaml:"page_size"`
}

// GroupConfig is a named set of targets sharing labels and collector settings.
type GroupConfig struct {
	Name                        string            `yaml:"name"`
	Labels                      map[string]string `yaml:"labels"`
	ExpectedClusterNodes        int               `yaml:"expected_cluster_nodes"`
	ExpectedRabbitMQClusterName string            `yaml:"expected_rabbitmq_cluster_name"`
	ScrapeInterval              Duration          `yaml:"scrape_interval"`
	ScrapeTimeout               Duration          `yaml:"scrape_timeout"`
	MaxStaleness                Duration          `yaml:"max_staleness"`
	MetricsSource               string            `yaml:"metrics_source"` // prometheus | json
	Prometheus                  PrometheusConfig  `yaml:"prometheus"`
	Management                  ManagementConfig  `yaml:"management"`
	Probes                      ProbesConfig      `yaml:"probes"`
	Limits                      LimitsConfig      `yaml:"limits"`
	Targets                     []TargetConfig    `yaml:"targets"`
}

// PrometheusConfig selects native Prometheus plugin endpoints.
type PrometheusConfig struct {
	Enabled   *bool               `yaml:"enabled"`
	Endpoints []PrometheusEndpoint `yaml:"endpoints"`
	TLS       TLSConfig           `yaml:"tls"`
	Auth      BasicAuthConfig     `yaml:"auth"`
}

// PrometheusEndpoint is one scrape path with optional query params.
type PrometheusEndpoint struct {
	Path   string              `yaml:"path"`
	Params map[string][]string `yaml:"params"`
}

// ManagementConfig controls the Management HTTP API collector.
type ManagementConfig struct {
	Enabled     bool            `yaml:"enabled"`
	Credentials BasicAuthConfig `yaml:"credentials"`
	TLS         TLSConfig       `yaml:"tls"`
	Collectors  []string        `yaml:"collectors"`
	// AllowUnpaginatedQueues permits falling back to full /api/queues when pagination fails.
	AllowUnpaginatedQueues bool `yaml:"allow_unpaginated_queues"`
}

// ProbesConfig controls active AMQP / TCP checks.
type ProbesConfig struct {
	AMQPTCP      ProbeAMQPTCP      `yaml:"amqp_tcp"`
	AMQPAuth     ProbeAMQPAuth     `yaml:"amqp_auth"`
	DeliveryTest ProbeDeliveryTest `yaml:"delivery_test"`
	EmitProbeSuccessAlias bool     `yaml:"emit_probe_success_alias"`
}

// ProbeAMQPTCP is a TCP connectivity check.
type ProbeAMQPTCP struct {
	Enabled  bool     `yaml:"enabled"`
	Interval Duration `yaml:"interval"`
	Timeout  Duration `yaml:"timeout"`
}

// ProbeAMQPAuth is an AMQP handshake + auth check.
type ProbeAMQPAuth struct {
	Enabled     bool              `yaml:"enabled"`
	Interval    Duration          `yaml:"interval"`
	Timeout     Duration          `yaml:"timeout"`
	VHosts      []string          `yaml:"vhosts"`
	Credentials BasicAuthConfig   `yaml:"credentials"`
	TLS         TLSConfig         `yaml:"tls"`
}

// ProbeDeliveryTest is an end-to-end publish/consume test.
type ProbeDeliveryTest struct {
	Enabled            bool            `yaml:"enabled"`
	Interval           Duration        `yaml:"interval"`
	Timeout            Duration        `yaml:"timeout"`
	VHost              string          `yaml:"vhost"`
	Credentials        BasicAuthConfig `yaml:"credentials"`
	TLS                TLSConfig       `yaml:"tls"`
	MaxConcurrentTests int             `yaml:"max_concurrent_tests"`
}

// TargetConfig is a single RabbitMQ broker instance.
type TargetConfig struct {
	Name          string            `yaml:"name"`
	MetricsURL    string            `yaml:"metrics_url"`
	ManagementURL string            `yaml:"management_url"`
	AMQPAddress   string            `yaml:"amqp_address"`
	Labels        map[string]string `yaml:"labels"`
	MetricsSource string            `yaml:"metrics_source"`
	Prometheus    *PrometheusConfig `yaml:"prometheus"`
	Management    *ManagementConfig `yaml:"management"`
	Probes        *ProbesConfig     `yaml:"probes"`
	// ClusterObserver marks this target as the sole runner of cluster-wide Management collectors.
	// When unset, the lexicographically first target name in the group is chosen.
	ClusterObserver *bool `yaml:"cluster_observer"`
}

// BasicAuthConfig supports inline values or *_file references.
type BasicAuthConfig struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	UsernameFile string `yaml:"username_file"`
	PasswordFile string `yaml:"password_file"`
}

// Resolve reads credentials from files when configured.
func (b BasicAuthConfig) Resolve() (user, pass string, err error) {
	user = b.Username
	pass = b.Password
	if b.UsernameFile != "" {
		data, err := os.ReadFile(b.UsernameFile)
		if err != nil {
			return "", "", fmt.Errorf("read username_file: %w", err)
		}
		user = strings.TrimSpace(string(data))
	}
	if b.PasswordFile != "" {
		data, err := os.ReadFile(b.PasswordFile)
		if err != nil {
			return "", "", fmt.Errorf("read password_file: %w", err)
		}
		pass = strings.TrimSpace(string(data))
	}
	return user, pass, nil
}

// TLSConfig controls TLS client settings.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	ServerName         string `yaml:"server_name"`
}

// Duration wraps time.Duration for YAML parsing.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	if s == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// ResolvedTarget is a fully expanded target ready for collection.
type ResolvedTarget struct {
	ID                          string
	Name                        string
	Group                       string
	Labels                      map[string]string
	MetricsURL                  string
	ManagementURL               string
	AMQPAddress                 string
	ScrapeInterval              time.Duration
	ScrapeTimeout               time.Duration
	MaxStaleness                time.Duration
	JitterRatio                 float64
	MaxResponseBytes            int64
	MetricsSource               string
	ExpectedClusterNodes        int
	ExpectedRabbitMQClusterName string
	Prometheus                  PrometheusConfig
	Management                  ManagementConfig
	Probes                      ProbesConfig
	Sanitize                    SanitizeConfig
	Limits                      LimitsConfig
	ClusterObserver bool
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, []ResolvedTarget, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	return Parse(data)
}

// Parse parses YAML bytes into a validated config and resolved targets.
func Parse(data []byte) (*Config, []ResolvedTarget, error) {
	cfg := defaultConfig()
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		// Empty document is invalid; EOF with no content is a parse error upstream.
		return nil, nil, fmt.Errorf("parse yaml: %w", err)
	}
	applyDefaults(&cfg)
	targets, err := ResolveTargets(&cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := Validate(&cfg, targets); err != nil {
		return nil, nil, err
	}
	targets = ApplySharding(targets, cfg.Sharding)
	return &cfg, targets, nil
}

func defaultConfig() Config {
	probe := false
	diag := false
	return Config{
		Server: ServerConfig{
			ListenAddress:     ":9419",
			MetricsPath:       "/metrics",
			EnableProbe:       &probe,
			EnableDiagnostics: &diag,
			ReadTimeout:       Duration(30 * time.Second),
			WriteTimeout:      Duration(60 * time.Second),
			IdleTimeout:       Duration(120 * time.Second),
		},
		Defaults: DefaultsConfig{
			ScrapeInterval:   Duration(15 * time.Second),
			ScrapeTimeout:    Duration(10 * time.Second),
			MaxConcurrency:   10,
			MaxResponseBytes: 16 * 1024 * 1024,
			MaxStaleness:     Duration(0), // computed as 2x interval if unset
			JitterRatio:      0.1,
			Sanitize: SanitizeConfig{
				Enabled: true,
				InvalidValues: []string{"unknown", "undefined", "null"},
				Strategy:          "drop_sample",
				MaxInvalidSamples: 10000,
			},
			Limits: LimitsConfig{
				MaxSeriesPerTarget: 200000,
				MaxQueues:          5000,
				PageSize:           500,
			},
		},
	}
}

func applyDefaults(cfg *Config) {
	if cfg.Server.ListenAddress == "" {
		cfg.Server.ListenAddress = ":9419"
	}
	if cfg.Server.MetricsPath == "" {
		cfg.Server.MetricsPath = "/metrics"
	}
	if cfg.Server.EnableProbe == nil {
		v := false
		cfg.Server.EnableProbe = &v
	}
	if cfg.Server.EnableDiagnostics == nil {
		v := false
		cfg.Server.EnableDiagnostics = &v
	}
	if cfg.Server.ReadTimeout == 0 {
		cfg.Server.ReadTimeout = Duration(30 * time.Second)
	}
	if cfg.Server.WriteTimeout == 0 {
		cfg.Server.WriteTimeout = Duration(60 * time.Second)
	}
	if cfg.Server.IdleTimeout == 0 {
		cfg.Server.IdleTimeout = Duration(120 * time.Second)
	}
	if cfg.Defaults.MaxConcurrency <= 0 {
		cfg.Defaults.MaxConcurrency = 10
	}
	if cfg.Defaults.MaxResponseBytes <= 0 {
		cfg.Defaults.MaxResponseBytes = 16 * 1024 * 1024
	}
	if cfg.Defaults.JitterRatio < 0 {
		cfg.Defaults.JitterRatio = 0
	}
	if cfg.Defaults.JitterRatio > 1 {
		cfg.Defaults.JitterRatio = 1
	}
	if cfg.Defaults.Sanitize.Strategy == "" {
		cfg.Defaults.Sanitize.Strategy = "drop_sample"
	}
	if len(cfg.Defaults.Sanitize.InvalidValues) == 0 {
		cfg.Defaults.Sanitize.InvalidValues = []string{"unknown", "undefined", "null"}
	}
}

// ProbeEnabled reports whether /probe is exposed (opt-in; default false).
func (c *Config) ProbeEnabled() bool {
	return c.Server.EnableProbe != nil && *c.Server.EnableProbe
}

// DiagnosticsEnabled reports whether /targets and /capabilities are exposed (opt-in; default false).
func (c *Config) DiagnosticsEnabled() bool {
	return c.Server.EnableDiagnostics != nil && *c.Server.EnableDiagnostics
}

// ResolveTargets expands groups into fully labeled targets.
func ResolveTargets(cfg *Config) ([]ResolvedTarget, error) {
	var out []ResolvedTarget
	seenIDs := map[string]struct{}{}

	for _, g := range cfg.Groups {
		if g.Name == "" {
			return nil, fmt.Errorf("group name is required")
		}
		metricsSource := g.MetricsSource
		if metricsSource == "" {
			metricsSource = "prometheus"
		}
		interval := g.ScrapeInterval.Duration()
		if interval == 0 {
			interval = cfg.Defaults.ScrapeInterval.Duration()
		}
		timeout := g.ScrapeTimeout.Duration()
		if timeout == 0 {
			timeout = cfg.Defaults.ScrapeTimeout.Duration()
		}
		staleness := g.MaxStaleness.Duration()
		if staleness == 0 {
			staleness = cfg.Defaults.MaxStaleness.Duration()
		}
		if staleness == 0 {
			staleness = 2 * interval
		}
		limits := mergeLimits(cfg.Defaults.Limits, g.Limits)

		for _, t := range g.Targets {
			if t.Name == "" {
				return nil, fmt.Errorf("group %q: target name is required", g.Name)
			}
			id := g.Name + "/" + t.Name
			if _, ok := seenIDs[id]; ok {
				return nil, fmt.Errorf("duplicate target id %q", id)
			}
			seenIDs[id] = struct{}{}

			labels := mergeMaps(cfg.Defaults.Labels, g.Labels, t.Labels)
			if _, ok := labels["instance"]; !ok {
				labels["instance"] = t.Name
			}

			src := t.MetricsSource
			if src == "" {
				src = metricsSource
			}
			prom := g.Prometheus
			if t.Prometheus != nil {
				prom = *t.Prometheus
			}
			mgmt := g.Management
			if t.Management != nil {
				mgmt = *t.Management
			}
			probes := g.Probes
			if t.Probes != nil {
				probes = *t.Probes
			}

			// Default prometheus endpoints.
			if src == "prometheus" && len(prom.Endpoints) == 0 {
				prom.Endpoints = []PrometheusEndpoint{{Path: "/metrics"}}
			}
			// JSON mode implies management enabled.
			if src == "json" {
				mgmt.Enabled = true
			}
			if len(mgmt.Collectors) == 0 && mgmt.Enabled {
				mgmt.Collectors = []string{"overview", "nodes", "queues", "vhosts", "health_checks"}
			}

			observer := false
			if t.ClusterObserver != nil {
				observer = *t.ClusterObserver
			}
			out = append(out, ResolvedTarget{
				ID:                          id,
				Name:                        t.Name,
				Group:                       g.Name,
				Labels:                      labels,
				MetricsURL:                  strings.TrimRight(t.MetricsURL, "/"),
				ManagementURL:               strings.TrimRight(t.ManagementURL, "/"),
				AMQPAddress:                 t.AMQPAddress,
				ScrapeInterval:              interval,
				ScrapeTimeout:               timeout,
				MaxStaleness:                staleness,
				JitterRatio:                 cfg.Defaults.JitterRatio,
				MaxResponseBytes:            cfg.Defaults.MaxResponseBytes,
				MetricsSource:               src,
				ExpectedClusterNodes:        g.ExpectedClusterNodes,
				ExpectedRabbitMQClusterName: g.ExpectedRabbitMQClusterName,
				Prometheus:                  prom,
				Management:                  mgmt,
				Probes:                      probes,
				Sanitize:                    cfg.Defaults.Sanitize,
				Limits:                      limits,
				ClusterObserver:             observer,
			})
		}
	}
	assignClusterObservers(out)
	return out, nil
}

// assignClusterObservers picks one observer per group for cluster-wide Management collectors.
func assignClusterObservers(targets []ResolvedTarget) {
	byGroup := map[string][]int{}
	for i, t := range targets {
		byGroup[t.Group] = append(byGroup[t.Group], i)
	}
	for _, idxs := range byGroup {
		explicit := -1
		for _, i := range idxs {
			if targets[i].ClusterObserver {
				explicit = i
				break
			}
		}
		if explicit >= 0 {
			for _, i := range idxs {
				targets[i].ClusterObserver = i == explicit
			}
			continue
		}
		best := idxs[0]
		for _, i := range idxs[1:] {
			if targets[i].Name < targets[best].Name {
				best = i
			}
		}
		for _, i := range idxs {
			targets[i].ClusterObserver = i == best
		}
	}
}

func mergeMaps(parts ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, p := range parts {
		for k, v := range p {
			out[k] = v
		}
	}
	return out
}

func mergeLimits(base, over LimitsConfig) LimitsConfig {
	out := base
	if over.MaxSeriesPerTarget > 0 {
		out.MaxSeriesPerTarget = over.MaxSeriesPerTarget
	}
	if over.MaxQueues > 0 {
		out.MaxQueues = over.MaxQueues
	}
	if over.PageSize > 0 {
		out.PageSize = over.PageSize
	}
	return out
}

// Validate checks configuration constraints.
func Validate(cfg *Config, targets []ResolvedTarget) error {
	if len(cfg.Groups) == 0 {
		return fmt.Errorf("at least one group is required")
	}
	if (cfg.Server.TLSCertFile == "") != (cfg.Server.TLSKeyFile == "") {
		return fmt.Errorf("server.tls_cert_file and server.tls_key_file must both be set or both empty")
	}
	if cfg.Sharding.Enabled {
		if cfg.Sharding.ShardCount <= 0 {
			return fmt.Errorf("sharding.shard_count must be > 0")
		}
		if cfg.Sharding.ShardIndex < 0 || cfg.Sharding.ShardIndex >= cfg.Sharding.ShardCount {
			return fmt.Errorf("sharding.shard_index must be in [0, shard_count)")
		}
	}

	identityKeys := []string{"job", "instance", "environment", "domain", "platform", "projectid", "cluster"}
	seenIdentity := map[string]string{}

	for _, t := range targets {
		for name := range t.Labels {
			if _, ok := model.IntrinsicLabels[name]; ok {
				return fmt.Errorf("target %q: label %q is intrinsic and must not be set in config", t.ID, name)
			}
		}
		switch t.MetricsSource {
		case "prometheus", "json":
		default:
			return fmt.Errorf("target %q: invalid metrics_source %q", t.ID, t.MetricsSource)
		}
		if t.MetricsSource == "prometheus" && t.MetricsURL == "" {
			return fmt.Errorf("target %q: metrics_url is required for prometheus mode", t.ID)
		}
		if (t.MetricsSource == "json" || t.Management.Enabled) && t.ManagementURL == "" {
			return fmt.Errorf("target %q: management_url is required when management is enabled", t.ID)
		}
		if t.Probes.AMQPTCP.Enabled || t.Probes.AMQPAuth.Enabled || t.Probes.DeliveryTest.Enabled {
			if t.AMQPAddress == "" {
				return fmt.Errorf("target %q: amqp_address is required when AMQP probes are enabled", t.ID)
			}
		}
		for _, ep := range t.Prometheus.Endpoints {
			if ep.Path == "/metrics/detailed" {
				if families, ok := ep.Params["family"]; !ok || len(families) == 0 {
					return fmt.Errorf("target %q: /metrics/detailed requires params.family", t.ID)
				}
			}
		}
		if t.ScrapeTimeout >= t.ScrapeInterval && t.ScrapeInterval > 0 {
			return fmt.Errorf("target %q: scrape_timeout (%s) must be < scrape_interval (%s)", t.ID, t.ScrapeTimeout, t.ScrapeInterval)
		}
		if t.Limits.PageSize <= 0 || t.Limits.PageSize > 500 {
			return fmt.Errorf("target %q: limits.page_size must be in [1, 500] (got %d)", t.ID, t.Limits.PageSize)
		}
		if err := validateHTTPURL(t.ID, "metrics_url", t.MetricsURL); err != nil {
			return err
		}
		if err := validateHTTPURL(t.ID, "management_url", t.ManagementURL); err != nil {
			return err
		}
		if t.AMQPAddress != "" && (strings.Contains(t.AMQPAddress, "://") || !strings.Contains(t.AMQPAddress, ":")) {
			return fmt.Errorf("target %q: amqp_address must be host:port", t.ID)
		}

		// Collision detection on identity label sets.
		parts := make([]string, 0, len(identityKeys))
		for _, k := range identityKeys {
			parts = append(parts, k+"="+t.Labels[k])
		}
		key := strings.Join(parts, ",")
		if prev, ok := seenIdentity[key]; ok {
			return fmt.Errorf("targets %q and %q produce identical identity label sets", prev, t.ID)
		}
		seenIdentity[key] = t.ID
	}
	return nil
}

func validateHTTPURL(targetID, field, raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("target %q: %s is not a valid URL: %w", targetID, field, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return fmt.Errorf("target %q: %s must use http or https scheme", targetID, field)
	}
	if u.Host == "" {
		return fmt.Errorf("target %q: %s must include a host", targetID, field)
	}
	if u.User != nil {
		return fmt.Errorf("target %q: %s must not embed credentials; use *_file auth instead", targetID, field)
	}
	return nil
}

// ApplySharding filters targets belonging to this shard.
func ApplySharding(targets []ResolvedTarget, s ShardingConfig) []ResolvedTarget {
	if !s.Enabled || s.ShardCount <= 1 {
		return targets
	}
	out := make([]ResolvedTarget, 0, len(targets)/s.ShardCount+1)
	for _, t := range targets {
		h := fnv.New32a()
		_, _ = h.Write([]byte(t.ID))
		if int(h.Sum32())%s.ShardCount == s.ShardIndex {
			out = append(out, t)
		}
	}
	return out
}

// Reloader watches a config file path for changes (caller triggers via SIGHUP or poll).
func Reloader(path string, currentModTime time.Time) (changed bool, modTime time.Time, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, currentModTime, err
	}
	mt := fi.ModTime()
	if !currentModTime.IsZero() && mt.After(currentModTime) {
		return true, mt, nil
	}
	return false, mt, nil
}

// AbsPath resolves relative paths against a base directory.
func AbsPath(base, p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(base, p)
}
