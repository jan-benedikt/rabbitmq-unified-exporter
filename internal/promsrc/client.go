package promsrc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

// Client fetches Prometheus exposition from RabbitMQ.
type Client struct {
	httpClient *http.Client
	maxBytes   int64
	sanitize   config.SanitizeConfig
}

// NewClient creates a Prometheus source client.
func NewClient(timeout time.Duration, maxBytes int64, sanitize config.SanitizeConfig, tlsCfg *tls.Config) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}
	return NewClientWithHTTP(&http.Client{Timeout: timeout, Transport: tr}, maxBytes, sanitize)
}

// NewClientWithHTTP reuses an existing HTTP client (connection pooling across scrapes).
func NewClientWithHTTP(httpClient *http.Client, maxBytes int64, sanitize config.SanitizeConfig) *Client {
	return &Client{
		httpClient: httpClient,
		maxBytes:   maxBytes,
		sanitize:   sanitize,
	}
}

// EndpointError records a failed scrape of one Prometheus path.
type EndpointError struct {
	Path  string
	Error string
}

// FetchResult is the outcome of scraping one or more endpoints.
type FetchResult struct {
	Families  []model.Family
	Stats     SanitizeStats
	Duration  time.Duration
	BytesRead int
	Errors    []EndpointError // non-fatal when at least one endpoint succeeded
}

type endpointResult struct {
	families []model.Family
	stats    SanitizeStats
	bytes    int
	err      *EndpointError
}

// Collect scrapes all configured endpoints for a target in parallel and merges families.
// Failure of one endpoint (e.g. /metrics/detailed) no longer drops successful ones (e.g. /metrics).
func (c *Client) Collect(ctx context.Context, baseURL string, endpoints []config.PrometheusEndpoint, auth config.BasicAuthConfig) (*FetchResult, error) {
	start := time.Now()
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no prometheus endpoints configured")
	}

	user, pass, err := auth.Resolve()
	if err != nil {
		return nil, err
	}

	results := make([]endpointResult, len(endpoints))
	var wg sync.WaitGroup
	// Per-endpoint deadline so a stuck /metrics/detailed cannot cancel /metrics
	// early via a shared context — each scrape gets the full client timeout budget.
	epTimeout := c.httpClient.Timeout
	if epTimeout <= 0 {
		epTimeout = 30 * time.Second
	}
	for i, ep := range endpoints {
		wg.Add(1)
		go func(i int, ep config.PrometheusEndpoint) {
			defer wg.Done()
			epCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), epTimeout)
			defer cancel()
			results[i] = c.scrapeOne(epCtx, baseURL, ep, user, pass)
		}(i, ep)
	}
	wg.Wait()

	var all [][]model.Family
	var totalStats SanitizeStats
	totalStats.Reasons = map[string]int{}
	bytesRead := 0
	var epErrs []EndpointError

	for _, r := range results {
		if r.err != nil {
			epErrs = append(epErrs, *r.err)
			continue
		}
		if r.families == nil {
			continue // optional skipped endpoint (e.g. 404 memory-breakdown)
		}
		all = append(all, r.families)
		bytesRead += r.bytes
		totalStats.DroppedSamples += r.stats.DroppedSamples
		totalStats.KeptSamples += r.stats.KeptSamples
		for k, v := range r.stats.Reasons {
			totalStats.Reasons[k] += v
		}
	}

	if len(all) == 0 {
		if len(epErrs) == 0 {
			return nil, fmt.Errorf("no prometheus endpoints returned data")
		}
		return nil, fmt.Errorf("all prometheus endpoints failed: %s: %s", epErrs[0].Path, epErrs[0].Error)
	}

	merged := model.MergeFamilies(all, model.MergeOptions{DedupeIdentical: true})
	// Expose detailed families under classic rabbitmq_* names on the exporter /metrics.
	merged = model.MergeFamilies([][]model.Family{NormalizeToClassicNames(merged)}, model.MergeOptions{DedupeIdentical: true})
	return &FetchResult{
		Families:  merged,
		Stats:     totalStats,
		Duration:  time.Since(start),
		BytesRead: bytesRead,
		Errors:    epErrs,
	}, nil
}

func (c *Client) scrapeOne(ctx context.Context, baseURL string, ep config.PrometheusEndpoint, user, pass string) endpointResult {
	u, err := buildURL(baseURL, ep)
	if err != nil {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: err.Error()}}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: err.Error()}}
	}
	req.Header.Set("Accept", "text/plain")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: err.Error()}}
	}
	body, err := io.ReadAll(LimitReader(resp.Body, c.maxBytes))
	_ = resp.Body.Close()
	if err != nil {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: err.Error()}}
	}
	if resp.StatusCode == http.StatusNotFound && strings.Contains(ep.Path, "memory-breakdown") {
		return endpointResult{} // skip optional
	}
	if resp.StatusCode != http.StatusOK {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: fmt.Sprintf("HTTP %d", resp.StatusCode)}}
	}
	if len(body) == 0 {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: "empty response"}}
	}

	var families []model.Family
	var stats SanitizeStats
	opts := SanitizeOptions{InvalidValues: nil, MaxInvalidSamples: 1 << 30}
	if c.sanitize.Enabled {
		opts = SanitizeOptions{
			InvalidValues:     c.sanitize.InvalidValues,
			MaxInvalidSamples: c.sanitize.MaxInvalidSamples,
		}
	}
	families, stats, err = Sanitize(body, opts)
	if err != nil {
		return endpointResult{err: &EndpointError{Path: ep.Path, Error: err.Error()}}
	}
	return endpointResult{families: families, stats: stats, bytes: len(body)}
}

func buildURL(base string, ep config.PrometheusEndpoint) (string, error) {
	base = strings.TrimRight(base, "/")
	path := ep.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u, err := url.Parse(base + path)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, vals := range ep.Params {
		for _, v := range vals {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// BuildTLSConfig builds a tls.Config from config.TLSConfig.
// InsecureSkipVerify alone is enough to produce a config (common for
// internal RabbitMQ Management HTTPS with a non-matching certificate).
func BuildTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled && !cfg.InsecureSkipVerify && cfg.CAFile == "" && cfg.CertFile == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // configurable for lab environments
		ServerName:         cfg.ServerName,
		MinVersion:         tls.VersionTLS12,
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to parse ca_file")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}
