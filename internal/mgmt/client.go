// Package mgmt is a RabbitMQ Management HTTP API client.
package mgmt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
)

// Client talks to the Management API.
type Client struct {
	httpClient *http.Client
	baseURL    string
	auth       config.BasicAuthConfig
	maxBytes   int64
	pageSize   int
}

// New creates a management client for one broker.
// Credentials are re-resolved from *_file on every request (secret rotation).
func New(baseURL string, auth config.BasicAuthConfig, timeout time.Duration, maxBytes int64, pageSize int, tlsCfg *tls.Config) (*Client, error) {
	if pageSize <= 0 {
		pageSize = 500
	}
	if pageSize > 500 {
		pageSize = 500
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}
	// Eagerly validate credential files are readable.
	if _, _, err := auth.Resolve(); err != nil {
		return nil, err
	}
	return &Client{
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: tr,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		baseURL:  strings.TrimRight(baseURL, "/"),
		auth:     auth,
		maxBytes: maxBytes,
		pageSize: pageSize,
	}, nil
}

// NewWithHTTPClient is like New but reuses an existing HTTP client (connection pooling).
func NewWithHTTPClient(baseURL string, auth config.BasicAuthConfig, httpClient *http.Client, maxBytes int64, pageSize int) (*Client, error) {
	if pageSize <= 0 {
		pageSize = 500
	}
	if pageSize > 500 {
		pageSize = 500
	}
	if httpClient == nil {
		return nil, fmt.Errorf("http client is required")
	}
	if _, _, err := auth.Resolve(); err != nil {
		return nil, err
	}
	return &Client{
		httpClient: httpClient,
		baseURL:    strings.TrimRight(baseURL, "/"),
		auth:       auth,
		maxBytes:   maxBytes,
		pageSize:   pageSize,
	}, nil
}

// Capabilities discovered for a broker.
type Capabilities struct {
	Version            string
	HasHealthChecks    bool
	HasQuorumMembers   bool
	HasShovels         bool
	HasFederation      bool
	HasFeatureFlags    bool
	HealthCheckPaths   []string
}

// Overview is a subset of /api/overview.
type Overview struct {
	Node            string                 `json:"node"`
	ClusterName     string                 `json:"cluster_name"`
	RabbitMQVersion string                 `json:"rabbitmq_version"`
	ErlangVersion   string                 `json:"erlang_version"`
	ObjectTotals    NumericMap             `json:"object_totals"`
	QueueTotals     NumericMap             `json:"queue_totals"`
	MessageStats    map[string]interface{} `json:"message_stats"`
	Listeners       []Listener             `json:"listeners"`
}

// NumericMap decodes RabbitMQ maps that mix scalar numbers with nested
// objects (e.g. queue_totals.messages_details.rate). Non-numeric values are skipped.
type NumericMap map[string]float64

func (m *NumericMap) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*m = nil
		return nil
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out := make(map[string]float64, len(raw))
	for k, v := range raw {
		switch n := v.(type) {
		case float64:
			out[k] = n
		case string:
			f, err := strconv.ParseFloat(n, 64)
			if err != nil {
				continue
			}
			out[k] = f
		}
	}
	*m = out
	return nil
}

// Listener is a network listener entry.
type Listener struct {
	Node     string `json:"node"`
	Protocol string `json:"protocol"`
	IP       string `json:"ip_address"`
	Port     int    `json:"port"`
}

// NodeInfo is a subset of /api/nodes/{name}.
type NodeInfo struct {
	Name              string        `json:"name"`
	Running           bool          `json:"running"`
	MemUsed           float64       `json:"mem_used"`
	MemLimit          float64       `json:"mem_limit"`
	MemAlarm          bool          `json:"mem_alarm"`
	DiskFree          float64       `json:"disk_free"`
	DiskFreeLimit     float64       `json:"disk_free_limit"`
	DiskFreeAlarm     bool          `json:"disk_free_alarm"`
	FDUsed            float64       `json:"fd_used"`
	FDTotal           float64       `json:"fd_total"`
	SocketsUsed       float64       `json:"sockets_used"`
	SocketsTotal      float64       `json:"sockets_total"`
	ProcUsed          float64       `json:"proc_used"`
	ProcTotal         float64       `json:"proc_total"`
	Uptime            float64       `json:"uptime"`
	Partitions        []string      `json:"partitions"`
	RabbitMQVersion   string        `json:"rabbitmq_version"`
	ErlangVersion     string        `json:"erlang_version"`
	Type              string        `json:"type"`
}

// Queue is a subset of /api/queues entries.
type Queue struct {
	Name                   string                 `json:"name"`
	VHost                  string                 `json:"vhost"`
	Node                   string                 `json:"node"`
	Type                   string                 `json:"type"`
	State                  string                 `json:"state"`
	Messages               *float64               `json:"messages"`
	MessagesReady          *float64               `json:"messages_ready"`
	MessagesUnacknowledged *float64               `json:"messages_unacknowledged"`
	Consumers              *float64               `json:"consumers"`
	Memory                 *float64               `json:"memory"`
	MessageBytesRAM        *float64               `json:"message_bytes_ram"`
	ConsumerUtilisation    *float64               `json:"consumer_utilisation"`
	ConsumerCapacity       *float64               `json:"consumer_capacity"`
	HeadMessageTimestamp   *float64               `json:"head_message_timestamp"`
	Members                []string               `json:"members"`
	Online                 []string               `json:"online"`
	Leader                 string                 `json:"leader"`
	SlaveNodes             []string               `json:"slave_nodes"`
	SynchronisedSlaveNodes []string               `json:"synchronised_slave_nodes"`
	MessageStats           map[string]interface{} `json:"message_stats"`
	IdleSince              string                 `json:"idle_since"`
}

// VHost is a subset of /api/vhosts entries.
type VHost struct {
	Name         string            `json:"name"`
	ClusterState map[string]string `json:"cluster_state"`
}

// FederationLink is a subset of /api/federation-links entries.
type FederationLink struct {
	Status   string `json:"status"`
	Node     string `json:"node"`
	VHost    string `json:"vhost"`
	Exchange string `json:"exchange"`
	Upstream string `json:"upstream"`
	Type     string `json:"type"`
	URI      string `json:"uri"`
	ID       string `json:"id"`
}

// Shovel is a subset of /api/shovels entries.
type Shovel struct {
	Name  string `json:"name"`
	VHost string `json:"vhost"`
	Node  string `json:"node"`
	Type  string `json:"type"`
	State string `json:"state"`
}

// Connection is a subset of /api/connections entries.
type Connection struct {
	Name  string `json:"name"`
	State string `json:"state"`
	Node  string `json:"node"`
	VHost string `json:"vhost"`
	User  string `json:"user"`
}

// GetJSON fetches a path and decodes JSON into dest.
func (c *Client) GetJSON(ctx context.Context, path string, dest interface{}) error {
	body, status, err := c.get(ctx, path)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d", path, status)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// GetStatus fetches a path and returns status code (for health checks).
func (c *Client) GetStatus(ctx context.Context, path string) (int, []byte, error) {
	body, status, err := c.get(ctx, path)
	return status, body, err
}

func (c *Client) get(ctx context.Context, path string) ([]byte, int, error) {
	u := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	user, pass, err := c.auth.Resolve()
	if err != nil {
		return nil, 0, err
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	limited := &limitedReader{r: resp.Body, max: c.maxBytes}
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

type limitedReader struct {
	r   io.Reader
	max int64
	n   int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.n >= l.max {
		return 0, fmt.Errorf("response exceeds max bytes")
	}
	maxRead := int64(len(p))
	if l.n+maxRead > l.max {
		maxRead = l.max - l.n + 1
	}
	n, err := l.r.Read(p[:maxRead])
	l.n += int64(n)
	if l.n > l.max {
		return n, fmt.Errorf("response exceeds max bytes")
	}
	return n, err
}

// Overview fetches /api/overview.
func (c *Client) Overview(ctx context.Context) (*Overview, error) {
	var o Overview
	if err := c.GetJSON(ctx, "/api/overview", &o); err != nil {
		return nil, err
	}
	return &o, nil
}

// Nodes fetches /api/nodes.
func (c *Client) Nodes(ctx context.Context) ([]NodeInfo, error) {
	var nodes []NodeInfo
	if err := c.GetJSON(ctx, "/api/nodes", &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// Node fetches /api/nodes/{name}.
func (c *Client) Node(ctx context.Context, name string) (*NodeInfo, error) {
	var n NodeInfo
	path := "/api/nodes/" + url.PathEscape(name)
	if err := c.GetJSON(ctx, path, &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// QueuesResult is a paginated queues fetch with optional truncation metadata.
type QueuesResult struct {
	Queues    []Queue
	Dropped   int
	TotalHint int
}

// Queues fetches queues with pagination. Columns include quorum membership fields.
// Unpaginated fallback requires allowUnpaginated=true.
func (c *Client) Queues(ctx context.Context, maxQueues int, allowUnpaginated bool) (QueuesResult, error) {
	const queueColumns = "name,vhost,node,type,state,messages,messages_ready,messages_unacknowledged,consumers,memory,message_bytes_ram,consumer_utilisation,consumer_capacity,head_message_timestamp,members,online,leader,slave_nodes,synchronised_slave_nodes,idle_since"
	var all []Queue
	page := 1
	for {
		path := fmt.Sprintf("/api/queues?page=%d&page_size=%d&pagination=true&columns=%s", page, c.pageSize, queueColumns)
		var resp struct {
			Items []Queue `json:"items"`
			Total int     `json:"total_count"`
			Page  int     `json:"page"`
		}
		body, status, err := c.get(ctx, path)
		if err != nil {
			return QueuesResult{}, err
		}
		if status != http.StatusOK {
			if !allowUnpaginated {
				return QueuesResult{}, fmt.Errorf("/api/queues pagination HTTP %d (set allow_unpaginated_queues to enable full dump fallback)", status)
			}
			var queues []Queue
			if err := c.GetJSON(ctx, "/api/queues", &queues); err != nil {
				return QueuesResult{}, err
			}
			return truncateQueues(queues, maxQueues, len(queues)), nil
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			// Maybe non-paginated array response.
			var queues []Queue
			if err2 := json.Unmarshal(body, &queues); err2 != nil {
				return QueuesResult{}, fmt.Errorf("decode queues: %w", err)
			}
			if !allowUnpaginated && page == 1 {
				return QueuesResult{}, fmt.Errorf("queues response is not paginated (set allow_unpaginated_queues to accept array payloads)")
			}
			return truncateQueues(queues, maxQueues, len(queues)), nil
		}
		all = append(all, resp.Items...)
		if maxQueues > 0 && len(all) >= maxQueues {
			dropped := 0
			if resp.Total > maxQueues {
				dropped = resp.Total - maxQueues
			} else if len(all) > maxQueues {
				dropped = len(all) - maxQueues
			}
			return QueuesResult{Queues: all[:maxQueues], Dropped: dropped, TotalHint: resp.Total}, nil
		}
		if len(resp.Items) == 0 || len(all) >= resp.Total {
			return QueuesResult{Queues: all, TotalHint: resp.Total}, nil
		}
		page++
		if page > 10000 {
			break
		}
	}
	return truncateQueues(all, maxQueues, len(all)), nil
}

func truncateQueues(queues []Queue, maxQueues, totalHint int) QueuesResult {
	if maxQueues > 0 && len(queues) > maxQueues {
		return QueuesResult{
			Queues:    queues[:maxQueues],
			Dropped:   len(queues) - maxQueues,
			TotalHint: totalHint,
		}
	}
	return QueuesResult{Queues: queues, TotalHint: totalHint}
}

// VHosts fetches /api/vhosts.
func (c *Client) VHosts(ctx context.Context) ([]VHost, error) {
	var v []VHost
	if err := c.GetJSON(ctx, "/api/vhosts", &v); err != nil {
		return nil, err
	}
	return v, nil
}

// Connections fetches /api/connections (opt-in, expensive).
func (c *Client) Connections(ctx context.Context) ([]Connection, error) {
	var conns []Connection
	path := fmt.Sprintf("/api/connections?page=1&page_size=%d&pagination=true&columns=name,state,node,vhost,user", c.pageSize)
	body, status, err := c.get(ctx, path)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		if err := c.GetJSON(ctx, "/api/connections", &conns); err != nil {
			return nil, err
		}
		return conns, nil
	}
	var resp struct {
		Items []Connection `json:"items"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		if err2 := json.Unmarshal(body, &conns); err2 != nil {
			return nil, err
		}
		return conns, nil
	}
	return resp.Items, nil
}

// optionalAPIUnavailable reports statuses RabbitMQ returns when an optional
// management plugin is not enabled (federation_management / shovel_management).
// RabbitMQ often answers 406 Not Acceptable instead of 404 for missing API routes.
func optionalAPIUnavailable(status int) bool {
	switch status {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotAcceptable:
		return true
	default:
		return false
	}
}

// FederationLinks fetches /api/federation-links (requires rabbitmq_federation_management).
// Returns (nil, nil) when the plugin/API is unavailable.
func (c *Client) FederationLinks(ctx context.Context) ([]FederationLink, error) {
	var links []FederationLink
	body, status, err := c.get(ctx, "/api/federation-links")
	if err != nil {
		return nil, err
	}
	if optionalAPIUnavailable(status) {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("/api/federation-links: HTTP %d", status)
	}
	if err := json.Unmarshal(body, &links); err != nil {
		return nil, fmt.Errorf("decode federation-links: %w", err)
	}
	return links, nil
}

// Shovels fetches /api/shovels (requires rabbitmq_shovel_management).
// Returns (nil, nil) when the plugin/API is unavailable.
func (c *Client) Shovels(ctx context.Context) ([]Shovel, error) {
	var shovels []Shovel
	body, status, err := c.get(ctx, "/api/shovels")
	if err != nil {
		return nil, err
	}
	if optionalAPIUnavailable(status) {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("/api/shovels: HTTP %d", status)
	}
	if err := json.Unmarshal(body, &shovels); err != nil {
		return nil, fmt.Errorf("decode shovels: %w", err)
	}
	return shovels, nil
}

// DiscoverCapabilities probes optional endpoints.
func (c *Client) DiscoverCapabilities(ctx context.Context) (*Capabilities, error) {
	ov, err := c.Overview(ctx)
	if err != nil {
		return nil, err
	}
	cap := &Capabilities{Version: ov.RabbitMQVersion}
	checks := []string{
		"/api/health/checks/alarms",
		"/api/health/checks/local-alarms",
		"/api/health/checks/virtual-hosts",
		"/api/health/checks/node-is-quorum-critical",
		"/api/health/checks/certificate-expiration/1/days",
	}
	for _, p := range checks {
		_, status, err := c.get(ctx, p)
		if err == nil && status != http.StatusNotFound {
			cap.HasHealthChecks = true
			cap.HealthCheckPaths = append(cap.HealthCheckPaths, p)
		}
	}
	_, status, _ := c.get(ctx, "/api/shovels")
	cap.HasShovels = status == http.StatusOK
	_, status, _ = c.get(ctx, "/api/federation-links")
	cap.HasFederation = status == http.StatusOK
	_, status, _ = c.get(ctx, "/api/feature-flags")
	cap.HasFeatureFlags = status == http.StatusOK
	return cap, nil
}

// HealthCheck runs one health check path; returns 1 for OK (200), 0 for failure, -1 for unsupported.
func (c *Client) HealthCheck(ctx context.Context, path string) (status float64, httpCode int, err error) {
	body, code, err := c.get(ctx, path)
	if err != nil {
		return 0, 0, err
	}
	if code == http.StatusNotFound {
		return -1, code, nil
	}
	if code == http.StatusOK {
		return 1, code, nil
	}
	_ = body
	return 0, code, nil
}

// FilterQueuesSelf returns queues whose node matches selfNode (leader/master local).
func FilterQueuesSelf(queues []Queue, selfNode string) []Queue {
	if selfNode == "" {
		return queues
	}
	out := make([]Queue, 0, len(queues))
	for _, q := range queues {
		node := q.Node
		if q.Leader != "" {
			node = q.Leader
		}
		if node == selfNode {
			out = append(out, q)
		}
	}
	return out
}

// EncodeVHost encodes a vhost for use in API paths.
func EncodeVHost(vhost string) string {
	return url.PathEscape(vhost)
}

// BoolToFloat converts bool to 1/0.
func BoolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// ParseVersionMajorMinor extracts major.minor from a version string.
func ParseVersionMajorMinor(v string) (major, minor int) {
	parts := strings.Split(v, ".")
	if len(parts) >= 1 {
		major, _ = strconv.Atoi(parts[0])
	}
	if len(parts) >= 2 {
		minor, _ = strconv.Atoi(parts[1])
	}
	return
}
