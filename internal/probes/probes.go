// Package probes implements AMQP TCP, auth, TLS cert and delivery tests.
package probes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/promsrc"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/safeerr"
)

const connectionName = "rabbitmq-unified-exporter"

// Result holds probe metric families and outcomes.
type Result struct {
	Families []model.Family
	TCPOK    bool
	AuthOK   *bool
	Delivery *bool
}

type probeCacheEntry struct {
	at       time.Time
	families []model.Family
	tcpOK    bool
	authOK   *bool
	delivery *bool
}

// Runner executes probes for a target.
type Runner struct {
	deliverySem *semaphore
	mu          sync.Mutex
	cache       map[string]probeCacheEntry // key: targetID|probeName
}

// NewRunner creates a probe runner.
// maxConcurrentDelivery is the global ceiling; 0 defaults to 32 (per-target
// delivery_test.max_concurrent_tests further limits concurrency at Run time).
func NewRunner(maxConcurrentDelivery int) *Runner {
	if maxConcurrentDelivery <= 0 {
		maxConcurrentDelivery = 32
	}
	return &Runner{
		deliverySem: newSemaphore(maxConcurrentDelivery),
		cache:       map[string]probeCacheEntry{},
	}
}

func (r *Runner) cacheGet(key string, interval time.Duration) (probeCacheEntry, bool) {
	if interval <= 0 {
		return probeCacheEntry{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cache[key]
	if !ok || time.Since(e.at) >= interval {
		return probeCacheEntry{}, false
	}
	return e, true
}

func (r *Runner) cacheSet(key string, e probeCacheEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e.at = time.Now()
	r.cache[key] = e
}

// Run executes enabled probes according to target configuration.
// When a probe's interval is set and the last run is still fresh, cached metrics are reused.
func (r *Runner) Run(ctx context.Context, t config.ResolvedTarget) Result {
	res := Result{}
	labels := baseLabels(t)

	if t.Probes.AMQPTCP.Enabled {
		interval := t.Probes.AMQPTCP.Interval.Duration()
		ck := t.ID + "|amqp_tcp"
		if cached, ok := r.cacheGet(ck, interval); ok {
			res.Families = append(res.Families, cached.families...)
			res.TCPOK = cached.tcpOK
		} else {
			timeout := t.Probes.AMQPTCP.Timeout.Duration()
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			ok, dur := TCPCheck(ctx, t.AMQPAddress, timeout)
			res.TCPOK = ok
			v := 0.0
			if ok {
				v = 1
			}
			fams := []model.Family{
				gauge("rabbitmq_amqp_tcp_success", "1 if AMQP TCP connect succeeded", labels, v),
				gauge("rabbitmq_amqp_tcp_duration_seconds", "AMQP TCP connect duration", labels, dur.Seconds()),
			}
			if t.Probes.EmitProbeSuccessAlias {
				aliasLabels := append(append([]model.LabelPair{}, labels...),
					model.LabelPair{Name: "service", Value: "rabbitmq"},
					model.LabelPair{Name: "probe", Value: "amqp_tcp"},
					model.LabelPair{Name: "namespace", Value: "rabbitmq-unified"},
				)
				if ep := t.AMQPAddress; ep != "" {
					aliasLabels = append(aliasLabels, model.LabelPair{Name: "endpoint", Value: ep})
				}
				fams = append(fams, gauge("probe_success", "Compatibility alias for blackbox-style AMQP TCP probe (exporter-owned)", aliasLabels, v))
			}
			res.Families = append(res.Families, fams...)
			r.cacheSet(ck, probeCacheEntry{families: fams, tcpOK: ok})
		}
	}

	if t.Probes.AMQPAuth.Enabled {
		interval := t.Probes.AMQPAuth.Interval.Duration()
		ck := t.ID + "|amqp_auth"
		if cached, ok := r.cacheGet(ck, interval); ok {
			res.Families = append(res.Families, cached.families...)
			res.AuthOK = cached.authOK
		} else {
			timeout := t.Probes.AMQPAuth.Timeout.Duration()
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			vhosts := t.Probes.AMQPAuth.VHosts
			if len(vhosts) == 0 {
				vhosts = []string{"/"}
			}
			auth := t.Probes.AMQPAuth.Credentials
			allOK := true
			var lastDur time.Duration
			var fams []model.Family
			for _, vh := range vhosts {
				ok, dur, err := AMQPAuthCheck(ctx, t.AMQPAddress, vh, auth, t.Probes.AMQPAuth.TLS, timeout)
				lastDur = dur
				if err != nil || !ok {
					allOK = false
				}
				l := append(append([]model.LabelPair{}, labels...), model.LabelPair{Name: "vhost", Value: vh})
				vv := 0.0
				if ok {
					vv = 1
				}
				fams = append(fams,
					gauge("rabbitmq_vhost_amqp_connect_success", "1 if AMQP auth connect to vhost succeeded", l, vv),
				)
			}
			res.AuthOK = &allOK
			av := 0.0
			if allOK {
				av = 1
			}
			fams = append(fams,
				gauge("rabbitmq_amqp_connection_success", "1 if AMQP handshake+auth succeeded", labels, av),
				gauge("rabbitmq_amqp_connection_duration_seconds", "AMQP auth connect duration", labels, lastDur.Seconds()),
			)
			res.Families = append(res.Families, fams...)
			r.cacheSet(ck, probeCacheEntry{families: fams, authOK: &allOK})
		}
	}

	if t.Probes.DeliveryTest.Enabled {
		interval := t.Probes.DeliveryTest.Interval.Duration()
		ck := t.ID + "|delivery"
		if cached, ok := r.cacheGet(ck, interval); ok {
			res.Families = append(res.Families, cached.families...)
			res.Delivery = cached.delivery
		} else {
			timeout := t.Probes.DeliveryTest.Timeout.Duration()
			if timeout == 0 {
				timeout = 15 * time.Second
			}
			limit := t.Probes.DeliveryTest.MaxConcurrentTests
			if limit <= 0 {
				limit = 2
			}
			if !r.deliverySem.tryAcquireN(limit) {
				// Do not report success when skipped due to concurrency.
				return res
			}
			ok, dur, err := DeliveryTest(ctx, t, timeout)
			r.deliverySem.release()
			if err != nil {
				ok = false
			}
			res.Delivery = &ok
			v := 0.0
			if ok {
				v = 1
			}
			l := append(append([]model.LabelPair{}, labels...),
				model.LabelPair{Name: "vhost", Value: t.Probes.DeliveryTest.VHost},
				model.LabelPair{Name: "endpoint", Value: t.AMQPAddress},
			)
			fams := []model.Family{
				gauge("rabbitmq_message_delivery_test_success", "1 if end-to-end AMQP delivery test succeeded", l, v),
				gauge("rabbitmq_message_delivery_test_duration_seconds", "Delivery test round-trip duration", l, dur.Seconds()),
			}
			res.Families = append(res.Families, fams...)
			r.cacheSet(ck, probeCacheEntry{families: fams, delivery: &ok})
		}
	}

	// TLS certificate expiry for management / metrics URLs (always fresh; cheap + time-sensitive).
	for _, ep := range []struct {
		name string
		raw  string
	}{
		{"management", t.ManagementURL},
		{"metrics", t.MetricsURL},
	} {
		if ep.raw == "" || !hasTLS(ep.raw) {
			continue
		}
		exp, err := TLSCertExpiry(ctx, ep.raw, 5*time.Second)
		if err != nil {
			continue
		}
		l := append(append([]model.LabelPair{}, labels...), model.LabelPair{Name: "endpoint", Value: ep.name})
		res.Families = append(res.Families,
			gauge("rabbitmq_endpoint_tls_cert_expiry_timestamp_seconds", "TLS certificate expiry unix timestamp", l, float64(exp.Unix())),
		)
	}

	return res
}

// TCPCheck dials the AMQP address.
func TCPCheck(ctx context.Context, address string, timeout time.Duration) (bool, time.Duration) {
	start := time.Now()
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", address)
	dur := time.Since(start)
	if err != nil {
		return false, dur
	}
	_ = conn.Close()
	return true, dur
}

// AMQPAuthCheck performs AMQP handshake and authentication.
func AMQPAuthCheck(ctx context.Context, address, vhost string, auth config.BasicAuthConfig, tlsCfg config.TLSConfig, timeout time.Duration) (bool, time.Duration, error) {
	start := time.Now()
	user, pass, err := auth.Resolve()
	if err != nil {
		return false, time.Since(start), err
	}
	uri := buildAMQPURI(address, vhost, user, pass, tlsCfg)
	cfg := amqp.Config{
		Dial:            amqp.DefaultDial(timeout),
		Properties:      amqp.Table{"connection_name": connectionName},
		Heartbeat:       10 * time.Second,
		Locale:          "en_US",
	}
	if tlsClient, err := promsrc.BuildTLSConfig(tlsCfg); err != nil {
		return false, time.Since(start), err
	} else if tlsClient != nil {
		cfg.TLSClientConfig = tlsClient
	}
	conn, err := amqp.DialConfig(uri, cfg)
	dur := time.Since(start)
	if err != nil {
		return false, dur, fmt.Errorf("%s", safeerr.Err(err))
	}
	_ = conn.Close()
	return true, dur, nil
}

// DeliveryTest publishes and consumes a message on an exclusive auto-delete queue.
func DeliveryTest(ctx context.Context, t config.ResolvedTarget, timeout time.Duration) (bool, time.Duration, error) {
	start := time.Now()
	auth := t.Probes.DeliveryTest.Credentials
	user, pass, err := auth.Resolve()
	if err != nil {
		return false, 0, err
	}
	vhost := t.Probes.DeliveryTest.VHost
	if vhost == "" {
		vhost = "monitoring"
	}
	uri := buildAMQPURI(t.AMQPAddress, vhost, user, pass, t.Probes.DeliveryTest.TLS)
	cfg := amqp.Config{
		Dial:       amqp.DefaultDial(timeout),
		Properties: amqp.Table{"connection_name": connectionName},
		Heartbeat:  10 * time.Second,
	}
	if tlsClient, err := promsrc.BuildTLSConfig(t.Probes.DeliveryTest.TLS); err != nil {
		return false, time.Since(start), err
	} else if tlsClient != nil {
		cfg.TLSClientConfig = tlsClient
	}
	conn, err := amqp.DialConfig(uri, cfg)
	if err != nil {
		return false, time.Since(start), fmt.Errorf("%s", safeerr.Err(err))
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return false, time.Since(start), err
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return false, time.Since(start), err
	}

	q, err := ch.QueueDeclare(
		"",    // server-named
		false, // durable
		true,  // auto-delete
		true,  // exclusive
		false,
		amqp.Table{
			"x-expires":     int32(60000),
			"x-message-ttl": int32(30000),
		},
	)
	if err != nil {
		return false, time.Since(start), err
	}

	corrID := fmt.Sprintf("rmq-ue-%d", time.Now().UnixNano())
	body := []byte(corrID)
	pub := amqp.Publishing{
		ContentType:   "text/plain",
		Body:          body,
		DeliveryMode:  amqp.Transient,
		CorrelationId: corrID,
		Expiration:    "30000",
	}
	if err := ch.PublishWithContext(ctx, "", q.Name, true, false, pub); err != nil {
		return false, time.Since(start), err
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false, time.Since(start), ctx.Err()
		default:
		}
		msg, ok, err := ch.Get(q.Name, false)
		if err != nil {
			return false, time.Since(start), err
		}
		if !ok {
			select {
			case <-ctx.Done():
				return false, time.Since(start), ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		if string(msg.Body) != corrID && msg.CorrelationId != corrID {
			_ = msg.Nack(false, false)
			return false, time.Since(start), fmt.Errorf("correlation mismatch")
		}
		if err := msg.Ack(false); err != nil {
			return false, time.Since(start), err
		}
		_, _ = ch.QueueDelete(q.Name, false, false, false)
		return true, time.Since(start), nil
	}
	return false, time.Since(start), fmt.Errorf("delivery test timeout")
}

// TLSCertExpiry returns the earliest not-after of the peer certificate chain.
func TLSCertExpiry(ctx context.Context, rawURL string, timeout time.Duration) (time.Time, error) {
	hostPort := hostPortFromURL(rawURL)
	if hostPort == "" {
		return time.Time{}, fmt.Errorf("invalid url")
	}
	dialer := &net.Dialer{Timeout: timeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", hostPort)
	if err != nil {
		return time.Time{}, err
	}
	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // we only inspect cert expiry
		MinVersion:         tls.VersionTLS12,
	})
	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
	} else {
		_ = tlsConn.SetDeadline(time.Now().Add(timeout))
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return time.Time{}, err
	}
	defer tlsConn.Close()
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return time.Time{}, fmt.Errorf("no peer certificates")
	}
	exp := state.PeerCertificates[0].NotAfter
	for _, c := range state.PeerCertificates {
		if c.NotAfter.Before(exp) {
			exp = c.NotAfter
		}
	}
	return exp, nil
}

func buildAMQPURI(address, vhost, user, pass string, tlsCfg config.TLSConfig) string {
	scheme := "amqp"
	if tlsCfg.Enabled {
		scheme = "amqps"
	}
	if vhost == "" {
		vhost = "/"
	}
	return fmt.Sprintf("%s://%s:%s@%s/%s", scheme, urlQueryEscape(user), urlQueryEscape(pass), address, urlPathEscape(vhost))
}

func urlQueryEscape(s string) string {
	r := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			r = append(r, c)
		} else {
			r = append(r, '%')
			r = append(r, "0123456789ABCDEF"[c>>4])
			r = append(r, "0123456789ABCDEF"[c&15])
		}
	}
	return string(r)
}

func urlPathEscape(s string) string {
	if s == "/" {
		return "%2F"
	}
	return urlQueryEscape(s)
}

func hasTLS(raw string) bool {
	return len(raw) >= 8 && (raw[:8] == "https://" || (len(raw) >= 6 && raw[:6] == "amqps:"))
}

func hostPortFromURL(raw string) string {
	i := 0
	if idx := indexOf(raw, "://"); idx >= 0 {
		i = idx + 3
	}
	rest := raw[i:]
	if end := indexOf(rest, "/"); end >= 0 {
		rest = rest[:end]
	}
	if at := indexOf(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	return rest
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func baseLabels(t config.ResolvedTarget) []model.LabelPair {
	keys := []string{"job", "instance", "environment", "domain", "platform", "projectid", "cluster"}
	out := make([]model.LabelPair, 0, len(keys))
	for _, k := range keys {
		if v, ok := t.Labels[k]; ok && v != "" {
			out = append(out, model.LabelPair{Name: k, Value: v})
		}
	}
	return out
}

func gauge(name, help string, labels []model.LabelPair, value float64) model.Family {
	return model.Family{
		Name: name, Help: help, Type: model.TypeGauge,
		Samples: []model.Sample{{Labels: labels, Value: value}},
	}
}

type semaphore struct {
	ch chan struct{}
	mu sync.Mutex
}

func newSemaphore(n int) *semaphore {
	return &semaphore{ch: make(chan struct{}, n)}
}

// tryAcquireN acquires if current in-flight is below limit and below channel capacity.
func (s *semaphore) tryAcquireN(limit int) bool {
	if limit <= 0 {
		limit = 2
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ch) >= limit {
		return false
	}
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *semaphore) release() {
	select {
	case <-s.ch:
	default:
	}
}
