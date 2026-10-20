package config

import (
	"strings"
	"testing"
	"time"
)

const sampleYAML = `
server:
  listen_address: ":9419"
  metrics_path: /metrics
defaults:
  scrape_interval: 15s
  scrape_timeout: 10s
  max_concurrency: 10
  labels:
    prometheus: ""
  sanitize:
    enabled: true
    invalid_values: [unknown, undefined, null]
    strategy: drop_sample
groups:
  - name: prod
    labels:
      job: rabbitmq-prod-metrics
      environment: prod
      cluster: rabbitmq-prod
    expected_cluster_nodes: 3
    metrics_source: prometheus
    management:
      enabled: true
    prometheus:
      endpoints:
        - path: /metrics
        - path: /metrics/detailed
          params:
            family:
              - queue_coarse_metrics
    targets:
      - name: rmq-prod-01
        metrics_url: http://rmq-prod-01:15692
        management_url: http://rmq-prod-01:15672
        amqp_address: rmq-prod-01:5672
      - name: rmq-prod-02
        metrics_url: http://rmq-prod-02:15692
        management_url: http://rmq-prod-02:15672
        amqp_address: rmq-prod-02:5672
  - name: staging
    labels:
      job: rabbitmq-staging-metrics
      environment: staging
      cluster: rabbitmq-staging
    expected_cluster_nodes: 2
    metrics_source: json
    targets:
      - name: rmq-staging-01
        management_url: http://rmq-staging-01:15672
        amqp_address: rmq-staging-01:5672
        labels:
          purpose: application-messaging
      - name: rmq-staging-02
        management_url: http://rmq-staging-02:15672
        amqp_address: rmq-staging-02:5672
`

func TestParseFiveTargets(t *testing.T) {
	cfg, targets, err := Parse([]byte(sampleYAML))
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 4 {
		t.Fatalf("got %d targets", len(targets))
	}
	if cfg.Server.ListenAddress != ":9419" {
		t.Fatal(cfg.Server.ListenAddress)
	}
	var found bool
	for _, tg := range targets {
		if tg.Name == "rmq-staging-01" {
			found = true
			if tg.Labels["purpose"] != "application-messaging" {
				t.Fatalf("labels=%v", tg.Labels)
			}
			if tg.Labels["job"] != "rabbitmq-staging-metrics" {
				t.Fatalf("job=%s", tg.Labels["job"])
			}
			if tg.Labels["instance"] != "rmq-staging-01" {
				t.Fatalf("instance=%s", tg.Labels["instance"])
			}
			if tg.MetricsSource != "json" {
				t.Fatalf("source=%s", tg.MetricsSource)
			}
			if !tg.Management.Enabled {
				t.Fatal("management should be enabled in json mode")
			}
		}
		if tg.Name == "rmq-prod-01" {
			if tg.ScrapeInterval != 15*time.Second {
				t.Fatal(tg.ScrapeInterval)
			}
			if tg.MaxStaleness != 30*time.Second {
				t.Fatal(tg.MaxStaleness)
			}
		}
	}
	if !found {
		t.Fatal("staging target missing")
	}
}

func TestDuplicateTargetID(t *testing.T) {
	y := `
groups:
  - name: g
    metrics_source: json
    targets:
      - name: a
        management_url: http://a:15672
      - name: a
        management_url: http://b:15672
`
	_, _, err := Parse([]byte(y))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIntrinsicLabelRejected(t *testing.T) {
	y := `
groups:
  - name: g
    metrics_source: json
    targets:
      - name: a
        management_url: http://a:15672
        labels:
          vhost: "/"
`
	_, _, err := Parse([]byte(y))
	if err == nil {
		t.Fatal("expected intrinsic label error")
	}
}

func TestDetailedRequiresFamily(t *testing.T) {
	y := `
groups:
  - name: g
    metrics_source: prometheus
    prometheus:
      endpoints:
        - path: /metrics/detailed
    targets:
      - name: a
        metrics_url: http://a:15692
`
	_, _, err := Parse([]byte(y))
	if err == nil {
		t.Fatal("expected family required")
	}
}

func TestIdentityCollision(t *testing.T) {
	y := `
groups:
  - name: g1
    labels:
      job: j
      cluster: c
    metrics_source: json
    targets:
      - name: a
        management_url: http://a:15672
  - name: g2
    labels:
      job: j
      cluster: c
    metrics_source: json
    targets:
      - name: a
        management_url: http://b:15672
`
	_, _, err := Parse([]byte(y))
	if err == nil {
		t.Fatal("expected identity collision")
	}
}

func TestSharding(t *testing.T) {
	y := sampleYAML + `
sharding:
  enabled: true
  shard_index: 0
  shard_count: 2
`
	_, targets, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) == 0 || len(targets) >= 4 {
		t.Fatalf("expected subset, got %d", len(targets))
	}
}

func TestUnknownYAMLFieldRejected(t *testing.T) {
	y := sampleYAML + `
not_a_real_key: true
`
	_, _, err := Parse([]byte(y))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestClusterObserverDefaultsToFirstName(t *testing.T) {
	y := `
server:
  listen_address: ":9419"
defaults:
  scrape_interval: 15s
  scrape_timeout: 10s
groups:
  - name: g
    metrics_source: prometheus
    management:
      enabled: true
      collectors: [overview, queues]
    targets:
      - name: zed
        metrics_url: http://z:15692
        management_url: http://z:15672
      - name: alpha
        metrics_url: http://a:15692
        management_url: http://a:15672
`
	_, targets, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	var alpha, zed bool
	for _, tgt := range targets {
		switch tgt.Name {
		case "alpha":
			alpha = tgt.ClusterObserver
		case "zed":
			zed = tgt.ClusterObserver
		}
	}
	if !alpha || zed {
		t.Fatalf("expected alpha observer only, alpha=%v zed=%v", alpha, zed)
	}
}

func TestURLUserinfoRejected(t *testing.T) {
	y := `
server: {listen_address: ":9419"}
defaults: {scrape_interval: 15s, scrape_timeout: 10s}
groups:
  - name: g
    metrics_source: prometheus
    targets:
      - name: n1
        metrics_url: http://user:pass@broker:15692
`
	_, _, err := Parse([]byte(y))
	if err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("expected userinfo rejection, got %v", err)
	}
}
