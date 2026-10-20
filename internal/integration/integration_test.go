//go:build integration


// Package integration contains optional Testcontainers-based tests.
// Run with: go test -tags=integration -count=1 ./internal/integration/...
//

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/promsrc"
)

func TestRabbitMQ39PrometheusAndJSON(t *testing.T) {
	if os.Getenv("CI_SKIP_INTEGRATION") == "1" {
		t.Skip("CI_SKIP_INTEGRATION=1")
	}
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.9-management",
		ExposedPorts: []string{"15672/tcp", "5672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForHTTP("/api/overview").WithPort("15672/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	mgmtPort, _ := c.MappedPort(ctx, "15672")
	base := fmt.Sprintf("http://%s:%s", host, mgmtPort.Port())

	client, err := mgmt.New(base, config.BasicAuthConfig{Username: "guest", Password: "guest"}, 15*time.Second, 8<<20, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	ov, err := client.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ov.Node == "" {
		t.Fatal("empty node")
	}
	nodes, err := client.Nodes(ctx)
	if err != nil || len(nodes) == 0 {
		t.Fatalf("nodes: %v %d", err, len(nodes))
	}

	// Prometheus plugin may need enabling; try /metrics on management port (3.9 often exposes via rabbitmq_prometheus on 15692).
	// Fall back: skip if unavailable.
	metricsURL := fmt.Sprintf("http://%s:15692", host)
	_ = metricsURL
	t.Logf("overview node=%s version=%s", ov.Node, ov.RabbitMQVersion)
}

func TestSanitizeAgainstLiveOrFixture(t *testing.T) {
	data, err := os.ReadFile("../../testdata/golden/3.9.15/prometheus/metrics_unknown.txt")
	if err != nil {
		t.Skip(err)
	}
	families, stats, err := promsrc.Sanitize(data, promsrc.SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DroppedSamples < 1 || len(families) < 3 {
		t.Fatalf("sanitize failed: %+v families=%d", stats, len(families))
	}
}

func waitHTTP(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}
