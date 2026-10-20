package server

import (
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/selfmetrics"
)

func TestMetricsMergesTargetsNoCollision(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{MetricsPath: "/metrics", ListenAddress: ":0"}}
	targets := []config.ResolvedTarget{
		{ID: "prd/a", Name: "a", Group: "prd", Labels: map[string]string{"instance": "a", "job": "j1", "cluster": "c1"}},
		{ID: "prd/b", Name: "b", Group: "prd", Labels: map[string]string{"instance": "b", "job": "j1", "cluster": "c1"}},
		{ID: "npd/c", Name: "c", Group: "npd", Labels: map[string]string{"instance": "c", "job": "j2", "cluster": "c2"}},
	}
	store := model.NewSnapshotStore(time.Minute)
	for _, tg := range targets {
		store.Set(&model.Snapshot{
			TargetID: tg.ID, Group: tg.Group, Success: true, CollectedAt: time.Now(),
			Families: []model.Family{{
				Name: "rabbitmq_queue_messages_ready", Type: model.TypeGauge,
				Samples: []model.Sample{{
					Labels: []model.LabelPair{
						{Name: "instance", Value: tg.Labels["instance"]},
						{Name: "job", Value: tg.Labels["job"]},
						{Name: "cluster", Value: tg.Labels["cluster"]},
						{Name: "vhost", Value: "/"},
						{Name: "queue", Value: "q"},
					},
					Value: 1,
				}},
			}},
			SeriesCount: 1,
		})
	}
	metrics := selfmetrics.New("test")
	srv := New(cfg, targets, store, nil, metrics, nil, func() bool { return true }, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatal(rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	text := string(body)
	if strings.Count(text, `rabbitmq_queue_messages_ready{`) < 3 {
		t.Fatalf("expected 3 series, body:\n%s", text)
	}
	// Group filter
	req2 := httptest.NewRequest(http.MethodGet, "/metrics/npd", nil)
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)
	body2, _ := io.ReadAll(rr2.Body)
	text2 := string(body2)
	readyLines := []string{}
	for _, line := range strings.Split(text2, "\n") {
		if strings.HasPrefix(line, "rabbitmq_queue_messages_ready{") {
			readyLines = append(readyLines, line)
		}
	}
	if len(readyLines) != 1 || !strings.Contains(readyLines[0], `instance="c"`) {
		t.Fatalf("group filter failed: %v\nfull:\n%s", readyLines, text2)
	}
	for _, line := range readyLines {
		if strings.Contains(line, `instance="a"`) {
			t.Fatal("prd leaked into npd queue metrics")
		}
	}

	req3 := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr3 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr3, req3)
	if rr3.Code != 200 {
		t.Fatal(rr3.Code)
	}

	// Per-target path
	req4 := httptest.NewRequest(http.MethodGet, "/metrics/targets/prd/a", nil)
	rr4 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr4, req4)
	if rr4.Code != 200 {
		t.Fatal(rr4.Body.String())
	}
	body4 := rr4.Body.String()
	if !strings.Contains(body4, `instance="a"`) || strings.Contains(body4, `instance="b"`) {
		t.Fatalf("per-target filter failed:\n%s", body4)
	}
	if strings.Contains(body4, "rabbitmq_unified_exporter_") {
		t.Fatal("per-target scrape must not include self-metrics")
	}

	// Per-source path (empty when FamiliesBySource unset — still 200)
	req5 := httptest.NewRequest(http.MethodGet, "/metrics/targets/prd/a/prometheus", nil)
	rr5 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr5, req5)
	if rr5.Code != 200 {
		t.Fatal(rr5.Code)
	}

	req6 := httptest.NewRequest(http.MethodGet, "/metrics/self", nil)
	rr6 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr6, req6)
	if rr6.Code != 200 || !strings.Contains(rr6.Body.String(), "rabbitmq_unified_exporter_build_info") {
		t.Fatalf("self metrics: %s", rr6.Body.String())
	}

	req7 := httptest.NewRequest(http.MethodGet, "/metrics/targets/prd/a", nil)
	req7.Header.Set("Accept-Encoding", "gzip")
	rr7 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr7, req7)
	if rr7.Code != 200 {
		t.Fatal(rr7.Code)
	}
	if rr7.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected gzip, got %q", rr7.Header().Get("Content-Encoding"))
	}
	gz, err := gzip.NewReader(rr7.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(decoded), `instance="a"`) {
		t.Fatalf("gzip body missing series: %s", decoded)
	}
}
