package promsrc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
)

func TestCollectPartialSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("# TYPE rabbitmq_build_info untyped\nrabbitmq_build_info{rabbitmq_version=\"3.9\"} 1\n"))
	})
	mux.HandleFunc("/metrics/detailed", func(w http.ResponseWriter, r *http.Request) {
		families := r.URL.Query()["family"]
		for _, f := range families {
			if f == "ra_metrics" {
				w.Header().Set("Content-Type", "text/plain")
				// Native detailed endpoint uses rabbitmq_detailed_ + modern raft names.
				_, _ = w.Write([]byte("# TYPE rabbitmq_detailed_raft_commit_latency_seconds gauge\nrabbitmq_detailed_raft_commit_latency_seconds{queue=\"qq\",vhost=\"/\"} 0.01\n"))
				return
			}
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(5*time.Second, 1<<20, config.SanitizeConfig{Enabled: true}, nil)
	res, err := client.Collect(context.Background(), srv.URL, []config.PrometheusEndpoint{
		{Path: "/metrics"},
		{Path: "/metrics/detailed", Params: map[string][]string{"family": {"queue_metrics"}}},
		{Path: "/metrics/detailed", Params: map[string][]string{"family": {"ra_metrics"}}},
	}, config.BasicAuthConfig{})
	if err != nil {
		t.Fatalf("expected partial success, got err: %v", err)
	}
	if len(res.Errors) != 1 {
		t.Fatalf("expected 1 endpoint error, got %#v", res.Errors)
	}
	names := map[string]bool{}
	for _, f := range res.Families {
		names[f.Name] = true
	}
	if !names["rabbitmq_build_info"] {
		t.Fatal("missing build_info from /metrics")
	}
	if !names["rabbitmq_raft_commit_latency_seconds"] {
		t.Fatal("missing classic raft name after detailed_ strip")
	}
	if !names["rabbitmq_raft_entry_commit_latency_seconds"] {
		t.Fatal("missing legacy raft alias for dashboards")
	}
	if names["rabbitmq_detailed_raft_commit_latency_seconds"] {
		t.Fatal("detailed_ prefix must not remain on exporter /metrics")
	}
}

func TestCollectAllFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer srv.Close()

	client := NewClient(2*time.Second, 1<<20, config.SanitizeConfig{Enabled: true}, nil)
	_, err := client.Collect(context.Background(), srv.URL, []config.PrometheusEndpoint{
		{Path: "/metrics"},
	}, config.BasicAuthConfig{})
	if err == nil || !strings.Contains(err.Error(), "all prometheus endpoints failed") {
		t.Fatalf("expected all-failed error, got %v", err)
	}
}

func TestBuildTLSConfigInsecureSkipVerify(t *testing.T) {
	cfg, err := BuildTLSConfig(config.TLSConfig{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil || !cfg.InsecureSkipVerify {
		t.Fatalf("expected insecure tls config, got %#v", cfg)
	}
	none, err := BuildTLSConfig(config.TLSConfig{})
	if err != nil || none != nil {
		t.Fatalf("expected nil for empty tls config, got %#v err=%v", none, err)
	}
}
