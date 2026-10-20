package promsrc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenMetricsUnknown(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "3.9.15", "prometheus", "metrics_unknown.txt"))
	if err != nil {
		t.Fatal(err)
	}
	families, stats, err := Sanitize(data, SanitizeOptions{
		InvalidValues:     []string{"unknown", "undefined", "null"},
		MaxInvalidSamples: 10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Reasons["unknown_value"] < 1 {
		t.Fatalf("expected unknown dropped, stats=%+v", stats)
	}
	foundLimit := false
	foundReady := false
	for _, f := range families {
		if f.Name == "rabbitmq_disk_space_available_bytes" && len(f.Samples) != 0 {
			t.Fatal("unknown disk metric sample must be dropped")
		}
		if f.Name == "rabbitmq_disk_space_available_limit_bytes" && len(f.Samples) > 0 {
			foundLimit = true
		}
		if f.Name == "rabbitmq_queue_messages_ready" && len(f.Samples) > 0 {
			foundReady = true
		}
		if f.Name == "rabbitmq_build_info" && f.Type != "untyped" {
			t.Fatalf("build_info type=%s", f.Type)
		}
	}
	if !foundLimit || !foundReady {
		t.Fatalf("valid metrics lost limit=%v ready=%v kept=%d", foundLimit, foundReady, stats.KeptSamples)
	}
}

func TestGoldenDetailedEmptyFamilies(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "golden", "3.9.15", "prometheus", "detailed.txt"))
	if err != nil {
		t.Fatal(err)
	}
	families, _, err := Sanitize(data, SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(families) < 2 {
		t.Fatalf("expected build/identity families, got %d", len(families))
	}
}
