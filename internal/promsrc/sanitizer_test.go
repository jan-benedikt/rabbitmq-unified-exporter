package promsrc

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestSanitizeUnknownSingle(t *testing.T) {
	in := `# TYPE rabbitmq_disk_space_available_bytes gauge
# HELP rabbitmq_disk_space_available_bytes Disk space available in bytes
rabbitmq_disk_space_available_bytes unknown
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DroppedSamples != 1 || stats.Reasons["unknown_value"] != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if len(families) != 1 || families[0].Name != "rabbitmq_disk_space_available_bytes" {
		t.Fatalf("families=%v", families)
	}
	if len(families[0].Samples) != 0 {
		t.Fatalf("expected no samples, got %v", families[0].Samples)
	}
	if families[0].Type != "gauge" || families[0].Help == "" {
		t.Fatalf("metadata lost: %+v", families[0])
	}
}

func TestSanitizeUnknownMultiple(t *testing.T) {
	in := `# TYPE a gauge
a unknown
# TYPE b gauge
b{x="1"} unknown
# TYPE c gauge
c 1
`
	_, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DroppedSamples != 2 || stats.KeptSamples != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestSanitizeUnknownWithTimestamp(t *testing.T) {
	in := `rabbitmq_disk_space_available_bytes unknown 1609459200000
`
	_, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Reasons["unknown_value"] != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestSanitizeNaNValid(t *testing.T) {
	in := `# TYPE m gauge
m NaN
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.KeptSamples != 1 || stats.DroppedSamples != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if !math.IsNaN(families[0].Samples[0].Value) {
		t.Fatalf("expected NaN")
	}
}

func TestSanitizeUnknownInsideLabelPreserved(t *testing.T) {
	in := `m{reason="unknown"} 1
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DroppedSamples != 0 || stats.KeptSamples != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if families[0].Samples[0].Labels[0].Value != "unknown" {
		t.Fatalf("label corrupted: %+v", families[0].Samples[0])
	}
}

func TestSanitizeBadLabels(t *testing.T) {
	in := `m{foo=bar} 1
`
	_, stats, err := Sanitize([]byte(in), SanitizeOptions{})
	if err != nil {
		// structural may or may not abort entire parse depending on strategy
		_ = err
	}
	if stats.DroppedSamples < 1 && stats.StructuralError == "" && stats.Reasons["bad_labels"] == 0 {
		// parseSampleLine returns reason bad_labels with error - currently we count structural
		if stats.DroppedSamples == 0 {
			t.Fatalf("expected drop, stats=%+v", stats)
		}
	}
}

func TestSanitizeHistogramAndSummary(t *testing.T) {
	in := `# TYPE h histogram
# HELP h hist
h_bucket{le="1"} 1
h_bucket{le="+Inf"} 2
h_sum 3
h_count 2
# TYPE s summary
# HELP s empty summary
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.KeptSamples != 4 {
		t.Fatalf("stats=%+v", stats)
	}
	foundSummary := false
	for _, f := range families {
		if f.Name == "s" {
			foundSummary = true
			if f.Type != "summary" {
				t.Fatalf("type=%s", f.Type)
			}
			if len(f.Samples) != 0 {
				t.Fatalf("summary should have 0 samples")
			}
		}
	}
	if !foundSummary {
		t.Fatal("summary family missing")
	}
}

func TestSanitizeUntyped(t *testing.T) {
	in := `# TYPE rabbitmq_build_info untyped
# HELP rabbitmq_build_info info
rabbitmq_build_info{v="1"} 1
`
	families, _, err := Sanitize([]byte(in), SanitizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if families[0].Type != "untyped" {
		t.Fatalf("type=%s", families[0].Type)
	}
}

func TestSanitizeEmpty(t *testing.T) {
	families, stats, err := Sanitize([]byte(""), SanitizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 0 || stats.KeptSamples != 0 {
		t.Fatalf("expected empty")
	}
}

func TestSanitizeInfAndExponent(t *testing.T) {
	in := `a +Inf
b -Inf
c 1.5e10
d -3.2E-2
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.KeptSamples != 4 {
		t.Fatalf("stats=%+v families=%v", stats, families)
	}
}

func TestSanitizeRealUnknownFixture(t *testing.T) {
	in := `# TYPE rabbitmq_disk_space_available_bytes gauge
# HELP rabbitmq_disk_space_available_bytes Disk space available in bytes
rabbitmq_disk_space_available_bytes unknown
# TYPE rabbitmq_disk_space_available_limit_bytes gauge
rabbitmq_disk_space_available_limit_bytes 50000000000
`
	families, stats, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown", "undefined", "null"}})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DroppedSamples != 1 || stats.KeptSamples != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	var foundLimit bool
	for _, f := range families {
		if f.Name == "rabbitmq_disk_space_available_limit_bytes" && len(f.Samples) == 1 {
			foundLimit = true
		}
		if f.Name == "rabbitmq_disk_space_available_bytes" && len(f.Samples) != 0 {
			t.Fatal("unknown sample should be dropped")
		}
	}
	if !foundLimit {
		t.Fatal("limit metric lost")
	}
}

func TestSanitizeDoesNotReplaceWithZero(t *testing.T) {
	in := `m unknown
`
	families, _, err := Sanitize([]byte(in), SanitizeOptions{InvalidValues: []string{"unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		for _, s := range f.Samples {
			if s.Value == 0 {
				t.Fatal("must not synthesize 0")
			}
		}
	}
}

func TestSanitizeLargePayload(t *testing.T) {
	var b strings.Builder
	b.WriteString("# TYPE m gauge\n")
	for i := 0; i < 5000; i++ {
		b.WriteString("m{i=\"")
		b.WriteString(strconv.Itoa(i))
		b.WriteString("\"} 1\n")
	}
	_, stats, err := Sanitize([]byte(b.String()), SanitizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.KeptSamples != 5000 {
		t.Fatalf("kept=%d", stats.KeptSamples)
	}
}
