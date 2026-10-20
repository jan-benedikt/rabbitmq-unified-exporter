package promsrc

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

func TestSanitizePerformanceSynthetic(t *testing.T) {
	var b strings.Builder
	b.WriteString("# TYPE rabbitmq_queue_messages_ready gauge\n")
	for i := 0; i < 20000; i++ {
		b.WriteString(`rabbitmq_queue_messages_ready{vhost="/",queue="q`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`"} 1` + "\n")
	}
	b.WriteString("rabbitmq_disk_space_available_bytes unknown\n")
	data := []byte(b.String())

	start := time.Now()
	families, stats, err := Sanitize(data, SanitizeOptions{
		InvalidValues:     []string{"unknown", "undefined", "null"},
		MaxInvalidSamples: 100000,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if stats.KeptSamples < 20000 {
		t.Fatalf("kept=%d", stats.KeptSamples)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("sanitize too slow: %s for %d samples / %d families", elapsed, stats.KeptSamples, len(families))
	}
	t.Logf("sanitized %d bytes -> kept=%d dropped=%d families=%d in %s", len(data), stats.KeptSamples, stats.DroppedSamples, len(families), elapsed)
}

func BenchmarkSanitize5kQueues(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("# TYPE rabbitmq_queue_messages_ready gauge\n")
	for i := 0; i < 5000; i++ {
		sb.WriteString(`rabbitmq_queue_messages_ready{vhost="/",queue="q`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`"} 1` + "\n")
	}
	data := []byte(sb.String())
	opts := SanitizeOptions{InvalidValues: []string{"unknown"}, MaxInvalidSamples: 100000}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := Sanitize(data, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteExposition5k(b *testing.B) {
	fams := make([]model.Family, 1)
	fams[0] = model.Family{Name: "rabbitmq_queue_messages_ready", Type: model.TypeGauge, Help: "n"}
	for i := 0; i < 5000; i++ {
		fams[0].Samples = append(fams[0].Samples, model.Sample{
			Labels: []model.LabelPair{{Name: "queue", Value: "q" + strconv.Itoa(i)}},
			Value:  1,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := model.WriteExposition(&buf, fams); err != nil {
			b.Fatal(err)
		}
	}
}
