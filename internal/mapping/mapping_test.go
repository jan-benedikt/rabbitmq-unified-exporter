package mapping

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
)

func ptr(f float64) *float64 { return &f }

func TestApplyQueueAndNodeMetrics(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("mappings_embed", "rabbitmq-3.9.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := LoadSchema(data)
	if err != nil {
		t.Fatal(err)
	}

	self := "rabbit@n1"
	in := Input{
		SelfNode: self,
		Self: &mgmt.NodeInfo{
			Name: self, Running: true,
			MemUsed: 100, MemLimit: 200, DiskFree: 300, DiskFreeLimit: 50,
			FDUsed: 10, FDTotal: 100, MemAlarm: true, DiskFreeAlarm: false, Uptime: 5000,
		},
		Queues: []mgmt.Queue{
			{Name: "q1", VHost: "/", Node: self, MessagesReady: ptr(5), Messages: ptr(7), MessagesUnacknowledged: ptr(2), Consumers: ptr(1), Memory: ptr(1024), MessageBytesRAM: ptr(512)},
			{Name: "q2", VHost: "/", Node: "rabbit@other", MessagesReady: ptr(99)}, // filtered out
		},
	}
	res := Apply(schema, in)
	byName := map[string]float64{}
	for _, f := range res.Families {
		for _, s := range f.Samples {
			if f.Name == "rabbitmq_queue_messages_ready" {
				for _, lp := range s.Labels {
					if lp.Name == "queue" && lp.Value == "q2" {
						t.Fatal("q2 should be filtered by node_filter=self")
					}
				}
			}
			if len(s.Labels) == 0 {
				byName[f.Name] = s.Value
			}
			if f.Name == "rabbitmq_queue_messages_ready" {
				byName[f.Name] = s.Value
			}
		}
	}
	if byName["rabbitmq_queue_messages_ready"] != 5 {
		t.Fatalf("ready=%v", byName)
	}
	if byName["rabbitmq_process_resident_memory_bytes"] != 100 {
		t.Fatalf("mem=%v", byName)
	}
	if byName["rabbitmq_alarms_memory_used_watermark"] != 1 {
		t.Fatalf("alarm=%v", byName)
	}
	if byName["rabbitmq_erlang_uptime_seconds"] != 5 {
		t.Fatalf("uptime should be ms_to_seconds: %v", byName["rabbitmq_erlang_uptime_seconds"])
	}

	foundUnsupported := false
	for _, u := range res.Unsupported {
		if u == "rabbitmq_raft_term_total" {
			foundUnsupported = true
		}
	}
	if !foundUnsupported {
		t.Fatalf("expected raft unsupported, got %v", res.Unsupported)
	}
}

func TestNullFieldNotEmittedAsZero(t *testing.T) {
	schema, err := LoadEmbedded("3.9.15")
	if err != nil {
		t.Fatal(err)
	}
	self := "rabbit@n1"
	in := Input{
		SelfNode: self,
		Self:     &mgmt.NodeInfo{Name: self, MemUsed: 1, MemLimit: 2, DiskFree: 3, DiskFreeLimit: 4, FDUsed: 5, FDTotal: 6},
		Queues: []mgmt.Queue{
			{Name: "q", VHost: "/", Node: self, MessagesReady: nil},
		},
	}
	res := Apply(schema, in)
	for _, f := range res.Families {
		if f.Name == "rabbitmq_queue_messages_ready" && len(f.Samples) > 0 {
			t.Fatal("must not emit sample for null field")
		}
	}
}

func TestLoadEmbeddedVersions(t *testing.T) {
	for _, v := range []string{"3.9.15", "3.13.0", "4.0.0"} {
		s, err := LoadEmbedded(v)
		if err != nil {
			t.Fatalf("%s: %v", v, err)
		}
		if len(s.Metrics) == 0 {
			t.Fatalf("%s empty", v)
		}
	}
}
