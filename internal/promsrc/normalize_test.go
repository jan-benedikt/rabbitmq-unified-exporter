package promsrc

import (
	"testing"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

func TestNormalizeDetailedPrefixAndRaftAliases(t *testing.T) {
	in := []model.Family{
		{
			Name: "rabbitmq_detailed_raft_commit_latency_seconds",
			Type: model.TypeGauge,
			Samples: []model.Sample{{
				Labels: []model.LabelPair{{Name: "queue", Value: "qq"}, {Name: "vhost", Value: "/"}},
				Value:  0.02,
			}},
		},
		{
			Name: "rabbitmq_detailed_queue_messages",
			Type: model.TypeGauge,
			Samples: []model.Sample{{
				Labels: []model.LabelPair{{Name: "queue", Value: "federation:x"}, {Name: "vhost", Value: "/"}},
				Value:  3,
			}},
		},
		{
			Name: "rabbitmq_build_info",
			Type: model.TypeUntyped,
			Samples: []model.Sample{{Value: 1}},
		},
	}
	out := NormalizeToClassicNames(in)
	got := map[string]float64{}
	for _, f := range out {
		if len(f.Samples) > 0 {
			got[f.Name] = f.Samples[0].Value
		}
	}
	if got["rabbitmq_raft_commit_latency_seconds"] != 0.02 {
		t.Fatalf("modern classic name missing: %v", got)
	}
	if got["rabbitmq_raft_entry_commit_latency_seconds"] != 0.02 {
		t.Fatalf("legacy raft alias missing: %v", got)
	}
	if got["rabbitmq_queue_messages"] != 3 {
		t.Fatalf("detailed queue not rewritten: %v", got)
	}
	if _, ok := got["rabbitmq_detailed_queue_messages"]; ok {
		t.Fatal("detailed_ prefix should be stripped")
	}
	if got["rabbitmq_build_info"] != 1 {
		t.Fatal("classic metrics must pass through")
	}
}
