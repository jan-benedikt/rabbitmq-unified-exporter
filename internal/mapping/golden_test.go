package mapping

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
)

func TestGoldenJSONFixtures(t *testing.T) {
	root := filepath.Join("..", "..", "testdata", "golden", "3.9.15")
	schema, err := LoadEmbedded("3.9.15")
	if err != nil {
		t.Fatal(err)
	}

	var overview mgmt.Overview
	mustJSON(t, filepath.Join(root, "overview.json"), &overview)
	var nodes []mgmt.NodeInfo
	mustJSON(t, filepath.Join(root, "nodes.json"), &nodes)
	var queues []mgmt.Queue
	mustJSON(t, filepath.Join(root, "queues.json"), &queues)

	var self *mgmt.NodeInfo
	for i := range nodes {
		if nodes[i].Name == overview.Node {
			self = &nodes[i]
			break
		}
	}
	res := Apply(schema, Input{
		SelfNode: overview.Node,
		Overview: &overview,
		Nodes:    nodes,
		Self:     self,
		Queues:   queues,
	})

	got := map[string]float64{}
	for _, f := range res.Families {
		for _, s := range f.Samples {
			key := f.Name
			for _, lp := range s.Labels {
				if lp.Name == "queue" {
					key = f.Name + "|" + lp.Value
				}
			}
			got[key] = s.Value
		}
	}

	if got["rabbitmq_queue_messages_ready|q1"] != 5 {
		t.Fatalf("q1 ready=%v", got)
	}
	if _, ok := got["rabbitmq_queue_messages_ready|other-node-q"]; ok {
		t.Fatal("other-node queue must be filtered")
	}
	if _, ok := got["rabbitmq_queue_messages_ready|null-ready"]; ok {
		t.Fatal("null field must not emit sample")
	}
	if got["rabbitmq_process_resident_memory_bytes"] != 104857600 {
		t.Fatalf("mem=%v", got["rabbitmq_process_resident_memory_bytes"])
	}
	if got["rabbitmq_disk_space_available_bytes"] != 50000000000 {
		t.Fatalf("disk=%v", got)
	}
}

func mustJSON(t *testing.T, path string, dest interface{}) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dest); err != nil {
		t.Fatal(err)
	}
}
