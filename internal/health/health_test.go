package health

import (
	"context"
	"testing"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
)

func TestClusterAndQuorum(t *testing.T) {
	target := config.ResolvedTarget{
		ID: "g/n1", Name: "n1", Group: "g",
		ExpectedClusterNodes:        3,
		ExpectedRabbitMQClusterName: "PROD",
		Labels:                      map[string]string{"instance": "n1", "cluster": "c", "job": "j"},
	}
	families, notes := Evaluate(context.Background(), Input{
		Target:   target,
		SelfNode: "rabbit@n1",
		Self:     &mgmt.NodeInfo{Name: "rabbit@n1", Running: true},
		Overview: &mgmt.Overview{ClusterName: "PROD", Node: "rabbit@n1", Listeners: []mgmt.Listener{{Node: "rabbit@n1", Protocol: "amqp"}}},
		Nodes: []mgmt.NodeInfo{
			{Name: "rabbit@n1", Running: true, RabbitMQVersion: "3.9.15"},
			{Name: "rabbit@n2", Running: true, RabbitMQVersion: "3.9.15"},
			{Name: "rabbit@n3", Running: false, RabbitMQVersion: "3.9.15", Partitions: []string{"rabbit@n1"}},
		},
		Queues: []mgmt.Queue{
			{Name: "qq", VHost: "/", Type: "quorum", Leader: "rabbit@n1", Members: []string{"rabbit@n1", "rabbit@n2", "rabbit@n3"}, Online: []string{"rabbit@n1", "rabbit@n2"}},
		},
		VHosts: []mgmt.VHost{
			{Name: "/", ClusterState: map[string]string{"rabbit@n1": "running"}},
		},
		EmitAsLeaderOnly:   true,
		EmitClusterMetrics: true,
	})
	_ = notes
	got := map[string]float64{}
	for _, f := range families {
		if len(f.Samples) > 0 {
			got[f.Name] = f.Samples[0].Value
		}
	}
	if got["rabbitmq_cluster_nodes"] != 3 {
		t.Fatalf("%v", got)
	}
	if got["rabbitmq_cluster_nodes_running"] != 2 {
		t.Fatalf("running=%v", got)
	}
	if got["rabbitmq_cluster_partitions"] != 1 {
		t.Fatalf("partitions=%v", got)
	}
	if got["rabbitmq_quorum_queue_available"] != 1 {
		t.Fatalf("quorum=%v", got)
	}
	if got["rabbitmq_unified_exporter_cluster_identity_mismatch"] != 0 {
		t.Fatalf("mismatch=%v", got)
	}
	if got["rabbitmq_expected_target"] != 1 {
		t.Fatal("expected_target")
	}
	if got["rabbitmq_application_running"] != 1 {
		t.Fatalf("app=%v", got)
	}
}

func TestQuorumUnknownWithoutMetadata(t *testing.T) {
	families, notes := Evaluate(context.Background(), Input{
		Target:   config.ResolvedTarget{Name: "n1", Labels: map[string]string{"instance": "n1"}},
		SelfNode: "rabbit@n1",
		Queues: []mgmt.Queue{
			{Name: "qq", VHost: "/", Type: "quorum"}, // no members/leader
		},
		EmitAsLeaderOnly:   true,
		EmitClusterMetrics: true,
	})
	for _, f := range families {
		if f.Name == "rabbitmq_quorum_queue_available" {
			t.Fatal("must not emit available without metadata")
		}
	}
	if len(notes) == 0 {
		t.Fatal("expected unsupported note")
	}
}

func TestQuorumEmitsWhenSelfNodeEmpty(t *testing.T) {
	families, _ := Evaluate(context.Background(), Input{
		Target: config.ResolvedTarget{Name: "n1", Labels: map[string]string{"instance": "n1"}},
		Queues: []mgmt.Queue{
			{Name: "qq", VHost: "/", Type: "quorum", Leader: "rabbit@n1", Members: []string{"rabbit@n1"}, Online: []string{"rabbit@n1"}},
		},
		EmitAsLeaderOnly:   true,
		EmitClusterMetrics: true,
	})
	found := false
	for _, f := range families {
		if f.Name == "rabbitmq_quorum_queue_available" {
			found = true
			if f.Samples[0].Value != 1 {
				t.Fatalf("available=%v", f.Samples[0].Value)
			}
		}
	}
	if !found {
		t.Fatal("expected quorum metrics when SelfNode is empty")
	}
}

func TestFederationAndShovel(t *testing.T) {
	families, _ := Evaluate(context.Background(), Input{
		Target:             config.ResolvedTarget{Name: "n1", Labels: map[string]string{"instance": "n1", "job": "j"}},
		EmitClusterMetrics: true,
		FederationLinks: []mgmt.FederationLink{
			{Status: "running", Node: "rabbit@n1", VHost: "/", Exchange: "fed", Upstream: "up", Type: "exchange"},
			{Status: "error", Node: "rabbit@n1", VHost: "/", Exchange: "fed2", Upstream: "up", Type: "exchange"},
		},
		Shovels: []mgmt.Shovel{
			{Name: "s1", VHost: "/", Node: "rabbit@n1", Type: "dynamic", State: "running"},
		},
	})
	got := map[string]float64{}
	for _, f := range families {
		for _, s := range f.Samples {
			key := f.Name
			for _, l := range s.Labels {
				if l.Name == "exchange" || l.Name == "shovel" {
					key += "/" + l.Value
				}
			}
			got[key] = s.Value
		}
	}
	if got["rabbitmq_federation_link_running/fed"] != 1 {
		t.Fatalf("fed running=%v", got)
	}
	if got["rabbitmq_federation_link_running/fed2"] != 0 {
		t.Fatalf("fed2 running=%v", got)
	}
	if got["rabbitmq_shovel_running/s1"] != 1 {
		t.Fatalf("shovel=%v", got)
	}
}
