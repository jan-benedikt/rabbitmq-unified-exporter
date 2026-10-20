// Package health evaluates cluster, quorum, vhost and application health metrics.
package health

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

// Input aggregates data needed for health evaluation.
type Input struct {
	Target           config.ResolvedTarget
	SelfNode         string
	Self             *mgmt.NodeInfo
	Overview         *mgmt.Overview
	Nodes            []mgmt.NodeInfo
	Queues           []mgmt.Queue
	VHosts           []mgmt.VHost
	FederationLinks  []mgmt.FederationLink
	Shovels          []mgmt.Shovel
	Client           *mgmt.Client
	AMQPAuthSuccess  *bool // nil = not run
	EmitAsLeaderOnly   bool
	RunHealthChecks    bool
	EmitClusterMetrics bool
}

// Evaluate produces health-related metric families.
func Evaluate(ctx context.Context, in Input) ([]model.Family, []string) {
	var families []model.Family
	var notes []string
	labels := targetLabels(in.Target)

	families = append(families, gauge("rabbitmq_expected_target", "1 if this target is configured", labels, 1))

	if in.EmitClusterMetrics && in.Target.ExpectedClusterNodes > 0 {
		families = append(families, gauge("rabbitmq_cluster_nodes", "Configured expected RabbitMQ cluster size", labels, float64(in.Target.ExpectedClusterNodes)))
	}

	if in.EmitClusterMetrics && len(in.Nodes) > 0 {
		running := 0
		partitions := 0
		versions := map[string]struct{}{}
		erlangVersions := map[string]struct{}{}
		for _, n := range in.Nodes {
			if n.Running {
				running++
			}
			partitions += len(n.Partitions)
			if n.RabbitMQVersion != "" {
				versions[n.RabbitMQVersion] = struct{}{}
			}
			if n.ErlangVersion != "" {
				erlangVersions[n.ErlangVersion] = struct{}{}
			}
		}
		families = append(families, gauge("rabbitmq_cluster_nodes_running", "Observed running cluster nodes", labels, float64(running)))
		families = append(families, gauge("rabbitmq_cluster_partitions", "Total network partition entries across nodes", labels, float64(partitions)))
		skew := 0.0
		if len(versions) > 1 || len(erlangVersions) > 1 {
			skew = 1
		}
		families = append(families, gauge("rabbitmq_cluster_version_skew", "1 if RabbitMQ or Erlang versions differ across nodes", labels, skew))
	}

	if in.EmitClusterMetrics && in.Target.ExpectedRabbitMQClusterName != "" && in.Overview != nil && in.Overview.ClusterName != "" {
		mismatch := 0.0
		if in.Overview.ClusterName != in.Target.ExpectedRabbitMQClusterName {
			mismatch = 1
		}
		families = append(families, gauge("rabbitmq_unified_exporter_cluster_identity_mismatch", "1 if broker cluster_name mismatches config", labels, mismatch))
	}

	appRunning, appKnown := evaluateApplication(ctx, in)
	if appKnown {
		families = append(families, gauge("rabbitmq_application_running", "1 if RabbitMQ application appears healthy", labels, appRunning))
	}

	if in.EmitClusterMetrics && len(in.VHosts) > 0 && in.SelfNode != "" {
		for _, vh := range in.VHosts {
			l := append(append([]model.LabelPair{}, labels...), model.LabelPair{Name: "vhost", Value: vh.Name})
			state, ok := vh.ClusterState[in.SelfNode]
			if !ok {
				continue
			}
			running := 0.0
			if state == "running" {
				running = 1
			}
			families = append(families, gaugeWithSamples("rabbitmq_vhost_running", "1 if vhost is running on this node", []model.Sample{{Labels: l, Value: running}}))
		}
	}

	if in.EmitClusterMetrics || len(in.Queues) > 0 {
		for _, q := range in.Queues {
			if !isQuorum(q) {
				continue
			}
			if len(q.Members) == 0 && q.Leader == "" {
				notes = append(notes, fmt.Sprintf("quorum metadata unavailable for %s/%s", q.VHost, q.Name))
				continue
			}
			if in.EmitAsLeaderOnly && in.SelfNode != "" && q.Leader != "" && q.Leader != in.SelfNode {
				continue
			}
			if in.EmitAsLeaderOnly && in.SelfNode != "" && q.Leader == "" && q.Node != "" && q.Node != in.SelfNode {
				continue
			}
			l := append(append([]model.LabelPair{}, labels...),
				model.LabelPair{Name: "vhost", Value: q.VHost},
				model.LabelPair{Name: "queue", Value: q.Name},
			)
			members := float64(len(q.Members))
			online := float64(len(q.Online))
			leaderPresent := 0.0
			if q.Leader != "" {
				leaderPresent = 1
			}
			available := 0.0
			if leaderPresent == 1 && len(q.Members) > 0 && len(q.Online) > len(q.Members)/2 {
				available = 1
			} else if leaderPresent == 1 && len(q.Members) == 0 && len(q.Online) > 0 {
				available = 1
			}
			families = append(families,
				gaugeWithSamples("rabbitmq_quorum_queue_members", "Quorum queue member count", []model.Sample{{Labels: l, Value: members}}),
				gaugeWithSamples("rabbitmq_quorum_queue_online_members", "Quorum queue online member count", []model.Sample{{Labels: l, Value: online}}),
				gaugeWithSamples("rabbitmq_quorum_queue_leader_present", "1 if quorum queue has a leader", []model.Sample{{Labels: l, Value: leaderPresent}}),
				gaugeWithSamples("rabbitmq_quorum_queue_available", "1 if quorum queue has leader and majority online", []model.Sample{{Labels: l, Value: available}}),
			)
		}
	}

	if in.EmitClusterMetrics {
		for _, link := range in.FederationLinks {
			status := strings.ToLower(link.Status)
			if status == "" {
				status = "unknown"
			}
			base := append(append([]model.LabelPair{}, labels...),
				model.LabelPair{Name: "vhost", Value: link.VHost},
				model.LabelPair{Name: "exchange", Value: link.Exchange},
				model.LabelPair{Name: "upstream", Value: link.Upstream},
				model.LabelPair{Name: "type", Value: link.Type},
				model.LabelPair{Name: "node", Value: link.Node},
			)
			withStatus := append(append([]model.LabelPair{}, base...), model.LabelPair{Name: "status", Value: status})
			running := 0.0
			if status == "running" {
				running = 1
			}
			families = append(families,
				gaugeWithSamples("rabbitmq_federation_link", "Federation link presence (1=reported)", []model.Sample{{Labels: withStatus, Value: 1}}),
				gaugeWithSamples("rabbitmq_federation_link_running", "1 if federation link status is running", []model.Sample{{Labels: base, Value: running}}),
			)
		}

		for _, s := range in.Shovels {
			state := strings.ToLower(s.State)
			if state == "" {
				state = "unknown"
			}
			base := append(append([]model.LabelPair{}, labels...),
				model.LabelPair{Name: "vhost", Value: s.VHost},
				model.LabelPair{Name: "shovel", Value: s.Name},
				model.LabelPair{Name: "type", Value: s.Type},
				model.LabelPair{Name: "node", Value: s.Node},
			)
			withState := append(append([]model.LabelPair{}, base...), model.LabelPair{Name: "state", Value: state})
			running := 0.0
			if state == "running" {
				running = 1
			}
			families = append(families,
				gaugeWithSamples("rabbitmq_shovel", "Shovel presence (1=reported)", []model.Sample{{Labels: withState, Value: 1}}),
				gaugeWithSamples("rabbitmq_shovel_running", "1 if shovel state is running", []model.Sample{{Labels: base, Value: running}}),
			)
		}

		for _, q := range in.Queues {
			if q.State == "" {
				continue
			}
			l := append(append([]model.LabelPair{}, labels...),
				model.LabelPair{Name: "vhost", Value: q.VHost},
				model.LabelPair{Name: "queue", Value: q.Name},
				model.LabelPair{Name: "state", Value: q.State},
			)
			families = append(families, gaugeWithSamples("rabbitmq_queue_state", "Queue state indicator", []model.Sample{{Labels: l, Value: 1}}))
		}
	}

	if in.RunHealthChecks && in.Client != nil {
		checks := []struct {
			name string
			path string
		}{
			{"alarms", "/api/health/checks/alarms"},
			{"local-alarms", "/api/health/checks/local-alarms"},
			{"virtual-hosts", "/api/health/checks/virtual-hosts"},
			{"node-is-quorum-critical", "/api/health/checks/node-is-quorum-critical"},
			{"protocol-listener-amqp", "/api/health/checks/protocol-listener/amqp"},
		}
		type checkSample struct {
			name  string
			value float64
		}
		samples := make([]checkSample, 0, len(checks))
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, ch := range checks {
			wg.Add(1)
			go func(ch struct {
				name string
				path string
			}) {
				defer wg.Done()
				st, code, err := in.Client.HealthCheck(ctx, ch.path)
				if err != nil || code == 404 || st < 0 {
					return
				}
				mu.Lock()
				samples = append(samples, checkSample{name: ch.name, value: st})
				mu.Unlock()
			}(ch)
		}
		wg.Wait()
		for _, s := range samples {
			l := append(append([]model.LabelPair{}, labels...), model.LabelPair{Name: "check", Value: s.name})
			families = append(families, gaugeWithSamples("rabbitmq_health_check_status", "Management health check status (1=ok,0=fail)", []model.Sample{{Labels: l, Value: s.value}}))
		}
	}

	return families, notes
}

func evaluateApplication(ctx context.Context, in Input) (value float64, known bool) {
	_ = ctx
	if in.Self == nil || !in.Self.Running {
		if in.Self != nil {
			return 0, true
		}
		return 0, false
	}
	listenerOK := true
	if in.Overview != nil {
		listenerOK = hasAMQPListener(in.Overview, in.SelfNode)
		known = true
	} else {
		known = true
	}
	if !listenerOK {
		return 0, true
	}
	if in.AMQPAuthSuccess != nil && !*in.AMQPAuthSuccess {
		return 0, true
	}
	return 1, true
}

func hasAMQPListener(ov *mgmt.Overview, self string) bool {
	for _, l := range ov.Listeners {
		proto := strings.ToLower(l.Protocol)
		if (proto == "amqp" || proto == "amqp/ssl") && (self == "" || l.Node == self) {
			return true
		}
	}
	return len(ov.Listeners) == 0 // unknown listeners — don't fail solely on this
}

func isQuorum(q mgmt.Queue) bool {
	t := strings.ToLower(q.Type)
	return t == "quorum" || len(q.Members) > 0
}

func targetLabels(t config.ResolvedTarget) []model.LabelPair {
	keys := []string{"job", "instance", "environment", "domain", "platform", "projectid", "cluster"}
	out := make([]model.LabelPair, 0, len(keys))
	for _, k := range keys {
		if v, ok := t.Labels[k]; ok && v != "" {
			out = append(out, model.LabelPair{Name: k, Value: v})
		}
	}
	if _, ok := t.Labels["instance"]; !ok {
		out = append(out, model.LabelPair{Name: "instance", Value: t.Name})
	}
	return out
}

func gauge(name, help string, labels []model.LabelPair, value float64) model.Family {
	return model.Family{
		Name: name, Help: help, Type: model.TypeGauge,
		Samples: []model.Sample{{Labels: labels, Value: value}},
	}
}

func gaugeWithSamples(name, help string, samples []model.Sample) model.Family {
	return model.Family{Name: name, Help: help, Type: model.TypeGauge, Samples: samples}
}
