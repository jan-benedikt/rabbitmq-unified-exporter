// Package mapping converts Management API JSON into native RabbitMQ Prometheus metrics.
package mapping

import (
	"embed"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/mgmt"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

//go:embed mappings_embed/*
var embeddedFS embed.FS

// Rule describes one JSON→Prometheus mapping.
type Rule struct {
	Metric     string            `yaml:"metric"`
	Type       string            `yaml:"type"`
	Fidelity   string            `yaml:"fidelity"` // exact | approximate | unsupported
	Scope      string            `yaml:"scope"`
	NodeFilter string            `yaml:"node_filter"`
	Unit       string            `yaml:"unit"`
	Notes      string            `yaml:"notes"`
	Transform  string            `yaml:"transform"`
	Aggregate  string            `yaml:"aggregate"`
	JSON       *JSONSource       `yaml:"json"`
	Labels     map[string]string `yaml:"labels"`
}

// JSONSource locates a field in the Management API.
type JSONSource struct {
	Endpoint string `yaml:"endpoint"`
	Field    string `yaml:"field"`
}

// Schema is a versioned set of mapping rules.
type Schema struct {
	Version string `yaml:"version"`
	Metrics []Rule `yaml:"metrics"`
}

// LoadSchema loads a mapping schema from YAML bytes.
func LoadSchema(data []byte) (*Schema, error) {
	var s Schema
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// LoadEmbedded loads an embedded schema by RabbitMQ major.minor family.
func LoadEmbedded(version string) (*Schema, error) {
	major, minor := mgmt.ParseVersionMajorMinor(version)
	name := "rabbitmq-3.9.yaml"
	if major >= 4 {
		name = "rabbitmq-4.x.yaml"
	} else if major == 3 && minor >= 13 {
		name = "rabbitmq-3.13.yaml"
	} else if major == 3 && minor >= 9 {
		name = "rabbitmq-3.9.yaml"
	}
	data, err := embeddedFS.ReadFile("mappings_embed/" + name)
	if err != nil {
		// Fall back to 3.9
		data, err = embeddedFS.ReadFile("mappings_embed/rabbitmq-3.9.yaml")
		if err != nil {
			return nil, err
		}
	}
	return LoadSchema(data)
}

// LoadFromFile loads schema from filesystem path (for tests / overrides).
func LoadFromFile(path string) (*Schema, error) {
	// Implemented via os in caller; keep for API completeness using embed fallback.
	return nil, fmt.Errorf("use LoadSchema with file contents")
}

// Input is the collected Management API data for one node.
type Input struct {
	SelfNode string
	Overview *mgmt.Overview
	Nodes    []mgmt.NodeInfo
	Self     *mgmt.NodeInfo
	Queues   []mgmt.Queue
	VHosts   []mgmt.VHost
}

// Result holds mapped families and diagnostics.
type Result struct {
	Families    []model.Family
	Unsupported []string
	Errors      []string
}

// Apply converts Management API data using the schema.
func Apply(schema *Schema, in Input) Result {
	res := Result{}
	byName := map[string]*model.Family{}
	order := []string{}

	selfQueues := mgmt.FilterQueuesSelf(in.Queues, in.SelfNode)

	for _, rule := range schema.Metrics {
		if rule.Fidelity == "unsupported" || rule.JSON == nil {
			res.Unsupported = append(res.Unsupported, rule.Metric)
			continue
		}
		samples, errs := applyRule(rule, in, selfQueues)
		res.Errors = append(res.Errors, errs...)
		if len(samples) == 0 && rule.Aggregate != "sum" {
			// No samples — skip emitting empty family unless we have explicit zeros from gauges.
			continue
		}
		f, ok := byName[rule.Metric]
		if !ok {
			nf := model.Family{
				Name: rule.Metric,
				Type: model.MetricType(rule.Type),
				Unit: rule.Unit,
				Help: helpFor(rule),
			}
			byName[rule.Metric] = &nf
			order = append(order, rule.Metric)
			f = byName[rule.Metric]
		}
		f.Samples = append(f.Samples, samples...)
	}

	for _, name := range order {
		res.Families = append(res.Families, *byName[name])
	}
	return res
}

func helpFor(rule Rule) string {
	h := "Mapped from Management API"
	if rule.Fidelity == "approximate" {
		h += " (approximate)"
	}
	if rule.Notes != "" {
		h += ": " + rule.Notes
	}
	return h
}

func applyRule(rule Rule, in Input, selfQueues []mgmt.Queue) ([]model.Sample, []string) {
	var errs []string
	ep := ""
	if rule.JSON != nil {
		ep = rule.JSON.Endpoint
	}

	switch {
	case strings.Contains(ep, "/api/nodes"):
		if in.Self == nil {
			errs = append(errs, rule.Metric+": no self node")
			return nil, errs
		}
		v, ok, err := fieldFromNode(in.Self, rule.JSON.Field)
		if err != nil {
			errs = append(errs, rule.Metric+": "+err.Error())
			return nil, errs
		}
		if !ok {
			errs = append(errs, rule.Metric+": missing/null field "+rule.JSON.Field)
			return nil, errs
		}
		v = transform(v, rule.Transform)
		return []model.Sample{{Value: v}}, errs

	case strings.Contains(ep, "/api/queues"):
		if rule.Aggregate == "sum" {
			var sum float64
			any := false
			for _, q := range selfQueues {
				v, ok, err := fieldFromQueue(q, rule.JSON.Field)
				if err != nil || !ok {
					continue
				}
				sum += v
				any = true
			}
			if !any {
				return nil, errs
			}
			return []model.Sample{{Value: sum}}, errs
		}
		samples := make([]model.Sample, 0, len(selfQueues))
		for _, q := range selfQueues {
			v, ok, err := fieldFromQueue(q, rule.JSON.Field)
			if err != nil {
				errs = append(errs, rule.Metric+": "+err.Error())
				continue
			}
			if !ok {
				continue // null / missing — do not emit 0
			}
			v = transform(v, rule.Transform)
			labels := resolveLabels(rule.Labels, q)
			samples = append(samples, model.Sample{Labels: labels, Value: v})
		}
		return samples, errs

	case strings.Contains(ep, "/api/health"):
		// Handled separately by health package; skip here.
		return nil, errs
	}
	return nil, errs
}

func transform(v float64, name string) float64 {
	switch name {
	case "ms_to_seconds":
		return v / 1000.0
	case "bool_to_int":
		return v
	default:
		return v
	}
}

func resolveLabels(spec map[string]string, q mgmt.Queue) []model.LabelPair {
	if len(spec) == 0 {
		return nil
	}
	out := make([]model.LabelPair, 0, len(spec))
	for k, expr := range spec {
		val := ""
		switch expr {
		case "$.vhost":
			val = q.VHost
		case "$.name":
			val = q.Name
		case "$.node":
			val = q.Node
		case "$.type":
			val = q.Type
		default:
			val = expr
		}
		out = append(out, model.LabelPair{Name: k, Value: val})
	}
	return out
}

func fieldFromNode(n *mgmt.NodeInfo, field string) (float64, bool, error) {
	switch field {
	case "mem_used":
		return n.MemUsed, true, nil
	case "mem_limit":
		return n.MemLimit, true, nil
	case "disk_free":
		return n.DiskFree, true, nil
	case "disk_free_limit":
		return n.DiskFreeLimit, true, nil
	case "fd_used":
		return n.FDUsed, true, nil
	case "fd_total":
		return n.FDTotal, true, nil
	case "mem_alarm":
		return mgmt.BoolToFloat(n.MemAlarm), true, nil
	case "disk_free_alarm":
		return mgmt.BoolToFloat(n.DiskFreeAlarm), true, nil
	case "uptime":
		return n.Uptime, true, nil
	case "sockets_used":
		return n.SocketsUsed, true, nil
	case "sockets_total":
		return n.SocketsTotal, true, nil
	case "proc_used":
		return n.ProcUsed, true, nil
	case "proc_total":
		return n.ProcTotal, true, nil
	default:
		return 0, false, fmt.Errorf("unknown node field %s", field)
	}
}

func fieldFromQueue(q mgmt.Queue, field string) (float64, bool, error) {
	switch field {
	case "messages":
		return ptrVal(q.Messages)
	case "messages_ready":
		return ptrVal(q.MessagesReady)
	case "messages_unacknowledged":
		return ptrVal(q.MessagesUnacknowledged)
	case "consumers":
		return ptrVal(q.Consumers)
	case "memory":
		return ptrVal(q.Memory)
	case "message_bytes_ram":
		return ptrVal(q.MessageBytesRAM)
	case "consumer_utilisation", "consumer_utilization":
		return ptrVal(q.ConsumerUtilisation)
	case "consumer_capacity":
		return ptrVal(q.ConsumerCapacity)
	case "head_message_timestamp":
		return ptrVal(q.HeadMessageTimestamp)
	case "message_stats.deliver_get":
		return statsField(q.MessageStats, "deliver_get")
	case "message_stats.redeliver":
		return statsField(q.MessageStats, "redeliver")
	case "message_stats.publish":
		return statsField(q.MessageStats, "publish")
	default:
		return 0, false, fmt.Errorf("unknown queue field %s", field)
	}
}

func ptrVal(p *float64) (float64, bool, error) {
	if p == nil {
		return 0, false, nil
	}
	return *p, true, nil
}

func statsField(stats map[string]interface{}, key string) (float64, bool, error) {
	if stats == nil {
		return 0, false, nil
	}
	v, ok := stats[key]
	if !ok || v == nil {
		return 0, false, nil
	}
	switch t := v.(type) {
	case float64:
		return t, true, nil
	case int:
		return float64(t), true, nil
	case int64:
		return float64(t), true, nil
	default:
		return 0, false, fmt.Errorf("non-numeric stats field %s", key)
	}
}

// CompatibilityMatrix returns rows for documentation /capabilities.
func (s *Schema) CompatibilityMatrix() []map[string]string {
	rows := make([]map[string]string, 0, len(s.Metrics))
	for _, r := range s.Metrics {
		endpoint := ""
		field := ""
		if r.JSON != nil {
			endpoint = r.JSON.Endpoint
			field = r.JSON.Field
		}
		rows = append(rows, map[string]string{
			"metric":    r.Metric,
			"fidelity":  r.Fidelity,
			"endpoint":  endpoint,
			"field":     field,
			"notes":     r.Notes,
			"version":   s.Version,
		})
	}
	return rows
}
