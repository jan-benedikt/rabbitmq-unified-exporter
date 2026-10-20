package promsrc

import (
	"strings"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

const detailedPrefix = "rabbitmq_detailed_"

// Legacy raft names used by community dashboards/alerts (pre RabbitMQ 3.13/4.x renames).
// Keys are modern native names (after stripping rabbitmq_detailed_).
var raftLegacyAliases = map[string]string{
	"rabbitmq_raft_commit_latency_seconds": "rabbitmq_raft_entry_commit_latency_seconds",
	"rabbitmq_raft_commit_index":           "rabbitmq_raft_log_commit_index",
	"rabbitmq_raft_last_written_index":     "rabbitmq_raft_log_last_written_index",
	"rabbitmq_raft_last_applied":           "rabbitmq_raft_log_last_applied_index",
	"rabbitmq_raft_snapshot_index":         "rabbitmq_raft_log_snapshot_index",
	"rabbitmq_raft_term":                   "rabbitmq_raft_term_total",
	"rabbitmq_raft_log_last_applied":       "rabbitmq_raft_log_last_applied_index",
}

// NormalizeToClassicNames rewrites /metrics/detailed families to classic rabbitmq_*
// names and emits legacy raft aliases expected by existing dashboards/alerts.
//
// RabbitMQ /metrics/detailed uses the rabbitmq_detailed_ prefix so it can be
// scraped alongside /metrics. The unified exporter serves a single /metrics
// surface, so callers expect the classic names.
func NormalizeToClassicNames(families []model.Family) []model.Family {
	if len(families) == 0 {
		return families
	}
	out := make([]model.Family, 0, len(families)*2)
	for _, f := range families {
		name := f.Name
		if strings.HasPrefix(name, detailedPrefix) {
			name = "rabbitmq_" + strings.TrimPrefix(name, detailedPrefix)
		}
		nf := f
		nf.Name = name
		out = append(out, nf)

		if alias, ok := raftLegacyAliases[name]; ok {
			af := nf
			af.Name = alias
			out = append(out, af)
		}
	}
	return out
}
