// Command alertlint extracts metric names from Grafana alert YAML and reports coverage.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

var metricRe = regexp.MustCompile(`\b((?:rabbitmq|erlang|probe|telemetry)_[a-zA-Z0-9_]+|up)\b`)

func main() {
	alerts := flag.String("alerts", "", "comma-separated Grafana alert YAML files")
	capabilities := flag.String("capabilities", "", "optional capabilities JSON (unused stub)")
	flag.Parse()
	_ = capabilities

	if *alerts == "" {
		fmt.Fprintln(os.Stderr, "usage: alertlint -alerts file1.yaml,file2.yaml")
		os.Exit(2)
	}

	known := map[string]string{
		// Native / mapped
		"rabbitmq_alarms_memory_used_watermark":              "native|json",
		"rabbitmq_alarms_free_disk_space_watermark":          "native|json",
		"rabbitmq_alarms_file_descriptor_limit":              "native|json-approx",
		"rabbitmq_channel_messages_unroutable_dropped_total": "native|json-unsupported",
		"rabbitmq_channel_messages_unroutable_returned_total": "native|json-unsupported",
		"rabbitmq_global_messages_redelivered_total":         "native|json-approx",
		"rabbitmq_global_messages_delivered_total":           "native|json-approx",
		"rabbitmq_queue_messages_ready":                      "native|json",
		"rabbitmq_queue_messages_unacked":                    "native|json",
		"rabbitmq_queue_consumers":                           "native|json",
		"rabbitmq_detailed_queue_messages_ready":             "native-detailed",
		"rabbitmq_detailed_queue_messages_unacked":           "native-detailed",
		"rabbitmq_detailed_queue_consumers":                  "native-detailed",
		"rabbitmq_raft_entry_commit_latency_seconds":         "native|json-unsupported",
		"rabbitmq_raft_log_last_written_index":               "native|json-unsupported",
		"rabbitmq_raft_log_commit_index":                     "native|json-unsupported",
		"rabbitmq_raft_log_last_applied_index":               "native|json-unsupported",
		"rabbitmq_raft_term_total":                           "native|json-unsupported",
		"rabbitmq_process_open_fds":                          "native|json",
		"rabbitmq_process_max_fds":                           "native|json",
		"rabbitmq_process_resident_memory_bytes":             "native|json",
		"rabbitmq_resident_memory_limit_bytes":               "native|json",
		"rabbitmq_disk_space_available_bytes":                "native|json",
		"rabbitmq_disk_space_available_limit_bytes":          "native|json",
		// Exporter contracts
		"rabbitmq_cluster_nodes":                             "exporter",
		"rabbitmq_cluster_nodes_running":                     "exporter",
		"rabbitmq_cluster_partitions":                        "exporter",
		"rabbitmq_application_running":                       "exporter",
		"rabbitmq_quorum_queue_available":                    "exporter",
		"rabbitmq_vhost_running":                             "exporter",
		"rabbitmq_expected_target":                           "exporter",
		"rabbitmq_message_delivery_test_success":             "exporter",
		"rabbitmq_amqp_tcp_success":                          "exporter",
		"rabbitmq_unified_exporter_target_up":                "exporter",
		"rabbitmq_unified_exporter_collection_success":       "exporter",
		"rabbitmq_unified_exporter_data_age_seconds":         "exporter",
		"probe_success":                                      "exporter-alias-or-blackbox",
		"up":                                                 "prometheus-scrape (exporter process only)",
	}

	found := map[string]struct{}{}
	for _, f := range strings.Split(*alerts, ",") {
		f = strings.TrimSpace(f)
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", f, err)
			os.Exit(1)
		}
		// Soft-parse: walk all strings in YAML for PromQL.
		var root interface{}
		if err := yaml.Unmarshal(data, &root); err != nil {
			fmt.Fprintf(os.Stderr, "yaml %s: %v\n", f, err)
			os.Exit(1)
		}
		walk(root, found)
	}

	names := make([]string, 0, len(found))
	for m := range found {
		names = append(names, m)
	}
	sort.Strings(names)

	missing := 0
	fmt.Println("METRIC\tCOVERAGE")
	for _, m := range names {
		cov, ok := known[m]
		if !ok {
			// regex __name__=~ may produce partials; skip non-metric noise
			if !strings.HasPrefix(m, "rabbitmq_") && m != "up" && m != "probe_success" && !strings.HasPrefix(m, "erlang_") {
				continue
			}
			cov = "UNKNOWN"
			missing++
		}
		fmt.Printf("%s\t%s\n", m, cov)
	}
	if missing > 0 {
		fmt.Fprintf(os.Stderr, "alertlint: %d metrics without coverage mapping\n", missing)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "alertlint: ok (%d metrics)\n", len(names))
}

func walk(v interface{}, found map[string]struct{}) {
	switch t := v.(type) {
	case map[string]interface{}:
		for _, child := range t {
			walk(child, found)
		}
	case map[interface{}]interface{}:
		for _, child := range t {
			walk(child, found)
		}
	case []interface{}:
		for _, child := range t {
			walk(child, found)
		}
	case string:
		for _, m := range metricRe.FindAllString(t, -1) {
			found[m] = struct{}{}
		}
	}
}
