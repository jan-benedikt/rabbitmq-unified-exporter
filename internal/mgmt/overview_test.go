package mgmt

import (
	"encoding/json"
	"testing"
)

func TestOverviewQueueTotalsWithDetails(t *testing.T) {
	raw := []byte(`{
		"node": "rabbit@n1",
		"cluster_name": "c",
		"rabbitmq_version": "3.9.15",
		"erlang_version": "24.3.4",
		"object_totals": {"channels": 2, "connections": 1, "queues": 2},
		"queue_totals": {
			"messages": 7,
			"messages_ready": 5,
			"messages_unacknowledged": 2,
			"messages_details": {"rate": 0.1},
			"messages_ready_details": {"rate": 0.0}
		},
		"message_stats": {
			"publish": 10,
			"publish_details": {"rate": 1.5}
		}
	}`)
	var ov Overview
	if err := json.Unmarshal(raw, &ov); err != nil {
		t.Fatal(err)
	}
	if ov.QueueTotals["messages"] != 7 || ov.QueueTotals["messages_ready"] != 5 {
		t.Fatalf("queue_totals: %#v", ov.QueueTotals)
	}
	if _, ok := ov.QueueTotals["messages_details"]; ok {
		t.Fatal("details object should be skipped")
	}
	if ov.ObjectTotals["queues"] != 2 {
		t.Fatalf("object_totals: %#v", ov.ObjectTotals)
	}
}
