package model

import (
	"strings"
	"testing"
	"time"
)

func TestMergeDedupesIdentical(t *testing.T) {
	a := []Family{{Name: "m", Type: TypeGauge, Samples: []Sample{{Value: 1, Labels: []LabelPair{{Name: "x", Value: "1"}}}}}}
	b := []Family{{Name: "m", Type: TypeGauge, Samples: []Sample{{Value: 1, Labels: []LabelPair{{Name: "x", Value: "1"}}}}}}
	out := MergeFamilies([][]Family{a, b}, MergeOptions{DedupeIdentical: true})
	if len(out) != 1 || len(out[0].Samples) != 1 {
		t.Fatalf("%+v", out)
	}
}

func TestMergeCardinalityGuard(t *testing.T) {
	var samples []Sample
	for i := 0; i < 10; i++ {
		samples = append(samples, Sample{Value: float64(i), Labels: []LabelPair{{Name: "i", Value: string(rune('a' + i))}}})
	}
	dropped := 0
	out := MergeFamilies([][]Family{{{Name: "m", Samples: samples}}}, MergeOptions{
		MaxSeries: 5,
		OnDropped: func(_ string, n int) { dropped = n },
	})
	if CountSeries(out) != 5 || dropped != 5 {
		t.Fatalf("series=%d dropped=%d", CountSeries(out), dropped)
	}
}

func TestApplyTargetLabelsPreservesIntrinsic(t *testing.T) {
	f := []Family{{Name: "m", Samples: []Sample{{
		Labels: []LabelPair{{Name: "vhost", Value: "/"}, {Name: "queue", Value: "q"}},
		Value:  1,
	}}}}
	out := ApplyTargetLabels(f, map[string]string{"instance": "rmq-1", "vhost": "SHOULD_NOT", "job": "j"})
	labs := out[0].Samples[0].Labels
	var vhost, instance string
	for _, lp := range labs {
		if lp.Name == "vhost" {
			vhost = lp.Value
		}
		if lp.Name == "instance" {
			instance = lp.Value
		}
	}
	if vhost != "/" {
		t.Fatalf("vhost overwritten: %s", vhost)
	}
	if instance != "rmq-1" {
		t.Fatalf("instance=%s", instance)
	}
}

func TestSnapshotStoreStaleness(t *testing.T) {
	s := NewSnapshotStore(time.Second)
	s.Set(&Snapshot{TargetID: "a", Group: "g", Success: true, CollectedAt: time.Now().Add(-2 * time.Second)})
	s.Set(&Snapshot{TargetID: "b", Group: "g", Success: true, CollectedAt: time.Now()})
	fresh := s.AllFresh(time.Now(), "")
	if len(fresh) != 1 || fresh[0].TargetID != "b" {
		t.Fatalf("%+v", fresh)
	}
}

func TestSnapshotStoreKeepsLastGoodOnFailure(t *testing.T) {
	s := NewSnapshotStore(time.Minute)
	now := time.Now()
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: now.Add(-5 * time.Second),
		Families: []Family{{Name: "rabbitmq_build_info", Samples: []Sample{{Value: 1}}}},
	})
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: false, CollectedAt: now,
		Error: "timeout",
	})
	fresh := s.AllFresh(now, "")
	if len(fresh) != 1 {
		t.Fatalf("expected last good retained, got %d", len(fresh))
	}
	if !fresh[0].Success || len(fresh[0].Families) != 1 {
		t.Fatalf("last good corrupted: %+v", fresh[0])
	}
	latest := s.Get("t1")
	if latest == nil || latest.Success {
		t.Fatalf("latest should be the failed attempt: %+v", latest)
	}
}

func TestSnapshotStoreMergesPartialFailure(t *testing.T) {
	s := NewSnapshotStore(time.Minute)
	now := time.Now()
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: now.Add(-10 * time.Second),
		Families: []Family{
			{Name: "rabbitmq_build_info", Samples: []Sample{{Value: 1}}},
			{Name: "rabbitmq_queues", Samples: []Sample{{Value: 10}}},
		},
	})
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: false, CollectedAt: now, Error: "prometheus timeout",
		Families: []Family{
			{Name: "rabbitmq_cluster_nodes_running", Samples: []Sample{{Value: 3}}},
		},
	})
	fresh := s.AllFresh(now, "")
	if len(fresh) != 1 {
		t.Fatalf("got %d", len(fresh))
	}
	names := map[string]float64{}
	for _, f := range fresh[0].Families {
		if len(f.Samples) > 0 {
			names[f.Name] = f.Samples[0].Value
		}
	}
	if names["rabbitmq_build_info"] != 1 || names["rabbitmq_queues"] != 10 {
		t.Fatalf("prometheus series lost: %v", names)
	}
	if names["rabbitmq_cluster_nodes_running"] != 3 {
		t.Fatalf("management series missing: %v", names)
	}
	if !fresh[0].CollectedAt.Equal(now.Add(-10 * time.Second)) {
		t.Fatalf("CollectedAt should stay at last full success, got %v", fresh[0].CollectedAt)
	}
}

func TestSnapshotStorePartialWithoutPrior(t *testing.T) {
	s := NewSnapshotStore(time.Minute)
	now := time.Now()
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: false, CollectedAt: now,
		Families: []Family{{Name: "rabbitmq_cluster_nodes", Samples: []Sample{{Value: 3}}}},
	})
	fresh := s.AllFresh(now, "")
	if len(fresh) != 1 || fresh[0].Families[0].Name != "rabbitmq_cluster_nodes" {
		t.Fatalf("%+v", fresh)
	}
}

func TestSnapshotStoreRetainsMissingSourceOnSuccess(t *testing.T) {
	s := NewSnapshotStore(5 * time.Minute)
	now := time.Now()
	old := now.Add(-time.Minute)
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: old,
		Families: []Family{
			{Name: "rabbitmq_queues", Samples: []Sample{{Value: 1}}},
			{Name: "rabbitmq_cluster_nodes", Samples: []Sample{{Value: 3}}},
		},
		FamiliesBySource: map[string][]Family{
			"prometheus": {{Name: "rabbitmq_queues", Samples: []Sample{{Value: 1}}}},
			"management": {{Name: "rabbitmq_cluster_nodes", Samples: []Sample{{Value: 3}}}},
		},
		SourceCollectedAt: map[string]time.Time{
			"prometheus": old,
			"management": old,
		},
	})
	// Prometheus OK, management failed — overall success in hybrid mode.
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: now,
		Families: []Family{{Name: "rabbitmq_queues", Samples: []Sample{{Value: 2}}}},
		FamiliesBySource: map[string][]Family{
			"prometheus": {{Name: "rabbitmq_queues", Samples: []Sample{{Value: 2}}}},
		},
		SourceCollectedAt: map[string]time.Time{"prometheus": now},
		Sources: map[string]SourceStatus{
			"prometheus": {Name: "prometheus", Success: true},
			"management": {Name: "management", Success: false, Error: "tls"},
		},
	})
	fresh := s.Fresh("t1", now)
	if fresh == nil {
		t.Fatal("expected last-good")
	}
	if len(fresh.FamiliesBySource["management"]) == 0 {
		t.Fatal("management families wiped on prometheus-only success")
	}
	if d := fresh.SourceCollectedAt["management"].Sub(old); d > time.Millisecond || d < -time.Millisecond {
		t.Fatalf("management SourceCollectedAt should stay old, got %v want %v", fresh.SourceCollectedAt["management"], old)
	}
	if fresh.FamiliesBySource["prometheus"][0].Samples[0].Value != 2 {
		t.Fatal("prometheus not updated")
	}
	names := map[string]float64{}
	for _, f := range fresh.Families {
		if len(f.Samples) > 0 {
			names[f.Name] = f.Samples[0].Value
		}
	}
	if names["rabbitmq_queues"] != 2 || names["rabbitmq_cluster_nodes"] != 3 {
		t.Fatalf("merged families wrong: %v", names)
	}
}

func TestSnapshotStoreExpiresStaleRetainedSource(t *testing.T) {
	s := NewSnapshotStore(30 * time.Second)
	now := time.Now()
	old := now.Add(-2 * time.Minute)
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: old,
		FamiliesBySource: map[string][]Family{
			"prometheus": {{Name: "rabbitmq_queues", Samples: []Sample{{Value: 1}}}},
			"management": {{Name: "rabbitmq_cluster_nodes", Samples: []Sample{{Value: 3}}}},
		},
		SourceCollectedAt: map[string]time.Time{"prometheus": old, "management": old},
	})
	s.Set(&Snapshot{
		TargetID: "t1", Group: "g", Success: true, CollectedAt: now,
		FamiliesBySource:  map[string][]Family{"prometheus": {{Name: "rabbitmq_queues", Samples: []Sample{{Value: 2}}}}},
		SourceCollectedAt: map[string]time.Time{"prometheus": now},
	})
	fresh := s.Fresh("t1", now)
	if fresh == nil {
		t.Fatal("expected fresh")
	}
	if len(fresh.FamiliesBySource["management"]) != 0 {
		t.Fatal("stale management should be expired")
	}
	if len(fresh.FamiliesBySource["prometheus"]) == 0 {
		t.Fatal("prometheus should remain")
	}
}


func TestFormatExposition(t *testing.T) {
	body := FormatExposition([]Family{{
		Name: "m", Help: "help", Type: TypeGauge,
		Samples: []Sample{{Labels: []LabelPair{{Name: "a", Value: `x"y`}}, Value: 1}},
	}})
	if !strings.Contains(body, `a="x\"y"`) {
		t.Fatal(body)
	}
	if !strings.Contains(body, "# TYPE m gauge") {
		t.Fatal(body)
	}
}
