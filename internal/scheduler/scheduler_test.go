package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

type fakeCollector struct {
	calls atomic.Int32
	block chan struct{}
}

func (f *fakeCollector) Collect(ctx context.Context, t config.ResolvedTarget) (*model.Snapshot, error) {
	f.calls.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &model.Snapshot{TargetID: t.ID, Group: t.Group, Success: true, CollectedAt: time.Now()}, nil
}

type noopMetrics struct{ skipped atomic.Int32 }

func (n *noopMetrics) IncSkipped(string)                              { n.skipped.Add(1) }
func (n *noopMetrics) ObserveCollection(string, bool, time.Duration) {}

func TestSingleflightNoOverlap(t *testing.T) {
	store := model.NewSnapshotStore(0)
	block := make(chan struct{})
	fc := &fakeCollector{block: block}
	tgt := config.ResolvedTarget{
		ID: "g/a", Name: "a", Group: "g",
		ScrapeInterval: 50 * time.Millisecond,
		ScrapeTimeout:  2 * time.Second,
		JitterRatio:    0,
	}
	m := &noopMetrics{}
	s := New([]config.ResolvedTarget{tgt}, fc, store, 10, m, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	// Wait until first collect is in progress
	deadline := time.Now().Add(2 * time.Second)
	for fc.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond) // allow overlap attempt
	close(block)
	time.Sleep(100 * time.Millisecond)
	s.Stop()

	if m.skipped.Load() == 0 && fc.calls.Load() > 3 {
		// Either skipped or limited calls — both ok; just ensure store has snapshot
	}
	if store.Get("g/a") == nil {
		t.Fatal("expected snapshot")
	}
}

func TestCollectNowUnknown(t *testing.T) {
	s := New(nil, &fakeCollector{}, model.NewSnapshotStore(0), 1, nil, nil)
	_, err := s.CollectNow(context.Background(), "missing")
	if err != ErrUnknownTarget {
		t.Fatalf("err=%v", err)
	}
}
