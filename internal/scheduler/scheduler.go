// Package scheduler runs per-target collection loops with jitter and singleflight.
package scheduler

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/safeerr"
)

// Collector collects metrics for one resolved target.
type Collector interface {
	Collect(ctx context.Context, target config.ResolvedTarget) (*model.Snapshot, error)
}

// Metrics reports scheduler self-metrics.
type Metrics interface {
	IncSkipped(targetID string)
	ObserveCollection(targetID string, success bool, duration time.Duration)
}

// Scheduler manages independent collection loops per target.
type Scheduler struct {
	targets   []config.ResolvedTarget
	collector Collector
	store     *model.SnapshotStore
	sem       *semaphore.Weighted
	sf        singleflight.Group
	metrics   Metrics
	log       *slog.Logger
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   bool
}

// New creates a scheduler.
func New(targets []config.ResolvedTarget, collector Collector, store *model.SnapshotStore, maxConcurrency int64, metrics Metrics, log *slog.Logger) *Scheduler {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		targets:   targets,
		collector: collector,
		store:     store,
		sem:       semaphore.NewWeighted(maxConcurrency),
		metrics:   metrics,
		log:       log,
	}
}

// Start launches per-target loops.
func (s *Scheduler) Start(parent context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.running = true
	for _, t := range s.targets {
		t := t
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.loop(ctx, t)
		}()
	}
}

// Stop cancels loops and waits.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.cancel()
	s.running = false
	s.mu.Unlock()
	s.wg.Wait()
}

// Running reports whether the scheduler loops are active.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// CollectNow triggers an immediate collection via singleflight (for /probe).
// targetID must be the fully-qualified id "group/name" (name-only matching is rejected
// to avoid cross-group collisions).
func (s *Scheduler) CollectNow(ctx context.Context, targetID string) (*model.Snapshot, error) {
	var target *config.ResolvedTarget
	for i := range s.targets {
		if s.targets[i].ID == targetID {
			target = &s.targets[i]
			break
		}
	}
	if target == nil {
		return nil, ErrUnknownTarget
	}
	return s.collectOnce(ctx, *target)
}

// ErrUnknownTarget is returned when /probe references an unknown target.
var ErrUnknownTarget = errString("unknown target")

type errString string

func (e errString) Error() string { return string(e) }

func (s *Scheduler) loop(ctx context.Context, t config.ResolvedTarget) {
	// Initial jitter so targets don't stampede.
	jitter := jitterDuration(t.ScrapeInterval, t.JitterRatio)
	timer := time.NewTimer(jitter)
	defer timer.Stop()

	collecting := false
	var collectMu sync.Mutex
	failStreak := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			collectMu.Lock()
			if collecting {
				collectMu.Unlock()
				if s.metrics != nil {
					s.metrics.IncSkipped(t.ID)
				}
				// Keep cadence from this tick; do not add a full extra interval.
				timer.Reset(t.ScrapeInterval + jitterDuration(t.ScrapeInterval, t.JitterRatio))
				continue
			}
			collecting = true
			collectMu.Unlock()

			started := time.Now()
			// Do not wrap the whole collect in one scrape_timeout: Prometheus and
			// Management each apply their own independent deadline inside Collect.
			// A generous safety bound still prevents a wedged collect from hanging forever.
			safety := t.ScrapeTimeout
			if safety < 5*time.Second {
				safety = 5 * time.Second
			}
			safety *= 2 // parallel sources, each up to scrape_timeout
			cctx, cancel := context.WithTimeout(ctx, safety)
			_, err := s.collectOnce(cctx, t)
			cancel()
			if err != nil {
				failStreak++
				s.log.Warn("collection failed", "target", t.ID, "err", safeerr.Err(err), "fail_streak", failStreak)
			} else {
				failStreak = 0
			}

			collectMu.Lock()
			collecting = false
			collectMu.Unlock()

			// Schedule relative to collection start so a slow scrape does not
			// permanently stretch the interval to timeout+interval (~30s+).
			elapsed := time.Since(started)
			wait := t.ScrapeInterval - elapsed
			if wait < 0 {
				wait = 0
			}
			if failStreak > 0 {
				shift := failStreak
				if shift > 3 {
					shift = 3
				}
				backoff := t.ScrapeInterval * time.Duration(1<<uint(shift))
				if backoff > 8*t.ScrapeInterval {
					backoff = 8 * t.ScrapeInterval
				}
				if backoff > wait {
					wait = backoff
				}
			}
			timer.Reset(wait + jitterDuration(t.ScrapeInterval, t.JitterRatio))
		}
	}
}

func (s *Scheduler) collectOnce(ctx context.Context, t config.ResolvedTarget) (*model.Snapshot, error) {
	v, err, _ := s.sf.Do(t.ID, func() (interface{}, error) {
		if err := s.sem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		defer s.sem.Release(1)

		start := time.Now()
		snap, err := s.collector.Collect(ctx, t)
		dur := time.Since(start)
		success := err == nil && snap != nil && snap.Success
		if s.metrics != nil {
			s.metrics.ObserveCollection(t.ID, success, dur)
		}
		if snap == nil {
			snap = &model.Snapshot{
				TargetID:    t.ID,
				Group:       t.Group,
				CollectedAt: time.Now(),
				Success:     false,
				Error:       errStringValue(err),
				Sources:     map[string]model.SourceStatus{},
			}
		}
		snap.TargetID = t.ID
		snap.Group = t.Group
		if snap.CollectedAt.IsZero() {
			snap.CollectedAt = time.Now()
		}
		// Only store successful RabbitMQ data; always store snapshot for diagnostics.
		s.store.Set(snap)
		if err != nil {
			return snap, err
		}
		return snap, nil
	})
	if v == nil {
		return nil, err
	}
	return v.(*model.Snapshot), err
}

func errStringValue(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func jitterDuration(interval time.Duration, ratio float64) time.Duration {
	if ratio <= 0 || interval <= 0 {
		return 0
	}
	max := float64(interval) * ratio
	return time.Duration(rand.Float64() * max) //nolint:gosec // jitter only
}
