// Package model defines the shared metric model used by all collectors.
package model

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// MetricType is the Prometheus metric type.
type MetricType string

const (
	TypeUntyped   MetricType = "untyped"
	TypeGauge     MetricType = "gauge"
	TypeCounter   MetricType = "counter"
	TypeHistogram MetricType = "histogram"
	TypeSummary   MetricType = "summary"
)

// LabelPair is a name/value label.
type LabelPair struct {
	Name  string
	Value string
}

// Sample is a single time series sample.
type Sample struct {
	Labels    []LabelPair
	Value     float64
	Timestamp *int64 // milliseconds, optional
}

// Fingerprint returns a stable identity for the sample labels (excluding value).
func (s Sample) Fingerprint() string {
	if len(s.Labels) == 0 {
		return ""
	}
	parts := make([]string, len(s.Labels))
	for i, lp := range s.Labels {
		parts[i] = lp.Name + "=" + lp.Value
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// Family is a Prometheus metric family.
type Family struct {
	Name    string
	Help    string
	Type    MetricType
	Unit    string
	Samples []Sample
}

// Snapshot holds the last successful collection for a target (or merged set).
type Snapshot struct {
	TargetID    string
	Group       string
	CollectedAt time.Time
	Success     bool
	Error       string
	Families    []Family
	// FamiliesBySource keeps per-source families for split scrape endpoints
	// (/metrics/targets/.../prometheus|management|probes).
	FamiliesBySource map[string][]Family
	SourceCollectedAt map[string]time.Time
	OldestSourceAt    time.Time
	SeriesCount    int
	Sources        map[string]SourceStatus
}

// CloneSnapshot returns a shallow copy with independent Sources / FamiliesBySource maps.
func CloneSnapshot(snap *Snapshot) *Snapshot {
	if snap == nil {
		return nil
	}
	cp := *snap
	if snap.Sources != nil {
		cp.Sources = make(map[string]SourceStatus, len(snap.Sources))
		for k, v := range snap.Sources {
			cp.Sources[k] = v
		}
	}
	if snap.FamiliesBySource != nil {
		cp.FamiliesBySource = make(map[string][]Family, len(snap.FamiliesBySource))
		for k, v := range snap.FamiliesBySource {
			cp.FamiliesBySource[k] = append([]Family(nil), v...)
		}
	}
	if snap.SourceCollectedAt != nil {
		cp.SourceCollectedAt = make(map[string]time.Time, len(snap.SourceCollectedAt))
		for k, v := range snap.SourceCollectedAt {
			cp.SourceCollectedAt[k] = v
		}
	}
	if snap.Families != nil {
		cp.Families = append([]Family(nil), snap.Families...)
	}
	return &cp
}

// SourceStatus describes one collection source for a target.
type SourceStatus struct {
	Name      string
	Success   bool
	Error     string
	Duration  time.Duration
	Samples   int
	Sanitized int
}

// IntrinsicLabels must never be overridden by config labels.
var IntrinsicLabels = map[string]struct{}{
	"vhost":    {},
	"queue":    {},
	"exchange": {},
	"node":     {},
	"channel":  {},
	"peer":     {},
	"table":    {},
	"queue_type": {},
	"protocol": {},
}

// MergeOptions controls merge behaviour.
type MergeOptions struct {
	MaxSeries           int
	DedupeIdentical     bool
	OnDuplicateConflict func(name string, existing, incoming Sample)
	OnDropped           func(reason string, count int)
	OnTypeConflict      func(name string, existing, incoming MetricType)
}

// MergeFamilies merges families from multiple snapshots into one sorted list.
// Identical series (same name+labels+value) are deduplicated when DedupeIdentical is true.
// Conflicting series (same name+labels, different value) keep the first and report via callback.
func MergeFamilies(parts [][]Family, opts MergeOptions) []Family {
	byName := make(map[string]*Family)
	order := make([]string, 0)
	seen := make(map[string]float64) // name\0fp -> value
	dropped := 0
	series := 0

	for _, families := range parts {
		for _, f := range families {
			dst, ok := byName[f.Name]
			if !ok {
				cp := Family{Name: f.Name, Help: f.Help, Type: f.Type, Unit: f.Unit}
				byName[f.Name] = &cp
				order = append(order, f.Name)
				dst = byName[f.Name]
			} else {
				if dst.Help == "" && f.Help != "" {
					dst.Help = f.Help
				}
				if dst.Type == "" || dst.Type == TypeUntyped {
					if f.Type != "" {
						dst.Type = f.Type
					}
				} else if f.Type != "" && f.Type != TypeUntyped && f.Type != dst.Type {
					if opts.OnTypeConflict != nil {
						opts.OnTypeConflict(f.Name, dst.Type, f.Type)
					}
				}
			}
			for _, s := range f.Samples {
				if opts.MaxSeries > 0 && series >= opts.MaxSeries {
					dropped++
					continue
				}
				key := f.Name + "\x00" + s.Fingerprint()
				if v, exists := seen[key]; exists {
					if opts.DedupeIdentical && v == s.Value {
						continue
					}
					if opts.OnDuplicateConflict != nil {
						opts.OnDuplicateConflict(f.Name, Sample{Labels: s.Labels, Value: v}, s)
					}
					continue
				}
				seen[key] = s.Value
				dst.Samples = append(dst.Samples, s)
				series++
			}
		}
	}
	if dropped > 0 && opts.OnDropped != nil {
		opts.OnDropped("cardinality", dropped)
	}
	out := make([]Family, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out
}

// SnapshotStore is a concurrency-safe store of per-target snapshots.
type SnapshotStore struct {
	mu       sync.RWMutex
	latest   map[string]*Snapshot // most recent attempt (success or failure)
	lastGood map[string]*Snapshot // last successful RabbitMQ payload
	maxAge   time.Duration
}

// NewSnapshotStore creates a store. maxAge of 0 means no staleness filtering on read.
func NewSnapshotStore(maxAge time.Duration) *SnapshotStore {
	return &SnapshotStore{
		latest:   make(map[string]*Snapshot),
		lastGood: make(map[string]*Snapshot),
		maxAge:   maxAge,
	}
}

// Set stores a snapshot for a target.
// Failed collections update diagnostics (latest) but do not clear the last
// good RabbitMQ families — otherwise Prometheus scrapes of /metrics would omit
// series and Grafana shows multi-minute gaps.
//
// Partial results (e.g. Management OK, Prometheus dial timeout) still merge into
// lastGood so cluster/health series stay available while broker /metrics is down.
// Retained per-source families keep their SourceCollectedAt and expire via maxAge.
// Aggregate Families is rebuilt from FamiliesBySource.
func (s *SnapshotStore) Set(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := CloneSnapshot(snap)
	s.latest[snap.TargetID] = cp
	now := time.Now()
	if snap.Success {
		good := CloneSnapshot(snap)
		if good.SourceCollectedAt == nil {
			good.SourceCollectedAt = map[string]time.Time{}
		}
		if prev, ok := s.lastGood[snap.TargetID]; ok {
			good.FamiliesBySource, good.SourceCollectedAt = mergeFamiliesBySourceTimed(
				snap.FamiliesBySource, snap.SourceCollectedAt,
				prev.FamiliesBySource, prev.SourceCollectedAt,
				prev.CollectedAt, s.maxAge, now,
			)
		} else {
			pruneStaleSources(good, s.maxAge, now)
		}
		finalizeSnapshotFamilies(good)
		s.lastGood[snap.TargetID] = good
		return
	}
	if len(snap.Families) == 0 && len(snap.FamiliesBySource) == 0 {
		return
	}
	if prev, ok := s.lastGood[snap.TargetID]; ok {
		good := CloneSnapshot(prev)
		good.Sources = cp.Sources
		good.Error = cp.Error
		if len(snap.FamiliesBySource) > 0 || len(prev.FamiliesBySource) > 0 {
			good.FamiliesBySource, good.SourceCollectedAt = mergeFamiliesBySourceTimed(
				snap.FamiliesBySource, snap.SourceCollectedAt,
				prev.FamiliesBySource, prev.SourceCollectedAt,
				prev.CollectedAt, s.maxAge, now,
			)
			// Also fold any top-level Families from the partial snap into the matching source bag.
			if len(snap.Families) > 0 && len(snap.FamiliesBySource) == 0 {
				src := "management"
				good.FamiliesBySource[src] = MergeFamilies([][]Family{snap.Families, good.FamiliesBySource[src]}, MergeOptions{DedupeIdentical: true})
				if good.SourceCollectedAt == nil {
					good.SourceCollectedAt = map[string]time.Time{}
				}
				good.SourceCollectedAt[src] = now
			}
			finalizeSnapshotFamilies(good)
		} else {
			good.Families = MergeFamilies([][]Family{snap.Families, prev.Families}, MergeOptions{DedupeIdentical: true})
			good.SeriesCount = CountSeries(good.Families)
		}
		// Keep previous CollectedAt so max_staleness still tracks last full success.
		s.lastGood[snap.TargetID] = good
		return
	}
	good := CloneSnapshot(snap)
	if good.SourceCollectedAt == nil {
		good.SourceCollectedAt = map[string]time.Time{}
		for src := range good.FamiliesBySource {
			good.SourceCollectedAt[src] = snap.CollectedAt
		}
	}
	finalizeSnapshotFamilies(good)
	s.lastGood[snap.TargetID] = good
}

func finalizeSnapshotFamilies(snap *Snapshot) {
	parts := make([][]Family, 0, len(snap.FamiliesBySource))
	var oldest time.Time
	for src, fams := range snap.FamiliesBySource {
		if len(fams) == 0 {
			continue
		}
		parts = append(parts, fams)
		if t, ok := snap.SourceCollectedAt[src]; ok {
			if oldest.IsZero() || t.Before(oldest) {
				oldest = t
			}
		}
	}
	if len(parts) > 0 {
		snap.Families = MergeFamilies(parts, MergeOptions{DedupeIdentical: true})
		snap.SeriesCount = CountSeries(snap.Families)
	}
	snap.OldestSourceAt = oldest
}

func pruneStaleSources(snap *Snapshot, maxAge time.Duration, now time.Time) {
	if maxAge <= 0 || snap.SourceCollectedAt == nil {
		return
	}
	for src, at := range snap.SourceCollectedAt {
		if now.Sub(at) > maxAge {
			delete(snap.FamiliesBySource, src)
			delete(snap.SourceCollectedAt, src)
		}
	}
}

func mergeFamiliesBySourceTimed(
	newer map[string][]Family, newerAt map[string]time.Time,
	older map[string][]Family, olderAt map[string]time.Time,
	prevCollectedAt time.Time, maxAge time.Duration, now time.Time,
) (map[string][]Family, map[string]time.Time) {
	out := make(map[string][]Family)
	outAt := make(map[string]time.Time)
	for k, v := range older {
		if len(v) == 0 {
			continue
		}
		at := olderAt[k]
		if at.IsZero() {
			at = prevCollectedAt
		}
		if at.IsZero() {
			at = now
		}
		if maxAge > 0 && now.Sub(at) > maxAge {
			continue
		}
		out[k] = append([]Family(nil), v...)
		outAt[k] = at
	}
	for k, v := range newer {
		if len(v) == 0 {
			continue
		}
		at := now
		if newerAt != nil {
			if t, ok := newerAt[k]; ok && !t.IsZero() {
				at = t
			}
		}
		if prev, ok := out[k]; ok {
			out[k] = MergeFamilies([][]Family{v, prev}, MergeOptions{DedupeIdentical: true})
		} else {
			out[k] = append([]Family(nil), v...)
		}
		outAt[k] = at
	}
	return out, outAt
}

// Get returns a copy of the latest snapshot for a target, or nil.
func (s *SnapshotStore) Get(targetID string) *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.latest[targetID]
	if !ok {
		return nil
	}
	return CloneSnapshot(snap)
}

// Fresh returns a non-stale last-good snapshot for one target, or nil.
func (s *SnapshotStore) Fresh(targetID string, now time.Time) *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.lastGood[targetID]
	if !ok {
		return nil
	}
	if s.maxAge > 0 && now.Sub(snap.CollectedAt) > s.maxAge {
		return nil
	}
	cp := CloneSnapshot(snap)
	if len(cp.FamiliesBySource) > 0 {
		pruneStaleSources(cp, s.maxAge, now)
		if len(cp.FamiliesBySource) == 0 {
			return nil
		}
		finalizeSnapshotFamilies(cp)
	}
	return cp
}

// AllFresh returns non-stale last-successful snapshots for exposition.
// A subsequent failed collection does not remove series until maxAge expires.
func (s *SnapshotStore) AllFresh(now time.Time, groupFilter string) []*Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Snapshot, 0, len(s.lastGood))
	for _, snap := range s.lastGood {
		if groupFilter != "" && snap.Group != groupFilter {
			continue
		}
		if s.maxAge > 0 && now.Sub(snap.CollectedAt) > s.maxAge {
			continue
		}
		cp := CloneSnapshot(snap)
		if len(cp.FamiliesBySource) > 0 {
			pruneStaleSources(cp, s.maxAge, now)
			if len(cp.FamiliesBySource) == 0 {
				continue
			}
			finalizeSnapshotFamilies(cp)
		}
		out = append(out, cp)
	}
	return out
}

// All returns all latest snapshots regardless of success/staleness.
func (s *SnapshotStore) All() []*Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Snapshot, 0, len(s.latest))
	for _, snap := range s.latest {
		out = append(out, CloneSnapshot(snap))
	}
	return out
}

// SetMaxAge updates the staleness window.
func (s *SnapshotStore) SetMaxAge(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxAge = d
}

// RetainOnly removes snapshots whose TargetID is not in keep.
func (s *SnapshotStore) RetainOnly(keep map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.latest {
		if _, ok := keep[id]; !ok {
			delete(s.latest, id)
		}
	}
	for id := range s.lastGood {
		if _, ok := keep[id]; !ok {
			delete(s.lastGood, id)
		}
	}
}

// ApplyTargetLabels prepends target identity labels to every sample.
// Intrinsic labels already present on the sample are preserved and never overwritten.
func ApplyTargetLabels(families []Family, labels map[string]string) []Family {
	if len(labels) == 0 {
		return families
	}
	// Stable order for deterministic output.
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]Family, len(families))
	for i, f := range families {
		nf := f
		nf.Samples = make([]Sample, len(f.Samples))
		for j, s := range f.Samples {
			existing := make(map[string]struct{}, len(s.Labels))
			for _, lp := range s.Labels {
				existing[lp.Name] = struct{}{}
			}
			merged := make([]LabelPair, 0, len(keys)+len(s.Labels))
			for _, k := range keys {
				if _, isIntrinsic := IntrinsicLabels[k]; isIntrinsic {
					continue
				}
				if _, ok := existing[k]; ok {
					continue
				}
				merged = append(merged, LabelPair{Name: k, Value: labels[k]})
			}
			merged = append(merged, s.Labels...)
			nf.Samples[j] = Sample{Labels: merged, Value: s.Value, Timestamp: s.Timestamp}
		}
		out[i] = nf
	}
	return out
}

// CountSeries returns the total number of samples across families.
func CountSeries(families []Family) int {
	n := 0
	for _, f := range families {
		n += len(f.Samples)
	}
	return n
}

// ExpositionOptions controls text exposition format.
type ExpositionOptions struct {
	OpenMetrics bool
}

// FormatExposition writes Prometheus text exposition (0.0.4) without timestamps
// unless a sample has an explicit Timestamp set.
func FormatExposition(families []Family) string {
	var b strings.Builder
	_ = WriteExposition(&b, families)
	return b.String()
}

// WriteExposition streams Prometheus text exposition (0.0.4) to w.
func WriteExposition(w io.Writer, families []Family) error {
	return WriteExpositionOpts(w, families, ExpositionOptions{})
}

// WriteExpositionOpts streams exposition; OpenMetrics adds # UNIT lines (EOF is caller's responsibility).
func WriteExpositionOpts(w io.Writer, families []Family, opts ExpositionOptions) error {
	for _, f := range families {
		if f.Help != "" {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n", f.Name, escapeHelp(f.Help)); err != nil {
				return err
			}
		}
		t := f.Type
		if t == "" {
			t = TypeUntyped
		}
		if opts.OpenMetrics && t == TypeUntyped {
			t = "unknown"
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", f.Name, t); err != nil {
			return err
		}
		if opts.OpenMetrics && f.Unit != "" {
			if _, err := fmt.Fprintf(w, "# UNIT %s %s\n", f.Name, f.Unit); err != nil {
				return err
			}
		}
		for _, s := range f.Samples {
			if _, err := io.WriteString(w, f.Name); err != nil {
				return err
			}
			if len(s.Labels) > 0 {
				if _, err := w.Write([]byte{'{'}); err != nil {
					return err
				}
				for i, lp := range s.Labels {
					if i > 0 {
						if _, err := w.Write([]byte{','}); err != nil {
							return err
						}
					}
					if _, err := fmt.Fprintf(w, `%s="%s"`, lp.Name, escapeLabelValue(lp.Value)); err != nil {
						return err
					}
				}
				if _, err := w.Write([]byte{'}'}); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, " %v", formatFloat(s.Value)); err != nil {
				return err
			}
			if s.Timestamp != nil {
				if _, err := fmt.Fprintf(w, " %d", *s.Timestamp); err != nil {
					return err
				}
			}
			if _, err := w.Write([]byte{'\n'}); err != nil {
				return err
			}
		}
	}
	return nil
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

func formatFloat(v float64) string {
	if v != v { // NaN
		return "NaN"
	}
	if v > 0 && v > 1e308 {
		return "+Inf"
	}
	if v < 0 && v < -1e308 {
		return "-Inf"
	}
	return fmt.Sprintf("%g", v)
}
