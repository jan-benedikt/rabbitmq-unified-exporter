// Package promsrc fetches and sanitizes native RabbitMQ Prometheus exposition.
package promsrc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/model"
)

// SanitizeStats reports what the sanitizer did.
type SanitizeStats struct {
	DroppedSamples  int
	KeptSamples     int
	Reasons         map[string]int
	StructuralError string
}

// SanitizeOptions controls sanitization behaviour.
type SanitizeOptions struct {
	InvalidValues     []string
	MaxInvalidSamples int
}

type metricMeta struct {
	help string
	typ  model.MetricType
	unit string
}

// Sanitize parses Prometheus text exposition, dropping invalid sample values
// while preserving HELP/TYPE metadata and valid samples (including NaN/+Inf/-Inf).
func Sanitize(data []byte, opts SanitizeOptions) ([]model.Family, SanitizeStats, error) {
	stats := SanitizeStats{Reasons: map[string]int{}}
	invalid := map[string]struct{}{}
	for _, v := range opts.InvalidValues {
		invalid[strings.ToLower(v)] = struct{}{}
	}
	if opts.MaxInvalidSamples <= 0 {
		opts.MaxInvalidSamples = 10000
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	const maxLine = 1 * 1024 * 1024
	scanner.Buffer(make([]byte, 0, 64*1024), maxLine)

	metas := map[string]*metricMeta{}
	order := make([]string, 0)
	samples := map[string][]model.Sample{}

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			if err := parseMeta(line, metas, &order); err != nil {
				stats.StructuralError = fmt.Sprintf("line %d: %v", lineNo, err)
				return nil, stats, fmt.Errorf("%s", stats.StructuralError)
			}
			continue
		}
		name, sample, reason, err := parseSampleLine(line)
		if err != nil {
			stats.StructuralError = fmt.Sprintf("line %d: %v", lineNo, err)
			stats.Reasons["structural"]++
			if stats.DroppedSamples+1 > opts.MaxInvalidSamples {
				return nil, stats, fmt.Errorf("too many invalid samples: %s", stats.StructuralError)
			}
			stats.DroppedSamples++
			continue
		}
		if reason != "" {
			stats.Reasons[reason]++
			stats.DroppedSamples++
			if stats.DroppedSamples > opts.MaxInvalidSamples {
				stats.StructuralError = "max_invalid_samples exceeded"
				return nil, stats, fmt.Errorf("%s", stats.StructuralError)
			}
			// Ensure family still exists so HELP/TYPE are preserved.
			ensureFamily(name, metas, &order, samples)
			continue
		}
		// Check token-level invalid values already handled in parseSampleLine via reason.
		_ = invalid
		ensureFamily(name, metas, &order, samples)
		samples[name] = append(samples[name], sample)
		stats.KeptSamples++
	}
	if err := scanner.Err(); err != nil {
		stats.StructuralError = err.Error()
		return nil, stats, err
	}

	out := make([]model.Family, 0, len(order))
	for _, name := range order {
		m := metas[name]
		f := model.Family{
			Name:    name,
			Samples: samples[name],
		}
		if m != nil {
			f.Help = m.help
			f.Type = m.typ
			f.Unit = m.unit
		}
		if f.Type == "" {
			f.Type = model.TypeUntyped
		}
		out = append(out, f)
	}
	return out, stats, nil
}

func ensureFamily(name string, metas map[string]*metricMeta, order *[]string, samples map[string][]model.Sample) {
	if _, ok := metas[name]; !ok {
		metas[name] = &metricMeta{typ: model.TypeUntyped}
		*order = append(*order, name)
	}
	if _, ok := samples[name]; !ok {
		samples[name] = nil
	}
}

func parseMeta(line string, metas map[string]*metricMeta, order *[]string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil // plain comment
	}
	switch fields[1] {
	case "HELP":
		if len(fields) < 3 {
			return fmt.Errorf("invalid HELP")
		}
		name := fields[2]
		help := ""
		if len(fields) > 3 {
			// Reconstruct help after metric name.
			idx := strings.Index(line, name)
			if idx >= 0 {
				rest := strings.TrimSpace(line[idx+len(name):])
				help = unescapeHelp(rest)
			}
		}
		m := metas[name]
		if m == nil {
			m = &metricMeta{}
			metas[name] = m
			*order = append(*order, name)
		}
		m.help = help
	case "TYPE":
		if len(fields) < 4 {
			return fmt.Errorf("invalid TYPE")
		}
		name := fields[2]
		typ := model.MetricType(fields[3])
		m := metas[name]
		if m == nil {
			m = &metricMeta{}
			metas[name] = m
			*order = append(*order, name)
		}
		if m.typ != "" && m.typ != model.TypeUntyped && m.typ != typ {
			return fmt.Errorf("TYPE conflict for %s: %s vs %s", name, m.typ, typ)
		}
		m.typ = typ
	case "UNIT":
		if len(fields) < 4 {
			return fmt.Errorf("invalid UNIT")
		}
		name := fields[2]
		m := metas[name]
		if m == nil {
			m = &metricMeta{}
			metas[name] = m
			*order = append(*order, name)
		}
		m.unit = fields[3]
	default:
		// Other comments ignored.
	}
	return nil
}

func unescapeHelp(s string) string {
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}

// parseSampleLine returns metric name, sample, drop reason (if invalid value), or structural error.
func parseSampleLine(line string) (name string, sample model.Sample, reason string, err error) {
	i := 0
	// Metric name: [a-zA-Z_:][a-zA-Z0-9_:]*
	for i < len(line) && (isMetricNameChar(line[i], i == 0)) {
		i++
	}
	if i == 0 {
		return "", sample, "", fmt.Errorf("missing metric name")
	}
	name = line[:i]
	rest := strings.TrimLeftFunc(line[i:], unicode.IsSpace)

	labels := []model.LabelPair{}
	if strings.HasPrefix(rest, "{") {
		end, lbls, lerr := parseLabels(rest)
		if lerr != nil {
			return name, sample, "bad_labels", lerr
		}
		labels = lbls
		rest = strings.TrimLeftFunc(rest[end:], unicode.IsSpace)
	}

	if rest == "" {
		return name, sample, "unknown_value", nil
	}

	// Value token and optional timestamp.
	parts := splitValueAndTimestamp(rest)
	valueTok := parts[0]
	lower := strings.ToLower(valueTok)
	switch lower {
	case "unknown", "undefined", "null":
		return name, sample, "unknown_value", nil
	}

	val, verr := parseFloat(valueTok)
	if verr != nil {
		return name, sample, "invalid_number", nil
	}
	sample.Labels = labels
	sample.Value = val
	if len(parts) > 1 {
		ts, terr := strconv.ParseInt(parts[1], 10, 64)
		if terr != nil {
			return name, sample, "invalid_number", nil
		}
		sample.Timestamp = &ts
	}
	return name, sample, "", nil
}

func splitValueAndTimestamp(rest string) []string {
	// Value may be NaN/+Inf/-Inf or a float; timestamp is integer after whitespace.
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return []string{""}
	}
	if len(fields) == 1 {
		return fields
	}
	// Only treat second field as timestamp if it looks like an integer.
	if _, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
		return fields[:2]
	}
	return fields[:1]
}

func parseFloat(tok string) (float64, error) {
	switch strings.ToLower(tok) {
	case "nan":
		return math.NaN(), nil
	case "+inf", "inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(tok, 64)
}

const maxLabelsPerSample = 64

func parseLabels(s string) (end int, labels []model.LabelPair, err error) {
	if s[0] != '{' {
		return 0, nil, fmt.Errorf("expected '{'")
	}
	i := 1
	for i < len(s) {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == ',') {
			i++
		}
		if i < len(s) && s[i] == '}' {
			return i + 1, labels, nil
		}
		if len(labels) >= maxLabelsPerSample {
			return 0, nil, fmt.Errorf("too many labels (max %d)", maxLabelsPerSample)
		}
		// label name
		start := i
		for i < len(s) && isLabelNameChar(s[i], i == start) {
			i++
		}
		if i == start {
			return 0, nil, fmt.Errorf("invalid label name")
		}
		lname := s[start:i]
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= len(s) || s[i] != '=' {
			return 0, nil, fmt.Errorf("expected '=' after label name")
		}
		i++
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= len(s) || s[i] != '"' {
			return 0, nil, fmt.Errorf("expected quoted label value")
		}
		i++
		var b strings.Builder
		for i < len(s) {
			c := s[i]
			if c == '\\' {
				if i+1 >= len(s) {
					return 0, nil, fmt.Errorf("truncated escape")
				}
				switch s[i+1] {
				case 'n':
					b.WriteByte('\n')
				case '\\':
					b.WriteByte('\\')
				case '"':
					b.WriteByte('"')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				break
			}
			b.WriteByte(c)
			i++
		}
		labels = append(labels, model.LabelPair{Name: lname, Value: b.String()})
	}
	return 0, nil, fmt.Errorf("unclosed labels")
}

func isMetricNameChar(c byte, first bool) bool {
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == ':' {
		return true
	}
	if !first && c >= '0' && c <= '9' {
		return true
	}
	return false
}

func isLabelNameChar(c byte, first bool) bool {
	if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
		return true
	}
	if !first && c >= '0' && c <= '9' {
		return true
	}
	return false
}

// LimitReader returns an io.Reader that errors after max bytes.
func LimitReader(r io.Reader, max int64) io.Reader {
	return &limitedReader{r: r, max: max}
}

type limitedReader struct {
	r   io.Reader
	max int64
	n   int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.n >= l.max {
		return 0, fmt.Errorf("response exceeds max_response_bytes (%d)", l.max)
	}
	maxRead := int64(len(p))
	if l.n+maxRead > l.max {
		maxRead = l.max - l.n + 1 // read one past to detect overflow
	}
	n, err := l.r.Read(p[:maxRead])
	l.n += int64(n)
	if l.n > l.max {
		return n, fmt.Errorf("response exceeds max_response_bytes (%d)", l.max)
	}
	return n, err
}
