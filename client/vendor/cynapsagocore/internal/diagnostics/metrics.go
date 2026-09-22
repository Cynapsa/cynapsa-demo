package diagnostics

import (
	"math"
	"sync"
)

// Metric is one fixed-cardinality private measurement.
type Metric string

const (
	MetricCommandsAdmitted Metric = "commands_admitted"
	MetricCommandsComplete Metric = "commands_complete"
	MetricCommandsFailed   Metric = "commands_failed"
	MetricEventsPublished  Metric = "events_published"
	MetricEventsRejected   Metric = "events_rejected"
	MetricWorkerPanics     Metric = "worker_panics"
	MetricActiveWorkers    Metric = "active_workers"
	MetricPendingTimers    Metric = "pending_timers"
)

var allowedMetrics = map[Metric]struct{}{
	MetricCommandsAdmitted: {},
	MetricCommandsComplete: {},
	MetricCommandsFailed:   {},
	MetricEventsPublished:  {},
	MetricEventsRejected:   {},
	MetricWorkerPanics:     {},
	MetricActiveWorkers:    {},
	MetricPendingTimers:    {},
}

// Metrics owns fixed-cardinality private counters and gauges.
type Metrics struct {
	mu     sync.RWMutex
	values map[Metric]float64
}

// NewMetrics constructs an empty bounded metric set.
func NewMetrics() *Metrics {
	return &Metrics{values: make(map[Metric]float64, len(allowedMetrics))}
}

// Record replaces one allowlisted finite private metric.
func (m *Metrics) Record(name Metric, value float64) error {
	if _, ok := allowedMetrics[name]; !ok {
		return ErrUnknownMetric
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrInvalidMetric
	}
	m.mu.Lock()
	m.ensureLocked()
	m.values[name] = value
	m.mu.Unlock()
	return nil
}

// Add increments one allowlisted metric by a finite amount.
func (m *Metrics) Add(name Metric, delta float64) error {
	if _, ok := allowedMetrics[name]; !ok {
		return ErrUnknownMetric
	}
	if math.IsNaN(delta) || math.IsInf(delta, 0) {
		return ErrInvalidMetric
	}
	m.mu.Lock()
	m.ensureLocked()
	next := m.values[name] + delta
	if math.IsNaN(next) || math.IsInf(next, 0) {
		m.mu.Unlock()
		return ErrInvalidMetric
	}
	m.values[name] = next
	m.mu.Unlock()
	return nil
}

// Snapshot returns a bounded copy of all recorded metrics.
func (m *Metrics) Snapshot() map[Metric]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[Metric]float64, len(m.values))
	for name, value := range m.values {
		result[name] = value
	}
	return result
}

func (m *Metrics) ensureLocked() {
	if m.values == nil {
		m.values = make(map[Metric]float64, len(allowedMetrics))
	}
}
