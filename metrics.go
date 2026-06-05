package flux

import "sync/atomic"

// MetricsSnapshot represents a read-only snapshot of Flux telemetry counters.
type MetricsSnapshot struct {
	WritesTotal      uint64
	ProcessedTotal   uint64
	FailedTotal      uint64
	PulsesTotal      uint64
	PulsesSaturated  uint64
	WALWritesFailed  uint64
	ProcessingTimeMs uint64
}

// telemetry encapsulates the atomic counters used internally by Flux.
type telemetry struct {
	WritesTotal      atomic.Uint64
	ProcessedTotal   atomic.Uint64
	FailedTotal      atomic.Uint64
	PulsesTotal      atomic.Uint64
	PulsesSaturated  atomic.Uint64
	WALWritesFailed  atomic.Uint64
	ProcessingTimeMs atomic.Uint64
}

// snapshot captures a copy of the current telemetry values.
func (t *telemetry) snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		WritesTotal:      t.WritesTotal.Load(),
		ProcessedTotal:   t.ProcessedTotal.Load(),
		FailedTotal:      t.FailedTotal.Load(),
		PulsesTotal:      t.PulsesTotal.Load(),
		PulsesSaturated:  t.PulsesSaturated.Load(),
		WALWritesFailed:  t.WALWritesFailed.Load(),
		ProcessingTimeMs: t.ProcessingTimeMs.Load(),
	}
}
