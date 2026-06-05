package flux

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// New creates a new Flux instance with the given configuration and registers it
// to the global background manager for Bootstrap startup.
//
// Parameters:
//   - name: A unique identifier for this Flux buffer instance.
//   - interval: The period at which the buffer will flush consolidated batches to the processor.
//   - shardingCount: Number of internal buckets/mutexes to distribute lock contention. Must be >= 1.
//   - maxBatchSize: Number of total added items across all shards that triggers an immediate flush. Set to 0 to disable size-based flushing.
//   - p: The Processor implementation to handle the flushed batches.
func New[V any](name string, interval time.Duration, shardingCount int, maxBatchSize int, p Processor[V], opts ...Option[V]) *Flux[V] {
	return NewWithManager(defaultManager, name, interval, shardingCount, maxBatchSize, p, opts...)
}

// NewWithManager creates a new Flux instance with the given configuration and registers it
// to the specified registry Manager.
func NewWithManager[V any](m *Manager, name string, interval time.Duration, shardingCount int, maxBatchSize int, p Processor[V], opts ...Option[V]) *Flux[V] {
	if shardingCount <= 0 {
		shardingCount = 1
	}

	f := &Flux[V]{
		Name:          name,
		Interval:      interval,
		ShardingCount: shardingCount,
		MaxBatchSize:  maxBatchSize,
		Processor:     p,
		MaxWorkers:    16, // Default maximum concurrent worker goroutines
		shards:        make([]*shard[V], shardingCount),
		trigger:       make(chan struct{}, 1),
	}
	f.enabled.Store(true)

	for i := 0; i < shardingCount; i++ {
		f.shards[i] = &shard[V]{
			data: make(map[uint64][]V),
		}
	}

	for _, opt := range opts {
		opt(f)
	}

	if f.walOpts != nil {
		wal, err := NewBufferedWAL[V](f.walOpts.filePath, f.walOpts.syncPolicy, f.walOpts.syncInterval)
		if err != nil {
			panic(fmt.Errorf("flux %q: failed to initialize WAL: %w", name, err))
		}
		f.WAL = wal
		if err := f.Recover(); err != nil {
			panic(fmt.Errorf("flux %q: failed to recover WAL: %w", name, err))
		}
	}

	m.Register(name, f)

	return f
}

// WithWAL configures the Flux instance to use a BufferedWAL with the specified file path, sync policy, and sync interval.
func WithWAL[V any](filePath string, policy SyncPolicy, syncInterval time.Duration) Option[V] {
	return func(f *Flux[V]) {
		f.walOpts = &walOptions{
			filePath:     filePath,
			syncPolicy:   policy,
			syncInterval: syncInterval,
		}
	}
}


// Add appends a value to the buffer for a specific key.
// If a Write-Ahead Log (WAL) is configured, it writes to the WAL first.
// It resolves the target shard based on the hash key (key % shardingCount)
// and handles thread-safe buffering. If the global count exceeds MaxBatchSize,
// a flush pulse is triggered immediately in the background.
//
// If the Flux instance is disabled, Add bypasses all buffering and WAL writing,
// and invokes the Processor synchronously.
func (f *Flux[V]) Add(key uint64, value V) {
	f.metrics.WritesTotal.Add(1)

	if !f.enabled.Load() {
		if f.Processor != nil {
			err := f.Processor.ProcessPulse(context.Background(), map[uint64][]V{key: {value}}, &f.State)
			if err != nil {
				f.metrics.FailedTotal.Add(1)
				slog.Error("Direct pass-through processor execution failed", "name", f.Name, "error", err)
			} else {
				f.metrics.ProcessedTotal.Add(1)
			}
		}
		return
	}

	var id uint64
	if f.WAL != nil {
		var err error
		id, err = f.WAL.Write(key, value)
		if err != nil {
			f.metrics.WALWritesFailed.Add(1)
			slog.Error("WAL write failed", "name", f.Name, "error", err)
		}
	}
	f.addInMemory(key, value, id)
}

func (f *Flux[V]) addInMemory(key uint64, value V, entryID uint64) {
	idx := key % uint64(f.ShardingCount)
	s := f.shards[idx]

	s.mu.Lock()
	s.data[key] = append(s.data[key], value)
	if entryID > 0 {
		s.walIDs = append(s.walIDs, entryID)
	}
	s.mu.Unlock()

	// Increment global count and check threshold
	count := f.count.Add(1)
	if f.MaxBatchSize > 0 && count >= int64(f.MaxBatchSize) {
		select {
		case f.trigger <- struct{}{}:
		default:
			// Trigger already pending
		}
	}
}

// Recover reads all persisted entries from the configured WAL (if present)
// and populates the internal buffer shards to restore the state before crash/restart.
func (f *Flux[V]) Recover() error {
	if f.WAL == nil {
		return nil
	}
	data, _, err := f.WAL.Recover()
	if err != nil {
		return err
	}
	for k, vals := range data {
		for _, val := range vals {
			f.addInMemory(k, val, 0)
		}
	}
	return nil
}

// SetMaxWorkers dynamically updates the limit of concurrent background workers processing batch pulses.
func (f *Flux[V]) SetMaxWorkers(limit int) {
	f.MaxWorkers = limit
	if f.sem != nil {
		f.sem.SetLimit(limit)
	}
}

// Enable dynamically enables buffered processing.
func (f *Flux[V]) Enable() {
	f.enabled.Store(true)
}

// Disable dynamically disables buffered processing, switching Flux to a direct pass-through mode.
func (f *Flux[V]) Disable() {
	f.enabled.Store(false)
}

// Metrics returns a snapshot of the current telemetry counters for the Flux instance.
func (f *Flux[V]) Metrics() MetricsSnapshot {
	return f.metrics.snapshot()
}
