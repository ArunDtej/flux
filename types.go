package flux

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Runner defines the interface for background lifecycle management.
// Any component that runs a background process loop should implement this interface.
type Runner interface {
	// Run starts the execution loop of the runner, blocking until the context is canceled.
	Run(ctx context.Context)
}

// shard represents a synchronized segment of the buffer.
type shard[V any] struct {
	mu     sync.Mutex
	data   map[uint64][]V
	walIDs []uint64
}

// Processor defines the interface for handling captured data batches.
// Implementations must process the batch of events in a thread-safe manner.
type Processor[V any] interface {
	// ProcessPulse is invoked periodically or when the batch size threshold is reached.
	// batch maps keys (uint64 hashes) to a slice of values collected during the interval.
	// state is a persistent, thread-safe map that survives across pulses.
	ProcessPulse(ctx context.Context, batch map[uint64][]V, state *sync.Map) error
}

// WAL defines the interface for Write-Ahead Log implementations to provide persistence.
type WAL[V any] interface {
	// Write appends a key-value write operation to the log and returns its assigned unique ID.
	Write(key uint64, value V) (entryID uint64, err error)

	// Remove discards the specified log entries that have been successfully processed.
	Remove(entryIDs []uint64) error

	// Recover retrieves all saved log entries and the highest entryID found.
	Recover() (data map[uint64][]V, maxEntryID uint64, err error)
}

// workerSemaphore is a thread-safe semaphore that allows dynamic limit adjustments.
type workerSemaphore struct {
	mu       sync.Mutex
	limit    int
	acquired int
}

func newWorkerSemaphore(limit int) *workerSemaphore {
	if limit <= 0 {
		limit = 16
	}
	return &workerSemaphore{limit: limit}
}

func (s *workerSemaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acquired >= s.limit {
		return false
	}
	s.acquired++
	return true
}

func (s *workerSemaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquired--
	if s.acquired < 0 {
		s.acquired = 0
	}
}

func (s *workerSemaphore) SetLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 1
	}
	s.limit = limit
}

// Flux is a high-performance, sharded, buffered event processor using uint64 keys.
// It groups incoming events by key and buffers them, periodically flushing them
// to the Processor in consolidated batches.
type Flux[V any] struct {
	// Name is a unique identifier for the Flux instance, used in logging and metrics.
	Name string

	// Interval is the periodic duration at which the buffer is flushed.
	Interval time.Duration

	// ShardingCount specifies the number of internal mutex-protected shards.
	// Higher numbers reduce lock contention under high write throughput.
	ShardingCount int

	// MaxBatchSize is the threshold of buffered items that triggers an immediate flush
	// without waiting for the next periodic Interval. Set to 0 to disable size-based flushes.
	MaxBatchSize int

	// Processor is the component responsible for handling consolidated batches of events.
	Processor Processor[V]

	// MaxWorkers controls backpressure by limiting the number of concurrent goroutines
	// executing ProcessPulse. If all workers are saturated, a pulse is skipped and its data
	// is deferred to the next tick. Defaults to 16.
	MaxWorkers int

	// WAL is an optional Write-Ahead Log interface for persistence.
	WAL WAL[V]

	shards  []*shard[V]
	count   atomic.Int64
	trigger chan struct{}
	sem     *workerSemaphore
	enabled atomic.Bool
	metrics telemetry

	// State is a thread-safe map that can be used to maintain state (like locks or cursor offsets)
	// across multiple pulses.
	State sync.Map
}
