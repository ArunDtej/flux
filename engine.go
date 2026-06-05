package flux

import (
	"context"
	"log/slog"
	"time"
)

// Run starts the heartbeat ticker loop for a Flux instance.
// It listens to the periodic ticker, manual triggers (e.g. from batch size threshold exceeded),
// and context cancellation.
//
// When the context is canceled, Run exits the loop after executing a synchronous flush
// (blocking pulse) of all remaining buffered items to ensure graceful shutdown and no data loss.
func (f *Flux[V]) Run(ctx context.Context) {
	if f.sem == nil {
		if f.MaxWorkers <= 0 {
			f.MaxWorkers = 16
		}
		f.sem = newWorkerSemaphore(f.MaxWorkers)
	}

	ticker := time.NewTicker(f.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.pulse(ctx, true) // Async
		case <-f.trigger:
			f.pulse(ctx, true) // Async
		case <-ctx.Done():
			f.pulse(ctx, false) // Sync flush on shutdown
			return
		}
	}
}

// pulse performs the atomic map-swap across all shards.
// If async is true, processing happens in a background goroutine and is limited by MaxWorkers.
// If async is false, processing happens immediately (blocking).
func (f *Flux[V]) pulse(ctx context.Context, async bool) {
	if f.sem == nil {
		if f.MaxWorkers <= 0 {
			f.MaxWorkers = 16
		}
		f.sem = newWorkerSemaphore(f.MaxWorkers)
	}

	f.metrics.PulsesTotal.Add(1)

	if async {
		if !f.sem.TryAcquire() {
			f.metrics.PulsesSaturated.Add(1)
			// All workers are busy processing previous pulses.
			// Skip this pulse; data remains in the shards and will be processed
			// on the next ticker/trigger when a worker is free.
			slog.Warn("Flux workers saturated, delaying pulse", "name", f.Name)
			return
		}
	}

	f.count.Store(0)
	var snapshots []map[uint64][]V
	var walIDs []uint64

	// Step 1: Atomic Map Swap (Synchronous & Fast)
	for _, s := range f.shards {
		s.mu.Lock()
		if len(s.data) == 0 {
			s.mu.Unlock()
			continue
		}

		// Capture the pointer, any associated WAL IDs, and reset the shard map
		snapshots = append(snapshots, s.data)
		s.data = make(map[uint64][]V)
		if len(s.walIDs) > 0 {
			walIDs = append(walIDs, s.walIDs...)
			s.walIDs = nil
		}
		s.mu.Unlock()
	}

	if len(snapshots) == 0 {
		if async {
			f.sem.Release()
		}
		return
	}

	// Step 2: Heavy Lifting
	processFn := func(snapBatch []map[uint64][]V, ids []uint64) {
		if async {
			defer f.sem.Release()
		}

		start := time.Now()
		consolidated := make(map[uint64][]V)
		var itemCount uint64

		// Merge all snapshots into one consistent view
		for _, snapshot := range snapBatch {
			for k, v := range snapshot {
				consolidated[k] = append(consolidated[k], v...)
				itemCount += uint64(len(v))
			}
		}

		if f.Processor != nil {
			err := f.Processor.ProcessPulse(ctx, consolidated, &f.State)
			f.metrics.ProcessingTimeMs.Add(uint64(time.Since(start).Milliseconds()))
			if err != nil {
				f.metrics.FailedTotal.Add(itemCount)
				slog.Error("Flux processor execution failed", "name", f.Name, "error", err)
			} else {
				f.metrics.ProcessedTotal.Add(itemCount)
				if f.WAL != nil && len(ids) > 0 {
					if err := f.WAL.Remove(ids); err != nil {
						slog.Error("Failed to remove entries from WAL", "name", f.Name, "error", err)
					}
				}
			}
		} else {
			f.metrics.ProcessingTimeMs.Add(uint64(time.Since(start).Milliseconds()))
			f.metrics.ProcessedTotal.Add(itemCount)
		}
	}

	if async {
		go processFn(snapshots, walIDs)
	} else {
		processFn(snapshots, walIDs)
	}
}
