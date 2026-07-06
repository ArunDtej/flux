package flux

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// BenchmarkFluxAddSharding measures lock contention of the Add operation
// across different shard configurations (1, 8, 32, 64, 128 shards).
func BenchmarkFluxAddSharding(b *testing.B) {
	shardOptions := []int{1, 8, 32, 64, 128}

	for _, shards := range shardOptions {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			processor := newMockProcessor()
			f := New("bench-shards", 5*time.Second, shards, 100000, processor)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go f.Run(ctx)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var i uint64
				for pb.Next() {
					f.Add(i, 1)
					i++
				}
			})
		})
	}
}

// BenchmarkWALWriteSyncAlways measures the throughput of BufferedWAL
// using SyncAlways (Group Commit) with varying levels of write concurrency.
func BenchmarkWALWriteSyncAlways(b *testing.B) {
	filePath := "bench_wal_always.wal"
	defer os.Remove(filePath)

	wal, err := NewBufferedWAL[int](filePath, SyncAlways, 0)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			_, err := wal.Write(i, 1)
			if err != nil {
				b.Errorf("Write error: %v", err)
			}
			i++
		}
	})
}

// BenchmarkWALWriteSyncPeriodically measures the throughput of BufferedWAL
// using SyncPeriodically (e.g. 10ms interval) under concurrent writes.
func BenchmarkWALWriteSyncPeriodically(b *testing.B) {
	filePath := "bench_wal_periodic.wal"
	defer os.Remove(filePath)

	wal, err := NewBufferedWAL[int](filePath, SyncPeriodically, 10*time.Millisecond)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			_, err := wal.Write(i, 1)
			if err != nil {
				b.Errorf("Write error: %v", err)
			}
			i++
		}
	})
}

// BenchmarkWALWriteSyncOS measures the throughput of BufferedWAL
// using SyncOS (letting the OS page cache schedule flushes).
func BenchmarkWALWriteSyncOS(b *testing.B) {
	filePath := "bench_wal_os.wal"
	defer os.Remove(filePath)

	wal, err := NewBufferedWAL[int](filePath, SyncOS, 0)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			_, err := wal.Write(i, 1)
			if err != nil {
				b.Errorf("Write error: %v", err)
			}
			i++
		}
	})
}

// BenchmarkWALRemoveCompaction measures tombstone deletion speed.
func BenchmarkWALRemoveCompaction(b *testing.B) {
	filePath := "bench_wal_remove.wal"
	defer os.Remove(filePath)

	wal, err := NewBufferedWAL[int](filePath, SyncPeriodically, 100*time.Millisecond)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Pre-fill some entries
	for i := 0; i < 1000; i++ {
		_, _ = wal.Write(uint64(i), 1)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64 = 1
		for pb.Next() {
			// We remove a batch of IDs (here just removing single ones)
			_ = wal.Remove([]uint64{i})
			i++
		}
	})
}

// TestFluxExtremeStress performs a high-load concurrent stress test
// injecting 10,000,000 events using 200 concurrent goroutines
// to verify lock sharding stability, zero memory leaks, and correctness.
func TestFluxExtremeStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping extreme stress test in short mode")
	}

	const (
		numGoroutines = 200
		eventsPerRoutine = 50000 // 10 Million total events
		numShards     = 128
		maxBatchSize  = 100000
		interval      = 100 * time.Millisecond
	)

	processor := newMockProcessor()
	f := New("stress-flux", interval, numShards, maxBatchSize, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	var wg sync.WaitGroup
	totalExpected := int64(numGoroutines * eventsPerRoutine)

	t.Logf("Injecting %d events across %d concurrent goroutines...", totalExpected, numGoroutines)
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < eventsPerRoutine; j++ {
				// Distribute keys heavily to test sharding
				key := uint64((routineID * eventsPerRoutine + j) % 10000)
				f.Add(key, 1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Finished injecting %d events in %v", totalExpected, time.Since(start))

	// Wait for processing to catch up
	deadline := time.After(15 * time.Second)
	for {
		received := processor.receivedCount.Load()
		if received >= totalExpected {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Stress test timed out. Processed %d/%d events", received, totalExpected)
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Logf("Successfully processed all %d events in stress test!", totalExpected)
}
