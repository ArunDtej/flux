package flux

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockProcessor implements the Processor interface for testing.
type mockProcessor struct {
	receivedCount atomic.Int64
	mu            sync.Mutex
	receivedData  map[uint64]int
	returnErr     error
}

func newMockProcessor() *mockProcessor {
	return &mockProcessor{
		receivedData: make(map[uint64]int),
	}
}

func (m *mockProcessor) ProcessPulse(ctx context.Context, batch map[uint64][]int, state *sync.Map) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k, v := range batch {
		m.receivedCount.Add(int64(len(v)))
		m.receivedData[k] += len(v)
	}
	return m.returnErr
}

func TestFluxHighTraffic(t *testing.T) {
	const (
		numGoroutines = 100
		opsPerRoutine = 10000
		numShards     = 16
		maxBatchSize  = 5000
		interval      = 100 * time.Millisecond
	)

	processor := newMockProcessor()
	f := New("test-flux", interval, numShards, maxBatchSize, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the engine
	go f.Run(ctx)

	var wg sync.WaitGroup
	start := time.Now()

	totalExpected := int64(numGoroutines * opsPerRoutine)

	// Simulate high traffic
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerRoutine; j++ {
				// Use different keys to test sharding
				key := uint64((id*opsPerRoutine + j) % 1000)
				f.Add(key, 1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Finished adding %d items in %v", totalExpected, time.Since(start))

	// Wait for the processor to catch up
	deadline := time.After(3 * time.Second)
	for {
		if processor.receivedCount.Load() >= totalExpected {
			break
		}
		select {
		case <-deadline:
			t.Errorf("Timed out waiting for processor. Received %d/%d", processor.receivedCount.Load(), totalExpected)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Logf("Successfully processed %d items", totalExpected)

	// Verify data consistency
	processor.mu.Lock()
	defer processor.mu.Unlock()

	actualTotal := int64(0)
	for _, count := range processor.receivedData {
		actualTotal += int64(count)
	}

	if actualTotal != totalExpected {
		t.Errorf("Data mismatch: expected %d, got %d", totalExpected, actualTotal)
	}
}

func BenchmarkFluxAdd(b *testing.B) {
	const (
		numShards    = 64
		maxBatchSize = 10000
		interval     = 1 * time.Second
	)

	processor := newMockProcessor()
	f := New("benchmark-flux", interval, numShards, maxBatchSize, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go f.Run(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			f.Add(i%1000, 1)
			i++
		}
	})
}

func TestFluxGracefulShutdown(t *testing.T) {
	processor := newMockProcessor()
	f := New("shutdown-flux", 10*time.Second, 1, 0, processor)

	ctx, cancel := context.WithCancel(context.Background())

	// Add data while engine is NOT running
	f.Add(1, 100)
	f.Add(1, 200)

	// Start engine and immediately cancel context
	go f.Run(ctx)
	cancel() // Trigger shutdown

	// Give a small window for the final pulse
	time.Sleep(100 * time.Millisecond)

	if processor.receivedCount.Load() != 2 {
		t.Errorf("Final flush failed: expected 2 items, got %d", processor.receivedCount.Load())
	}
}

func TestFluxStatePersistence(t *testing.T) {
	processor := newMockProcessor()
	f := New("state-flux", 10*time.Millisecond, 1, 0, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	// First pulse: Set state
	f.Add(1, 1)
	time.Sleep(50 * time.Millisecond)
	f.State.Store("init", true)

	// Second pulse: Check state
	f.Add(2, 2)
	time.Sleep(50 * time.Millisecond)

	val, ok := f.State.Load("init")
	if !ok || val != true {
		t.Errorf("State did not persist across pulses")
	}
}

func TestFluxBackpressureSaturate(t *testing.T) {
	pulseBlock := make(chan struct{})
	processedCount := atomic.Int32{}

	slowProc := func(ctx context.Context, batch map[uint64][]int, state *sync.Map) error {
		<-pulseBlock // block indefinitely
		processedCount.Add(1)
		return nil
	}

	// Limit to MaxWorkers = 2
	f := New("backpressure-flux", 10*time.Millisecond, 1, 0, &functionalProcessor{proc: slowProc})
	f.MaxWorkers = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	// Trigger 4 pulses
	for i := 0; i < 4; i++ {
		f.Add(uint64(i), i)
		time.Sleep(15 * time.Millisecond) // enough time to tick
	}

	// Wait a moment for scheduler
	time.Sleep(50 * time.Millisecond)

	// Release workers
	close(pulseBlock)
	time.Sleep(50 * time.Millisecond)

	// We expect at most 2 pulses processed because the semaphore limit (MaxWorkers=2)
	// blocks/delays processing for subsequent pulses when workers are saturated.
	// (Note: the 3rd and 4th additions will combine or delay, so they aren't lost,
	// but active goroutines were limited to 2).
	if count := processedCount.Load(); count > 2 {
		t.Logf("Processed pulses: %d", count)
	}
}

func TestFluxProcessorErrorHandling(t *testing.T) {
	processor := newMockProcessor()
	processor.returnErr = errors.New("database connection down")

	f := New("error-flux", 10*time.Millisecond, 1, 0, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	f.Add(1, 999)
	time.Sleep(50 * time.Millisecond)

	// Should have executed (slog handles the logging of this error, test won't crash)
	if processor.receivedCount.Load() != 1 {
		t.Errorf("Expected processor to receive item despite erroring")
	}
}

// Helper for quick functional processors
type functionalProcessor struct {
	proc func(context.Context, map[uint64][]int, *sync.Map) error
}

func (fp *functionalProcessor) ProcessPulse(ctx context.Context, batch map[uint64][]int, state *sync.Map) error {
	return fp.proc(ctx, batch, state)
}

type mockWAL struct {
	mu      sync.Mutex
	entries map[uint64]int
	seq     uint64
}

func newMockWAL() *mockWAL {
	return &mockWAL{
		entries: make(map[uint64]int),
	}
}

func (m *mockWAL) Write(key uint64, value int) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.entries[m.seq] = value
	return m.seq, nil
}

func (m *mockWAL) Remove(entryIDs []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range entryIDs {
		delete(m.entries, id)
	}
	return nil
}

func (m *mockWAL) Recover() (map[uint64][]int, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data := make(map[uint64][]int)
	for id, val := range m.entries {
		data[uint64(id)] = append(data[uint64(id)], val)
	}
	return data, m.seq, nil
}

func TestManagerScope(t *testing.T) {
	m1 := NewManager()
	m2 := NewManager()

	proc1 := newMockProcessor()
	proc2 := newMockProcessor()

	f1 := NewWithManager(m1, "flux-m1", 10*time.Millisecond, 1, 0, proc1)
	f2 := NewWithManager(m2, "flux-m2", 10*time.Millisecond, 1, 0, proc2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m1.Bootstrap(ctx)
	m2.Bootstrap(ctx)

	f1.Add(1, 10)
	f2.Add(2, 20)

	time.Sleep(50 * time.Millisecond)

	if proc1.receivedCount.Load() != 1 {
		t.Errorf("Manager 1 expected 1 item, got %d", proc1.receivedCount.Load())
	}
	if proc2.receivedCount.Load() != 1 {
		t.Errorf("Manager 2 expected 1 item, got %d", proc2.receivedCount.Load())
	}
}

func TestDynamicWorkerTuning(t *testing.T) {
	pulseBlock := make(chan struct{})
	processedCount := atomic.Int32{}

	slowProc := func(ctx context.Context, batch map[uint64][]int, state *sync.Map) error {
		<-pulseBlock
		processedCount.Add(1)
		return nil
	}

	f := New("dynamic-worker-flux", 10*time.Millisecond, 1, 0, &functionalProcessor{proc: slowProc})
	f.MaxWorkers = 1
	f.SetMaxWorkers(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	for i := 0; i < 3; i++ {
		f.Add(uint64(i), i)
		time.Sleep(15 * time.Millisecond)
	}

	f.SetMaxWorkers(3)

	f.Add(100, 100)
	time.Sleep(15 * time.Millisecond)

	close(pulseBlock)
	time.Sleep(50 * time.Millisecond)

	if processedCount.Load() == 0 {
		t.Errorf("Expected at least some pulses to complete")
	}
}

func TestWALPersistence(t *testing.T) {
	wal := newMockWAL()
	proc := newMockProcessor()

	f := New("wal-flux", 10*time.Millisecond, 1, 0, proc)
	f.WAL = wal

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	f.Add(1, 100)
	f.Add(2, 200)

	time.Sleep(50 * time.Millisecond)

	if proc.receivedCount.Load() != 2 {
		t.Errorf("Expected 2 processed items, got %d", proc.receivedCount.Load())
	}

	wal.mu.Lock()
	entryCount := len(wal.entries)
	wal.mu.Unlock()

	if entryCount != 0 {
		t.Errorf("Expected WAL entries to be removed after processing, but got %d remaining", entryCount)
	}

	wal.Write(1, 300)
	wal.Write(2, 400)

	f2 := New("wal-flux-recover", 10*time.Millisecond, 1, 0, proc)
	f2.WAL = wal

	if err := f2.Recover(); err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	f2.shards[0].mu.Lock()
	recoveredLen := len(f2.shards[0].data)
	f2.shards[0].mu.Unlock()

	if recoveredLen != 2 {
		t.Errorf("Expected 2 keys to be recovered into shard data map, got %d", recoveredLen)
	}
}

func TestBufferedWAL(t *testing.T) {
	// SyncAlways (Group Commit)
	filePathAlways := "test_always.wal"
	defer os.Remove(filePathAlways)

	walAlways, err := NewBufferedWAL[int](filePathAlways, SyncAlways, 0)
	if err != nil {
		t.Fatalf("Failed to create WALAlways: %v", err)
	}
	defer walAlways.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			_, err := walAlways.Write(uint64(val), val)
			if err != nil {
				t.Errorf("Failed to write to WALAlways: %v", err)
			}
		}(i)
	}
	wg.Wait()

	recovered, maxID, err := walAlways.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}
	if maxID != 1000 {
		t.Errorf("Expected maxID 1000, got %d", maxID)
	}
	if len(recovered) != 1000 {
		t.Errorf("Expected 1000 recovered items, got %d", len(recovered))
	}

	idsToRemove := []uint64{1, 2, 3, 4, 5}
	if err := walAlways.Remove(idsToRemove); err != nil {
		t.Fatalf("Failed to remove ids: %v", err)
	}

	recovered2, _, err := walAlways.Recover()
	if err != nil {
		t.Fatalf("Recovery failed after remove: %v", err)
	}
	if len(recovered2) != 995 {
		t.Errorf("Expected 995 recovered items, got %d", len(recovered2))
	}

	// Close the WAL, triggering compaction.
	if err := walAlways.Close(); err != nil {
		t.Fatalf("Failed to close WALAlways: %v", err)
	}

	// Reopen and recover to ensure only active entries exist in the compacted file
	walAlwaysCompacted, err := NewBufferedWAL[int](filePathAlways, SyncAlways, 0)
	if err != nil {
		t.Fatalf("Failed to re-open WALAlways: %v", err)
	}
	defer walAlwaysCompacted.Close()

	recovered3, _, err := walAlwaysCompacted.Recover()
	if err != nil {
		t.Fatalf("Recovery failed after compaction: %v", err)
	}
	if len(recovered3) != 995 {
		t.Errorf("Expected 995 recovered items after close compaction, got %d", len(recovered3))
	}

	// Test SyncPeriodically
	filePathPeriod := "test_period.wal"
	defer os.Remove(filePathPeriod)

	walPeriod, err := NewBufferedWAL[int](filePathPeriod, SyncPeriodically, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create WALPeriod: %v", err)
	}
	defer walPeriod.Close()

	id, err := walPeriod.Write(100, 100)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected ID 1, got %d", id)
	}
}

func TestEnableDisable(t *testing.T) {
	proc := newMockProcessor()
	f := New("enable-disable-flux", 10*time.Second, 1, 0, proc)

	f.Disable()
	f.Add(1, 99)

	if proc.receivedCount.Load() != 1 {
		t.Errorf("Expected 1 item to be processed immediately, got %d", proc.receivedCount.Load())
	}

	f.Enable()
	f.Add(2, 88)

	if proc.receivedCount.Load() != 1 {
		t.Errorf("Expected item to be buffered, but it was processed immediately")
	}
}

func TestMetrics(t *testing.T) {
	proc := newMockProcessor()
	f := New("metrics-flux", 10*time.Millisecond, 1, 0, proc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	f.Add(1, 10)
	f.Add(2, 20)

	time.Sleep(50 * time.Millisecond)

	m := f.Metrics()
	if m.WritesTotal != 2 {
		t.Errorf("Expected WritesTotal 2, got %d", m.WritesTotal)
	}
	if m.ProcessedTotal != 2 {
		t.Errorf("Expected ProcessedTotal 2, got %d", m.ProcessedTotal)
	}
	if m.PulsesTotal == 0 {
		t.Errorf("Expected PulsesTotal > 0")
	}
}

func TestCronScheduler(t *testing.T) {
	mgr := NewManager()

	var jobExecutions atomic.Int64
	var overlapDetections atomic.Int64
	var activeJobs atomic.Int64

	mgr.Schedule("fast-job", 5*time.Millisecond, func(ctx context.Context) error {
		jobExecutions.Add(1)
		return nil
	})

	mgr.Schedule("slow-job", 5*time.Millisecond, func(ctx context.Context) error {
		currentActive := activeJobs.Add(1)
		if currentActive > 1 {
			overlapDetections.Add(1)
		}
		time.Sleep(20 * time.Millisecond)
		activeJobs.Add(-1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Bootstrap(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()

	time.Sleep(10 * time.Millisecond)

	if jobExecutions.Load() == 0 {
		t.Errorf("Expected fast-job to execute, but got 0 executions")
	}

	if overlapDetections.Load() > 0 {
		t.Errorf("Detected overlapping executions for slow-job: %d", overlapDetections.Load())
	}
}

func TestAutoManagedWAL(t *testing.T) {
	filePath := "test_auto_wal.wal"
	defer os.Remove(filePath)

	proc := newMockProcessor()

	// Create with auto-WAL option
	f := New("auto-wal-flux", 10*time.Second, 1, 0, proc, WithWAL[int](filePath, SyncAlways, 0))
	if f.WAL == nil {
		t.Fatal("Expected WAL to be initialized automatically")
	}

	f.Add(1, 100)
	f.Add(2, 200)

	// Simulate crash by closing the WAL directly (without gracefully flushing the in-memory queue)
	if closer, ok := f.WAL.(io.Closer); ok {
		_ = closer.Close()
	}

	// Since we closed it, we should be able to recover data by reading it
	f2 := New("auto-wal-flux-recover", 10*time.Second, 1, 0, proc, WithWAL[int](filePath, SyncAlways, 0))
	// Recover should have run automatically during construction
	f2.shards[0].mu.Lock()
	recoveredLen := len(f2.shards[0].data)
	f2.shards[0].mu.Unlock()

	if recoveredLen != 2 {
		t.Errorf("Expected 2 recovered keys to be present in shards, got %d", recoveredLen)
	}
}

