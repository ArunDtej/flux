# Flux

[![Go Reference](https://pkg.go.dev/badge/github.com/ArunDtej/flux.svg)](https://pkg.go.dev/github.com/ArunDtej/flux)
[![Go Report Card](https://goreportcard.com/badge/github.com/ArunDtej/flux)](https://goreportcard.com/report/github.com/ArunDtej/flux)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Flux is a high-performance, concurrent-safe, sharded, buffered event processing library written in Go. It allows you to group incoming high-throughput events or operations by a `uint64` key, buffer them thread-safely with minimal contention, and process them in consolidated batches periodically or when size thresholds are reached.

Flux is ideal for write-heavy applications, logging, analytical counters, metrics collection, and batched database operations where you want to trade a tiny delay for significantly reduced database/downstream network load.

---

## Key Features

- **High-Throughput Sharded Buffers:** Distributes event locks across multiple concurrent-safe buckets (shards) to minimize mutex contention under heavy write workloads.
- **Dual Triggering Mechanism:** Flushes batches based on a configurable time interval or when total buffered items reach a size threshold (`MaxBatchSize`).
- **Atomic Map Swapping:** Instantly resets active buffer maps on flush cycles, keeping lock duration minimal. Heavy consolidation and downstream processing are done asynchronously.
- **Dynamic Backpressure Control:** Restricts active concurrent batch worker goroutines using `MaxWorkers`. You can dynamically adjust the worker limit at runtime using `SetMaxWorkers(limit)`.
- **Thread-safe State Persistence:** Offers a built-in state map per buffer instance that persists across processing cycles.
- **Write-Ahead Log (WAL) Integration:** Supports plugging in custom WAL implementations for strict crash durability and recovery.
- **Isolated Registry Managers:** Provides `Manager` instances to scope and bootstrap groups of event loops independently, in addition to the package-level default manager.
- **Zero-Dependency Scheduler:** Includes a lightweight cron job scheduler to run recurring background tasks alongside event processors.

---

## Installation

```bash
go get github.com/ArunDtej/flux
```

---

## Quick Start

Here is a complete example of creating a batch user-click processor.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ArunDtej/flux"
)

// ClickEvent represents the user action we want to buffer.
type ClickEvent struct {
	UserID    string
	Timestamp time.Time
}

// ClickProcessor implements the flux.Processor interface.
type ClickProcessor struct{}

// ProcessPulse handles consolidated click events.
func (p *ClickProcessor) ProcessPulse(ctx context.Context, batch map[uint64][]ClickEvent, state *sync.Map) error {
	for userIDHash, clicks := range batch {
		fmt.Printf("Processing %d clicks for User Hash: %d\n", len(clicks), userIDHash)
	}
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize a new Flux processor registered to the default manager
	clickFlux := flux.New[ClickEvent](
		"clicks-processor",
		1*time.Second,
		8,
		500,
		&ClickProcessor{},
	)

	// 2. Start all registered Flux buffers
	flux.Bootstrap(ctx)

	// 3. Add events to the buffer
	userID := "user_12345"
	key := flux.Hash(userID) // Helper function for FNV-1a hashing

	clickFlux.Add(key, ClickEvent{
		UserID:    userID,
		Timestamp: time.Now(),
	})

	// Keep running to observe the periodic flush
	time.Sleep(2 * time.Second)
}
```

---

## Advanced Usage

### 1. Isolated Registry Managers
If you run multiple micro-services inside a single process, or want clean boundaries for testing, you can use custom instantiable `Manager` scopes instead of global package variables:

```go
package main

import (
	"context"
	"time"

	"github.com/ArunDtej/flux"
)

func main() {
	ctx := context.Background()
	mgr := flux.NewManager()

	// Create a Flux instance bound to this manager
	f := flux.NewWithManager(mgr, "scoped-flux", 1*time.Second, 4, 100, &MyProcessor{})

	// Bootstrap only the runners registered in this specific manager
	mgr.Bootstrap(ctx)
}
```

### 2. Write-Ahead Log (WAL) & Durability
To enforce durability across restarts or crashes, Flux provides a built-in, high-performance `BufferedWAL[V]` implementation. You can easily opt-in using the `WithWAL` functional option when creating your Flux instance. 

When you configure a WAL using `WithWAL`, Flux handles:
- **Initialization**: Automatically instantiates the underlying WAL.
- **Synchronous Recovery**: Reads active logs and populates internal shards on startup, guaranteeing recovery finishes before processing starts.
- **Lifecycle Cleanup**: Automatically closes/flushes the WAL when the runner context is cancelled.

The `BufferedWAL` supports three sync policies:
- **`SyncAlways` (Strict Durability with Group Commit):** Synchronizes every write to disk before returning success. Under concurrent load, writers are automatically grouped together, executing a single batch disk write and `fsync()`, offering both safety and extreme concurrency speed.
- **`SyncPeriodically` (Near-Strict Durability):** Batches writes and flushes/syncs to disk on a background ticker (e.g. every 10ms), keeping write performance near native-RAM speeds.
- **`SyncOS` (OS-managed Sync):** Writes to the OS page cache immediately, letting the operating system dictate the sync schedule.

To optimize write amplification, `BufferedWAL` utilizes **Tombstone-Based Remove** logic: calls to `Remove()` write a small tombstone entry to the log in $O(1)$ time rather than rewriting the entire file immediately. The file is only compacted once the accumulated deleted items cross a threshold (e.g., 5,000 entries) or when the WAL is closed.

```go
package main

import (
	"context"
	"time"

	"github.com/ArunDtej/flux"
)

func main() {
	ctx := context.Background()
	
	// Create a new Flux instance with an auto-managed WAL (SyncAlways/Group Commit)
	f := flux.New[MyEvent](
		"durable-flux",
		5*time.Second,
		16,
		1000,
		processor,
		flux.WithWAL[MyEvent]("events.wal", flux.SyncAlways, 0),
	)

	// Start the engine - recovery happened automatically during New()
	flux.Bootstrap(ctx)
}
```

### 3. Telemetry Metrics
Each Flux instance tracks real-time operation counters. Call `Metrics()` at any time to get a read-only snapshot:

```go
stats := fluxInstance.Metrics()
fmt.Printf("Writes: %d | Processed: %d | Failed: %d | Processing Time: %dms\n",
    stats.WritesTotal, stats.ProcessedTotal, stats.FailedTotal, stats.ProcessingTimeMs)
```

### 4. Enable/Disable Pass-Through
You can dynamically toggle Flux buffering on and off at runtime. When disabled, `Add()` bypasses all internal queues and writes directly and synchronously to the processor, facilitating maintenance modes, fallback rules, or testing:

```go
// Switch Flux to direct pass-through mode
fluxInstance.Disable()

// Re-enable buffered queue processing
fluxInstance.Enable()
```

### 5. Dynamic Worker Tuning
Adjust your concurrency bounds at runtime based on resource consumption or downstream load:

```go
// Set active concurrent background worker limit to 32
fluxInstance.SetMaxWorkers(32)
```

---

## Configuration & Tuning

To get the best performance out of Flux, adjust these parameters based on your ingestion rates:

| Field Name | Type | Description | Recommendation |
|---|---|---|---|
| `ShardingCount` | `int` | Number of internal mutex shards. | Use `8` to `64` for high concurrent throughput. |
| `Interval` | `time.Duration` | Periodic interval between flushes. | `1s` to `10s` depending on latency tolerance. |
| `MaxBatchSize` | `int` | Max buffered items before triggering a flush. | Set to `1000`-`5000` to flush early during traffic spikes. |
| `MaxWorkers` | `int` | Maximum concurrent background processors. | Defaults to `16`. Increase if batch processing takes a long time. |

---

## Built-in Cron Scheduler

Flux also includes a lightweight scheduler for periodic background jobs that prevents overlapping executions.

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/ArunDtej/flux"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register a periodic job
	flux.Schedule("db-cleanup", 5*time.Minute, func(ctx context.Context) error {
		slog.Info("Running database cleanup...")
		// Cleanup logic here
		return nil
	})

	// Starts both the scheduler and all Flux instance loops
	flux.Bootstrap(ctx)

	select {}
}
```

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
