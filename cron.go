package flux

import (
	"context"
	"log/slog"
	"time"
)

// JobFunc is the function signature for a cron job managed by the scheduler.
// If the function returns an error, it is logged, but scheduling continues.
type JobFunc func(ctx context.Context) error

// cronJob defines a periodic scheduled task.
type cronJob struct {
	Name     string
	Interval time.Duration
	Fn       JobFunc
}

// Schedule registers a new background job to be run periodically on the default manager.
// The job will start running when flux.Bootstrap is called.
// Schedule must be invoked before Bootstrap.
func Schedule(name string, interval time.Duration, fn JobFunc) {
	defaultManager.Schedule(name, interval, fn)
}

// Schedule registers a new background job to be run periodically on this manager.
// The job will start running when m.Bootstrap is called.
func (m *Manager) Schedule(name string, interval time.Duration, fn JobFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cronJobs = append(m.cronJobs, &cronJob{
		Name:     name,
		Interval: interval,
		Fn:       fn,
	})
}

// startScheduler starts all registered cron jobs in separate background goroutines.
func (m *Manager) startScheduler(ctx context.Context) {
	m.mu.RLock()
	jobs := make([]*cronJob, len(m.cronJobs))
	copy(jobs, m.cronJobs)
	m.mu.RUnlock()

	if len(jobs) == 0 {
		return
	}

	slog.Info("⏰ Flux Scheduler starting", "job_count", len(jobs))

	for _, job := range jobs {
		job := job // capture loop var
		go func() {
			slog.Info("▶️ Job scheduled", "name", job.Name, "interval", job.Interval.String())
			ticker := time.NewTicker(job.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					slog.Debug("Running job", "name", job.Name)
					start := time.Now()

					// Running job.Fn directly blocks this goroutine's loop.
					// This ensures that the next tick cannot run this job concurrently,
					// effectively preventing overlapping executions.
					if err := job.Fn(ctx); err != nil {
						slog.Error("Job failed", "name", job.Name, "duration", time.Since(start), "error", err)
					} else {
						slog.Info("Job completed", "name", job.Name, "duration", time.Since(start))
					}
				}
			}
		}()
	}
}
