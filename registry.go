package flux

import (
	"context"
	"sync"
)

// Manager orchestrates a registry of background Flux runners and cron jobs.
// It allows grouping background jobs and runners into isolated boundaries.
type Manager struct {
	mu       sync.RWMutex
	registry map[string]Runner
	cronJobs []*cronJob
}

// NewManager creates a new instantiable, isolated registry Manager.
func NewManager() *Manager {
	return &Manager{
		registry: make(map[string]Runner),
	}
}

// Register adds a Runner to the manager under the given name.
func (m *Manager) Register(name string, r Runner) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registry[name] = r
}

var defaultManager = NewManager()

// Bootstrap starts all registered Flux instances and cron jobs in the default manager.
// It should be called exactly once during application startup.
func Bootstrap(ctx context.Context) {
	defaultManager.Bootstrap(ctx)
}

// Bootstrap starts all registered Flux instances and cron jobs in this manager.
// It runs each Flux runner and cron scheduler in the background.
func (m *Manager) Bootstrap(ctx context.Context) {
	m.mu.RLock()
	runners := make([]Runner, 0, len(m.registry))
	for _, r := range m.registry {
		runners = append(runners, r)
	}
	m.mu.RUnlock()

	for _, r := range runners {
		go r.Run(ctx)
	}

	m.startScheduler(ctx)
}
