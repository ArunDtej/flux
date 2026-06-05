package flux

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// SyncPolicy defines how the Write-Ahead Log flushes and syncs writes to disk.
type SyncPolicy int

const (
	// SyncAlways synchronizes writes to disk immediately. It utilizes Group Commit
	// to batch concurrent write requests and execute a single fsync, providing durability
	// with optimal concurrency.
	SyncAlways SyncPolicy = iota

	// SyncPeriodically flushes and calls fsync on a background timer.
	SyncPeriodically

	// SyncOS delegates flushing to the Operating System's file cache manager.
	SyncOS
)

// WALEntry represents a single JSON line record persisted in the WAL.
// It can represent either a write entry (ID/Key/Value) or a Tombstone delete marker.
type WALEntry[V any] struct {
	ID         uint64   `json:"id,omitempty"`
	Key        uint64   `json:"key,omitempty"`
	Value      V        `json:"value,omitempty"`
	Tombstones []uint64 `json:"tombstones,omitempty"`
}

type writeRequest[V any] struct {
	key     uint64
	value   V
	entryID uint64
	err     error
	done    chan struct{}
}

// BufferedWAL implements the WAL[V] interface with support for group-commit sync,
// O(1) Tombstone deletes, and amortized threshold compaction.
type BufferedWAL[V any] struct {
	mu           sync.Mutex
	filePath     string
	file         *os.File
	syncPolicy   SyncPolicy
	syncInterval time.Duration
	nextID       atomic.Uint64

	// Compaction metrics
	writeCount  int
	deleteCount int

	// Group Commit queue state
	queue  []*writeRequest[V]
	active bool

	// Background flushing state
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewBufferedWAL creates a new high-performance BufferedWAL using the specified sync policy.
func NewBufferedWAL[V any](filePath string, policy SyncPolicy, syncInterval time.Duration) (*BufferedWAL[V], error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	w := &BufferedWAL[V]{
		filePath:     filePath,
		file:         file,
		syncPolicy:   policy,
		syncInterval: syncInterval,
		stopChan:     make(chan struct{}),
	}

	if policy == SyncPeriodically {
		if w.syncInterval <= 0 {
			w.syncInterval = 10 * time.Millisecond
		}
		w.wg.Add(1)
		go w.backgroundFlushLoop()
	}

	return w, nil
}

// Write writes a key-value record to the WAL, using group commit if configured.
func (w *BufferedWAL[V]) Write(key uint64, value V) (uint64, error) {
	if w.syncPolicy == SyncAlways {
		return w.writeGroupCommit(key, value)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, errors.New("WAL file is closed")
	}

	id := w.nextID.Add(1)
	entry := WALEntry[V]{
		ID:    id,
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return 0, err
	}

	w.writeCount++
	return id, nil
}

// writeGroupCommit implements the group commit algorithm for SyncAlways.
func (w *BufferedWAL[V]) writeGroupCommit(key uint64, value V) (uint64, error) {
	req := &writeRequest[V]{
		key:   key,
		value: value,
		done:  make(chan struct{}),
	}

	w.mu.Lock()
	if w.file == nil {
		w.mu.Unlock()
		return 0, errors.New("WAL file is closed")
	}
	w.queue = append(w.queue, req)
	isLeader := !w.active
	if isLeader {
		w.active = true
	}
	w.mu.Unlock()

	if isLeader {
		w.leaderCommitLoop()
	} else {
		<-req.done
	}

	return req.entryID, req.err
}

func (w *BufferedWAL[V]) leaderCommitLoop() {
	for {
		w.mu.Lock()
		batch := w.queue
		w.queue = nil
		if len(batch) == 0 {
			w.active = false
			w.mu.Unlock()
			return
		}
		if w.file == nil {
			for _, req := range batch {
				req.err = errors.New("WAL file is closed")
				close(req.done)
			}
			w.active = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()

		var err error
		var buf []byte

		for _, req := range batch {
			req.entryID = w.nextID.Add(1)
			entry := WALEntry[V]{
				ID:    req.entryID,
				Key:   req.key,
				Value: req.value,
			}
			data, errMarshal := json.Marshal(entry)
			if errMarshal != nil {
				err = errMarshal
				break
			}
			buf = append(buf, data...)
			buf = append(buf, '\n')
		}

		if err == nil {
			w.mu.Lock()
			if w.file != nil {
				_, err = w.file.Write(buf)
				if err == nil {
					err = w.file.Sync()
				}
			} else {
				err = errors.New("WAL file is closed")
			}
			if err == nil {
				w.writeCount += len(batch)
			}
			w.mu.Unlock()
		}

		for _, req := range batch {
			req.err = err
			close(req.done)
		}
	}
}

// Remove logs a Tombstone delete marker in O(1) time and triggers compaction
// only when compaction thresholds are met.
func (w *BufferedWAL[V]) Remove(entryIDs []uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	entry := WALEntry[V]{
		Tombstones: entryIDs,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return err
	}

	if w.syncPolicy == SyncAlways {
		_ = w.file.Sync()
	}

	w.deleteCount += len(entryIDs)
	w.writeCount++ // Tombstone is itself a logged line

	// Amortized Compaction: trigger when we have accumulated a significant amount of deletes.
	// We'll set the threshold to 5000 deletes (or less for testing compatibility if needed,
	// but 5000 is excellent for general production use). Let's make it 5000.
	if w.deleteCount >= 5000 {
		if err := w.compactLocked(); err != nil {
			slog.Error("WAL compaction failed", "path", w.filePath, "error", err)
		}
	}

	return nil
}

// compactLocked rewrites the log file containing only active (non-tombstoned) records.
func (w *BufferedWAL[V]) compactLocked() error {
	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	activeEntries, err := w.readActiveLocked()
	if err != nil {
		return err
	}

	tempPath := w.filePath + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	var buf []byte
	for _, entry := range activeEntries {
		data, err := json.Marshal(entry)
		if err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			return err
		}
		buf = append(buf, data...)
		buf = append(buf, '\n')
	}

	if len(buf) > 0 {
		if _, err := tempFile.Write(buf); err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			return err
		}
	}

	_ = tempFile.Sync()
	_ = tempFile.Close()

	_ = w.file.Close()
	if err := os.Rename(tempPath, w.filePath); err != nil {
		// Attempt to reopen original file to restore handle
		w.file, _ = os.OpenFile(w.filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		return err
	}

	file, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	w.file = file

	w.deleteCount = 0
	w.writeCount = len(activeEntries)

	return nil
}

// Recover reads all active entries in the WAL and resets internal counters.
func (w *BufferedWAL[V]) Recover() (map[uint64][]V, uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, 0, errors.New("WAL file is closed")
	}

	entries, err := w.readActiveLocked()
	if err != nil {
		return nil, 0, err
	}

	var maxID uint64
	data := make(map[uint64][]V)
	for _, entry := range entries {
		data[entry.Key] = append(data[entry.Key], entry.Value)
		if entry.ID > maxID {
			maxID = entry.ID
		}
	}

	w.nextID.Store(maxID)
	w.writeCount = len(entries)
	w.deleteCount = 0
	return data, maxID, nil
}

func (w *BufferedWAL[V]) readActiveLocked() ([]WALEntry[V], error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var rawEntries []WALEntry[V]
	tombstones := make(map[uint64]struct{})

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry WALEntry[V]
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		if len(entry.Tombstones) > 0 {
			for _, id := range entry.Tombstones {
				tombstones[id] = struct{}{}
			}
		} else {
			rawEntries = append(rawEntries, entry)
		}
	}

	var active []WALEntry[V]
	for _, entry := range rawEntries {
		if _, deleted := tombstones[entry.ID]; !deleted {
			active = append(active, entry)
		}
	}

	_, _ = w.file.Seek(0, io.SeekEnd)
	return active, scanner.Err()
}

func (w *BufferedWAL[V]) backgroundFlushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.file != nil {
				_ = w.file.Sync()
			}
			w.mu.Unlock()
		case <-w.stopChan:
			w.mu.Lock()
			if w.file != nil {
				_ = w.file.Sync()
			}
			w.mu.Unlock()
			return
		}
	}
}

// Close performs a final compaction to tidy up the file, syncs writes,
// and releases underlying file resources.
func (w *BufferedWAL[V]) Close() error {
	if w.syncPolicy == SyncPeriodically {
		close(w.stopChan)
		w.wg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		// Clean up file if there are tombstones before closing
		if w.deleteCount > 0 {
			_ = w.compactLocked()
		}
		_ = w.file.Sync()
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}
