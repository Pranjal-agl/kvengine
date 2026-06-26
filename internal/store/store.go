// Package store provides a durable in-memory key-value store backed by
// internal/wal. See docs/ARCHITECTURE.md "Core invariant: write-ahead" —
// every mutation hits the log (and is fsynced) before the in-memory map is
// updated.
package store

import (
	"encoding/json"
	"fmt"
	"sync"

	"kvengine/internal/wal"
)

// Store is safe for concurrent use. Current concurrency model is a single
// RWMutex guarding the map (see docs/ARCHITECTURE.md "Concurrency
// (current)" — this is a known, intentional placeholder pending Phase 2).
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	log  *wal.WAL
}

// Open opens (or creates) the store backed by a WAL file at walPath. On
// open, it replays the existing log to rebuild in-memory state before
// accepting new writes — this is the crash-recovery path.
func Open(walPath string) (*Store, error) {
	records, err := wal.Replay(walPath)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	data := make(map[string][]byte, len(records))
	for _, rec := range records {
		switch rec.Type {
		case wal.RecordPut:
			data[string(rec.Key)] = rec.Value
		case wal.RecordDelete:
			delete(data, string(rec.Key))
		}
	}

	log, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	return &Store{data: data, log: log}, nil
}

// Put durably writes key=value. The WAL append + fsync happens before the
// in-memory map is updated and before Put returns — see the package doc.
func (s *Store) Put(key, value []byte) error {
	if err := s.log.Append(wal.Record{Type: wal.RecordPut, Key: key, Value: value}); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.mu.Lock()
	s.data[string(key)] = append([]byte(nil), value...)
	s.mu.Unlock()
	return nil
}

// Delete durably removes key, if present. Deleting a non-existent key is a
// no-op but still gets logged (simplest correct behavior; an optimization
// to skip logging no-op deletes is possible but not worth the complexity
// yet).
func (s *Store) Delete(key []byte) error {
	if err := s.log.Append(wal.Record{Type: wal.RecordDelete, Key: key}); err != nil {
		return err
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.data, string(key))
	s.mu.Unlock()
	return nil
}

// Get returns the value for key and whether it was present.
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Len returns the current number of keys (mostly useful for tests/debugging).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot returns a JSON snapshot of the current in-memory state, used by
// the Raft layer to periodically compact the log (TakeSnapshot).
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

// RestoreSnapshot replaces in-memory state with the contents of a JSON
// snapshot. Used on startup if a Raft snapshot is newer than the WAL.
func (s *Store) RestoreSnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(data, &s.data)
}

// WAL returns the underlying write-ahead log. Used by the replication
// leader to stream WAL bytes to followers via wal.StreamFrom. Not for
// general use — callers should go through Store's Put/Get/Delete API.
func (s *Store) WAL() *wal.WAL {
	return s.log
}

// Close closes the underlying WAL.
func (s *Store) Close() error {
	return s.log.Close()
}
