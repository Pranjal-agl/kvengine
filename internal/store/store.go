// Package store provides a durable in-memory key-value store backed by
// internal/wal. See docs/ARCHITECTURE.md "Core invariant: write-ahead" —
// every mutation hits the log (and is fsynced) before the in-memory map is
// updated.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"kvengine/internal/wal"
)

// ErrClosed is returned by Put and Delete after Close has been called.
var ErrClosed = errors.New("store: closed")

// maxBatch bounds how many writes one fsync may cover. Large enough to
// amortise fsync under heavy concurrency, small enough that a single batch
// never holds the map write lock for long.
const maxBatch = 256

// Store is safe for concurrent use.
//
// Reads take an RWMutex read lock on the map. Writes go through a single
// committer goroutine (see commitLoop) that implements group commit: it
// drains every write that is waiting, appends them all to the WAL, fsyncs
// once, applies them to the map in log order, and only then wakes the
// writers. This gives two properties at once:
//
//   - Throughput: N concurrent writers share one fsync instead of paying
//     for N. fsync dominates write latency, so this is where the win is.
//   - Correctness: the map is updated in exactly the order records appear
//     in the WAL, so the state readers see before a crash is the state
//     replay rebuilds after it.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	log  *wal.WAL

	reqs       chan *writeReq
	closeMu    sync.RWMutex // held for read while sending on reqs; for write by Close
	closed     bool
	commitDone chan struct{}
}

type writeReq struct {
	rec  wal.Record
	done chan error
}

// Open opens (or creates) the store backed by a WAL file at walPath. On
// open, it replays the existing log to rebuild in-memory state before
// accepting new writes — this is the crash-recovery path. A torn tail left
// by a crash is truncated away so new writes land after the last valid
// record (see wal.Open).
func Open(walPath string) (*Store, error) {
	log, records, err := wal.OpenAndReplay(walPath)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	data := make(map[string][]byte, len(records))
	for _, rec := range records {
		applyRecord(data, rec)
	}
	s := &Store{
		data:       data,
		log:        log,
		reqs:       make(chan *writeReq, maxBatch),
		commitDone: make(chan struct{}),
	}
	go s.commitLoop()
	return s, nil
}

func applyRecord(data map[string][]byte, rec wal.Record) {
	switch rec.Type {
	case wal.RecordPut:
		data[string(rec.Key)] = rec.Value
	case wal.RecordDelete:
		delete(data, string(rec.Key))
	}
}

// Put durably writes key=value. Returns once the record is fsynced and
// visible to readers.
func (s *Store) Put(key, value []byte) error {
	return s.write(wal.Record{
		Type:  wal.RecordPut,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	})
}

// Delete durably removes key, if present. Deleting a non-existent key is a
// no-op but still gets logged (simplest correct behavior).
func (s *Store) Delete(key []byte) error {
	return s.write(wal.Record{Type: wal.RecordDelete, Key: append([]byte(nil), key...)})
}

func (s *Store) write(rec wal.Record) error {
	req := &writeReq{rec: rec, done: make(chan error, 1)}
	s.closeMu.RLock()
	if s.closed {
		s.closeMu.RUnlock()
		return ErrClosed
	}
	s.reqs <- req
	s.closeMu.RUnlock()
	return <-req.done
}

// commitLoop is the single writer. Each iteration commits one batch.
func (s *Store) commitLoop() {
	defer close(s.commitDone)
	batch := make([]*writeReq, 0, maxBatch)
	for first := range s.reqs {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < maxBatch {
			select {
			case r, ok := <-s.reqs:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		s.commit(batch)
	}
}

func (s *Store) commit(batch []*writeReq) {
	var err error
	for _, r := range batch {
		if err = s.log.Append(r.rec); err != nil {
			break
		}
	}
	if err == nil {
		err = s.log.Sync()
	}
	if err == nil {
		s.mu.Lock()
		for _, r := range batch {
			applyRecord(s.data, r.rec)
		}
		s.mu.Unlock()
	}
	// On error nothing is applied to memory and every writer in the batch
	// is told it failed. Some of the batch may still have reached disk;
	// as with any fsync failure, the caller cannot assume either outcome.
	for _, r := range batch {
		r.done <- err
	}
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

// snapshotEntry is one key/value pair in a snapshot. []byte fields encode
// as base64 in JSON, so arbitrary binary keys and values round-trip intact.
// (A JSON object keyed by string would replace invalid UTF-8 in keys with
// U+FFFD, silently corrupting binary keys.)
type snapshotEntry struct {
	K []byte `json:"k"`
	V []byte `json:"v"`
}

// Snapshot returns a JSON snapshot of the current in-memory state, used by
// the Raft layer to periodically compact the log (TakeSnapshot).
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]snapshotEntry, 0, len(s.data))
	for k, v := range s.data {
		entries = append(entries, snapshotEntry{K: []byte(k), V: v})
	}
	return json.Marshal(entries)
}

// RestoreSnapshot replaces the in-memory state with the contents of a
// snapshot produced by Snapshot. Keys not present in the snapshot are
// removed. Also accepts the legacy format (a JSON object of key → base64
// value) written by earlier versions.
//
// Only the in-memory state changes; the WAL is not rewritten. Callers that
// restore a snapshot (the Raft layer) are responsible for restoring it
// again on every restart, before replaying newer entries.
func (s *Store) RestoreSnapshot(data []byte) error {
	fresh := make(map[string][]byte)
	var entries []snapshotEntry
	if err := json.Unmarshal(data, &entries); err == nil {
		for _, e := range entries {
			fresh[string(e.K)] = e.V
		}
	} else if legacyErr := json.Unmarshal(data, &fresh); legacyErr != nil {
		return fmt.Errorf("store: restore snapshot: %w", err)
	}
	s.mu.Lock()
	s.data = fresh
	s.mu.Unlock()
	return nil
}

// WAL returns the underlying write-ahead log. Used by the replication
// leader to stream WAL bytes to followers via wal.StreamFrom. Not for
// general use — callers should go through Store's Put/Get/Delete API.
func (s *Store) WAL() *wal.WAL {
	return s.log
}

// Close stops accepting writes, waits for in-flight writes to commit, and
// closes the underlying WAL. Safe to call more than once.
func (s *Store) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.reqs)
	s.closeMu.Unlock()
	<-s.commitDone
	return s.log.Close()
}
