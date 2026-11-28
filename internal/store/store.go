// Package store provides a durable in-memory key-value store backed by
// internal/wal. See docs/ARCHITECTURE.md "Core invariant: write-ahead" —
// every mutation hits the log (and is fsynced) before the in-memory map is
// updated.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"kvengine/internal/wal"
)

// ErrClosed is returned by Put and Delete after Close has been called.
var ErrClosed = errors.New("store: closed")

// ErrReplicating is returned by Compact once the WAL has been handed to a
// replication leader (WAL()): followers resume by byte offset into the
// log, and rewriting it would invalidate those offsets.
var ErrReplicating = errors.New("store: cannot compact a WAL that is being replicated")

// Options configure a WAL-backed Store.
type Options struct {
	// The WAL is rewritten to just the live data (compacted) when it is
	// both larger than CompactMinBytes and more than CompactRatio times
	// the size of the live data. Defaults: 64 MiB and 4. A negative
	// CompactMinBytes disables automatic compaction; Compact still works.
	CompactMinBytes int64
	CompactRatio    float64
}

// recordOverhead is a WAL record's framing: CRC, length, type and the two
// length fields.
const recordOverhead = 4 + 4 + 1 + 4 + 4

// maxBatch bounds how many writes one fsync may cover. Large enough to
// amortise fsync under heavy concurrency, small enough that a single batch
// never holds the map write lock for long.
const maxBatch = 256

// Store is safe for concurrent use.
//
// Reads take an RWMutex read lock on the map. Writes use leader-based group
// commit (the pattern LevelDB uses): writers join a queue; whichever writer
// finds no commit in progress becomes the leader, takes everything queued
// as one batch, appends it to the WAL, fsyncs once, applies it to the map
// in log order, and wakes the batch's writers. If more writers queued up
// meanwhile, it hands leadership to the next one in line.
//
//   - Throughput: N concurrent writers share one fsync instead of paying
//     for N. fsync dominates write latency, so this is where the win is.
//   - Latency: an uncontended writer commits on its own goroutine with no
//     handoff, so a single writer pays no more than before.
//   - Correctness: only one leader runs at a time and it applies batches
//     to the map in exactly the order they hit the WAL, so the state
//     readers see before a crash is the state replay rebuilds after it.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	log  *wal.WAL

	wmu        sync.Mutex  // guards queue and committing
	queue      []*writeReq // writes waiting to be committed, in arrival order
	committing bool        // a leader is currently committing

	closeMu sync.RWMutex // read-held for the whole of each write; write-held by Close
	closed  bool

	opts        Options
	path        string
	liveBytes   int64       // encoded size of the live data, if rewritten; guarded by mu
	replicating atomic.Bool // WAL() was called; compaction is off
	compactMu   sync.Mutex  // held for a whole compaction, and by WAL()
	compactErr  error       // last automatic compaction failure (commit leader only)
}

type writeReq struct {
	rec     wal.Record
	compact bool // a Compact() request rather than a write
	done    chan writeResult
}

// writeResult is sent to a waiting writer either when its write has been
// committed (lead=false) or when it has become the leader (lead=true).
type writeResult struct {
	err  error
	lead bool
}

// Open opens (or creates) the store backed by a WAL file at walPath. On
// open, it replays the existing log to rebuild in-memory state before
// accepting new writes — this is the crash-recovery path. A torn tail left
// by a crash is truncated away so new writes land after the last valid
// record (see wal.Open).
func Open(walPath string) (*Store, error) { return OpenWithOptions(walPath, Options{}) }

// OpenWithOptions is Open with explicit options.
func OpenWithOptions(walPath string, opts Options) (*Store, error) {
	if opts.CompactMinBytes == 0 {
		opts.CompactMinBytes = 64 << 20
	}
	if opts.CompactRatio <= 0 {
		opts.CompactRatio = 4
	}
	os.Remove(compactTmp(walPath)) // an interrupted compaction; the old WAL is intact
	log, records, err := wal.OpenAndReplay(walPath)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	s := &Store{data: make(map[string][]byte, len(records)), log: log, opts: opts, path: walPath}
	for _, rec := range records {
		s.liveBytes += applyRecord(s.data, rec)
	}
	return s, nil
}

func compactTmp(path string) string { return path + ".compact.tmp" }

// NewMemory returns a Store with no write-ahead log: writes are applied to
// memory only and are lost on exit. Used as the Raft state machine, where
// the Raft log and snapshots already provide durability and a second WAL
// would only add an fsync per write and an ever-growing file.
func NewMemory() *Store {
	return &Store{data: make(map[string][]byte)}
}

// applyRecord applies rec to data and returns the change in live bytes
// (the size the live data would take in a freshly written WAL).
func applyRecord(data map[string][]byte, rec wal.Record) int64 {
	k := string(rec.Key)
	var delta int64
	if old, ok := data[k]; ok {
		delta -= int64(recordOverhead + len(k) + len(old))
	}
	switch rec.Type {
	case wal.RecordPut:
		data[k] = rec.Value
		delta += int64(recordOverhead + len(k) + len(rec.Value))
	case wal.RecordDelete:
		delete(data, k)
	}
	return delta
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

// Compact rewrites the WAL to contain only the live data, reclaiming the
// space of overwritten and deleted records. Writes queue behind it while
// it runs (it holds the commit leadership); reads are not blocked. The new
// log is fully written and fsynced before it atomically replaces the old
// one, so a crash at any point leaves one complete log.
func (s *Store) Compact() error {
	if s.path == "" {
		return nil // in-memory store: nothing to compact
	}
	return s.submit(&writeReq{compact: true, done: make(chan writeResult, 1)})
}

func (s *Store) write(rec wal.Record) error {
	return s.submit(&writeReq{rec: rec, done: make(chan writeResult, 1)})
}

func (s *Store) submit(req *writeReq) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return ErrClosed
	}

	s.wmu.Lock()
	s.queue = append(s.queue, req)
	if s.committing {
		// A leader is active; it (or a successor) will commit us, or hand
		// us leadership when our turn comes.
		s.wmu.Unlock()
		r := <-req.done
		if !r.lead {
			return r.err
		}
		s.wmu.Lock()
	} else {
		s.committing = true
	}

	// We are the leader and our request is at the front of the queue.
	n := len(s.queue)
	if n > maxBatch {
		n = maxBatch
	}
	batch := s.queue[:n:n]
	s.queue = s.queue[n:]
	s.wmu.Unlock()

	s.commit(batch) // fills every req.done in the batch, including ours

	s.wmu.Lock()
	if len(s.queue) > 0 {
		s.queue[0].done <- writeResult{lead: true}
	} else {
		s.committing = false
		s.queue = nil // drop the backing array once idle
	}
	s.wmu.Unlock()
	return (<-req.done).err
}

func (s *Store) commit(batch []*writeReq) {
	var err error
	compactAsked := false
	if s.log != nil {
		for _, r := range batch {
			if r.compact {
				compactAsked = true
				continue
			}
			if err = s.log.Append(r.rec); err != nil {
				break
			}
		}
		if err == nil {
			err = s.log.Sync()
		}
	}
	if err == nil {
		s.mu.Lock()
		for _, r := range batch {
			if !r.compact {
				s.liveBytes += applyRecord(s.data, r.rec)
			}
		}
		s.mu.Unlock()
	}

	// Compaction runs here because the commit leader is the only writer:
	// nothing can be appended between taking the snapshot and swapping
	// the log in.
	var cerr error
	if compactAsked || (err == nil && s.shouldCompact()) {
		cerr = s.compactLocked()
		if !compactAsked {
			s.compactErr = cerr // automatic: don't fail the writes over it
		}
	}

	// On a write error nothing is applied to memory and every writer in
	// the batch is told it failed. Some of the batch may still have
	// reached disk; as with any fsync failure, the caller cannot assume
	// either outcome.
	for _, r := range batch {
		if r.compact {
			if err != nil {
				r.done <- writeResult{err: err}
			} else {
				r.done <- writeResult{err: cerr}
			}
			continue
		}
		r.done <- writeResult{err: err}
	}
}

func (s *Store) shouldCompact() bool {
	if s.log == nil || s.opts.CompactMinBytes < 0 || s.replicating.Load() {
		return false
	}
	size := s.log.DurableOffset()
	s.mu.RLock()
	live := s.liveBytes
	s.mu.RUnlock()
	return size > s.opts.CompactMinBytes && float64(size) > s.opts.CompactRatio*float64(live)
}

// compactLocked rewrites the WAL as one Put per live key. Called by the
// commit leader.
func (s *Store) compactLocked() error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	if s.replicating.Load() {
		return ErrReplicating
	}
	tmp := compactTmp(s.path)
	os.Remove(tmp)
	w, err := wal.Open(tmp)
	if err != nil {
		return fmt.Errorf("store: compact: %w", err)
	}
	s.mu.RLock() // readers continue; writers are queued behind us
	for k, v := range s.data {
		if err = w.Append(wal.Record{Type: wal.RecordPut, Key: []byte(k), Value: v}); err != nil {
			break
		}
	}
	s.mu.RUnlock()
	if err == nil {
		err = w.Sync()
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, s.path)
	}
	if err == nil {
		err = syncDir(filepath.Dir(s.path))
	}
	if err != nil {
		os.Remove(tmp)
		return fmt.Errorf("store: compact: %w", err)
	}
	nw, err := wal.Open(s.path)
	if err != nil {
		// The new log is in place but can't be reopened for appends;
		// writes will fail until restart, which recovers from it.
		return fmt.Errorf("store: compact: reopen: %w", err)
	}
	s.mu.Lock() // WALSize reads s.log under mu
	old := s.log
	s.log = nw
	s.mu.Unlock()
	old.Close()
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WALSize returns the current size of the WAL in bytes (0 for in-memory
// stores). Mostly for tests and monitoring.
func (s *Store) WALSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.log == nil {
		return 0
	}
	return s.log.DurableOffset()
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
	var live int64
	for k, v := range fresh {
		live += int64(recordOverhead + len(k) + len(v))
	}
	s.mu.Lock()
	s.data = fresh
	s.liveBytes = live
	s.mu.Unlock()
	return nil
}

// WAL returns the underlying write-ahead log (nil for NewMemory stores).
// Used by the replication leader to stream WAL bytes to followers via
// wal.StreamFrom. Not for general use — callers should go through Store's
// Put/Get/Delete API.
func (s *Store) WAL() *wal.WAL {
	// Wait out any compaction in progress, then disable compaction:
	// followers' offsets into this file must stay valid.
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
	s.replicating.Store(true)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log
}

// Close stops accepting writes, waits for in-flight writes to commit, and
// closes the underlying WAL. Safe to call more than once.
func (s *Store) Close() error {
	s.closeMu.Lock() // waits for every in-flight write to return
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.log == nil {
		return nil
	}
	return s.log.Close()
}
