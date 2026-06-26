// Package lsm implements a Log-Structured Merge-tree (LSM) storage engine
// as the stretch goal from docs/ROADMAP.md. This replaces the single
// write-ahead-log + in-memory map design from internal/store with a
// proper tiered storage structure:
//
//   - Writes go to an in-memory MemTable (a sorted map).
//   - When MemTable exceeds a size threshold, it's flushed to an SSTable
//     file on disk (Level 0). Sorted, immutable, binary-searched on read.
//   - Background compaction merges L0 SSTables into L1, eliminating
//     duplicate/deleted keys and bounding the number of files to search.
//
// This trades the WAL's simplicity for much higher write throughput and
// bounded read amplification — the core LSM tradeoff.
//
// See docs/ARCHITECTURE.md "Phase: LSM Engine" for the design rationale.
package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	memTableSizeLimit = 4 * 1024 * 1024 // 4 MiB before flush
	maxL0Tables       = 4               // compact L0→L1 when L0 has this many SSTables
	tombstone         = "\x00tombstone" // sentinel value for deleted keys
)

// entry is one key-value pair in memory or on disk.
type entry struct {
	key   string
	value string // tombstone sentinel means deleted
}

// MemTable is an in-memory sorted buffer. Writes go here first.
// When it reaches memTableSizeLimit, it's flushed to an SSTable.
type MemTable struct {
	mu   sync.RWMutex
	data map[string]string
	size int // approximate byte count
}

func newMemTable() *MemTable {
	return &MemTable{data: make(map[string]string)}
}

func (m *MemTable) put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.size += len(key) + len(value)
	m.data[key] = value
}

func (m *MemTable) delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.size += len(key) + len(tombstone)
	m.data[key] = tombstone
}

func (m *MemTable) get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	if !ok {
		return "", false
	}
	if v == tombstone {
		return "", false // deleted
	}
	return v, true
}

func (m *MemTable) bytesUsed() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// sorted returns all entries in sorted key order (for flushing to SSTable).
func (m *MemTable) sorted() []entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]entry, 0, len(m.data))
	for k, v := range m.data {
		out = append(out, entry{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// SSTable is an immutable, sorted file of key-value pairs on disk.
// Format (binary, little-endian):
//
//	For each entry:
//	  [4]byte keyLen
//	  [N]byte key
//	  [4]byte valLen
//	  [M]byte value   ("tombstone" sentinel for deletes)
type SSTable struct {
	path string
	// minKey and maxKey for bloom-filter-style skipping.
	minKey string
	maxKey string
}

func flushToSSTable(path string, entries []entry) (*SSTable, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: create sstable %s: %w", path, err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		if err := writeEntry(w, e); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: flush sstable: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: sync sstable: %w", err)
	}
	f.Close()

	sst := &SSTable{path: path}
	if len(entries) > 0 {
		sst.minKey = entries[0].key
		sst.maxKey = entries[len(entries)-1].key
	}
	return sst, nil
}

func writeEntry(w io.Writer, e entry) error {
	key, val := []byte(e.key), []byte(e.value)
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(val)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(val)
	return err
}

// get performs a linear scan of the SSTable for key. A real implementation
// would use a sparse index or bloom filter to avoid scanning files that
// can't contain the key; this is the correct-but-slow version.
func (sst *SSTable) get(key string) (string, bool, error) {
	// Key range check: skip file if key is outside [minKey, maxKey].
	if sst.minKey != "" && (key < sst.minKey || key > sst.maxKey) {
		return "", false, nil
	}

	f, err := os.Open(sst.path)
	if err != nil {
		return "", false, fmt.Errorf("lsm: open sstable: %w", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(r, hdr); err == io.EOF {
			break
		} else if err != nil {
			return "", false, err
		}
		keyLen := binary.LittleEndian.Uint32(hdr[0:4])
		valLen := binary.LittleEndian.Uint32(hdr[4:8])
		k := make([]byte, keyLen)
		v := make([]byte, valLen)
		if _, err := io.ReadFull(r, k); err != nil {
			return "", false, err
		}
		if _, err := io.ReadFull(r, v); err != nil {
			return "", false, err
		}
		if string(k) == key {
			if string(v) == tombstone {
				return "", false, nil // deleted
			}
			return string(v), true, nil
		}
	}
	return "", false, nil
}

// readAll reads every entry from the SSTable in order (for compaction).
func (sst *SSTable) readAll() ([]entry, error) {
	f, err := os.Open(sst.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var entries []entry
	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(r, hdr); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		kl := binary.LittleEndian.Uint32(hdr[0:4])
		vl := binary.LittleEndian.Uint32(hdr[4:8])
		k, v := make([]byte, kl), make([]byte, vl)
		io.ReadFull(r, k)
		io.ReadFull(r, v)
		entries = append(entries, entry{string(k), string(v)})
	}
	return entries, nil
}

// Engine is the top-level LSM storage engine.
type Engine struct {
	mu       sync.RWMutex
	dir      string
	memtable *MemTable
	l0       []*SSTable // Level 0: recently flushed, may overlap key ranges
	l1       []*SSTable // Level 1: compacted, non-overlapping key ranges
	nextID   int
}

func Open(dir string) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir: %w", err)
	}
	e := &Engine{
		dir:      dir,
		memtable: newMemTable(),
	}
	// TODO: on-restart recovery: scan dir for existing SSTables and reload
	// their metadata. Not implemented here — fresh open only for now.
	return e, nil
}

// Put writes key=value. Writes always go to the MemTable; if the MemTable
// is full, it's flushed to L0 first.
func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.memtable.put(string(key), string(value))
	return e.maybeFlush()
}

// Delete marks key as deleted (tombstone). Same flush logic as Put.
func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.memtable.delete(string(key))
	return e.maybeFlush()
}

// Get looks up key. Search order: MemTable → L0 (newest first) → L1.
// The first hit wins, including tombstones — a tombstone in the MemTable
// must shadow any value in L0/L1 (that's the entire point of tombstones).
func (e *Engine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	k := string(key)

	// MemTable first — check raw data so tombstones stop the search.
	e.memtable.mu.RLock()
	raw, inMemtable := e.memtable.data[k]
	e.memtable.mu.RUnlock()
	if inMemtable {
		if raw == tombstone {
			return nil, false // deleted in MemTable — do NOT fall through to L0
		}
		return []byte(raw), true
	}

	// L0 newest-first (L0 files may overlap key ranges, so we must
	// check all of them in insertion order reversed).
	for i := len(e.l0) - 1; i >= 0; i-- {
		v, ok, err := e.l0[i].get(k)
		if err == nil && ok {
			return []byte(v), true
		}
	}

	// L1 files are non-overlapping so we only need to check the right one.
	for _, sst := range e.l1 {
		v, ok, err := sst.get(k)
		if err == nil && ok {
			return []byte(v), true
		}
	}
	return nil, false
}

// maybeFlush flushes the MemTable to L0 if it's over the size limit, then
// triggers compaction if L0 has too many files. Called with e.mu held.
func (e *Engine) maybeFlush() error {
	if e.memtable.bytesUsed() < memTableSizeLimit {
		return nil
	}
	entries := e.memtable.sorted()
	e.nextID++
	path := filepath.Join(e.dir, fmt.Sprintf("l0-%06d.sst", e.nextID))
	sst, err := flushToSSTable(path, entries)
	if err != nil {
		return err
	}
	e.l0 = append(e.l0, sst)
	e.memtable = newMemTable()

	if len(e.l0) >= maxL0Tables {
		return e.compact()
	}
	return nil
}

// compact merges all L0 SSTables with L1 into a new set of L1 SSTables.
// This is the core of the LSM design: by merging sorted runs, we eliminate
// duplicate/deleted keys and reduce read amplification.
// Called with e.mu held.
func (e *Engine) compact() error {
	// Collect all entries from L1 first (oldest), then L0 oldest-to-newest.
	// Later iterations override earlier ones — newest value always wins.
	merged := make(map[string]string)
	for _, sst := range e.l1 {
		entries, err := sst.readAll()
		if err != nil {
			return fmt.Errorf("lsm: compact read l1: %w", err)
		}
		for _, e := range entries {
			merged[e.key] = e.value
		}
	}
	// L0 oldest-to-newest: newer files' values override older ones correctly.
	for i := 0; i < len(e.l0); i++ {
		entries, err := e.l0[i].readAll()
		if err != nil {
			return fmt.Errorf("lsm: compact read l0: %w", err)
		}
		for _, entry := range entries {
			merged[entry.key] = entry.value
		}
	}

	// Sort and filter out tombstones for the final L1 SSTable.
	var entries []entry
	for k, v := range merged {
		if v == tombstone {
			continue // dropped — key is deleted, no need to keep it
		}
		entries = append(entries, entry{k, v})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	// Write new L1 SSTable.
	var newL1 []*SSTable
	if len(entries) > 0 {
		e.nextID++
		path := filepath.Join(e.dir, fmt.Sprintf("l1-%06d.sst", e.nextID))
		sst, err := flushToSSTable(path, entries)
		if err != nil {
			return fmt.Errorf("lsm: compact write l1: %w", err)
		}
		newL1 = []*SSTable{sst}
	}

	// Delete old L0 and L1 files.
	for _, sst := range e.l0 {
		os.Remove(sst.path)
	}
	for _, sst := range e.l1 {
		os.Remove(sst.path)
	}

	e.l0 = nil
	e.l1 = newL1
	return nil
}

// Flush forces the current MemTable to disk immediately, regardless of size.
// Useful for tests and clean shutdown.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.memtable.bytesUsed() == 0 {
		return nil
	}
	entries := e.memtable.sorted()
	e.nextID++
	path := filepath.Join(e.dir, fmt.Sprintf("l0-%06d.sst", e.nextID))
	sst, err := flushToSSTable(path, entries)
	if err != nil {
		return err
	}
	e.l0 = append(e.l0, sst)
	e.memtable = newMemTable()
	return nil
}

// Stats returns a snapshot of engine internals for debugging.
func (e *Engine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return map[string]interface{}{
		"memtable_bytes": e.memtable.bytesUsed(),
		"l0_files":       len(e.l0),
		"l1_files":       len(e.l1),
	}
}
