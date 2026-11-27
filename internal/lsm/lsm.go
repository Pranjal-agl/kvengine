// Package lsm implements a Log-Structured Merge-tree (LSM) storage engine
// as an alternative to the WAL + in-memory map design in internal/store:
//
//   - Writes go to an in-memory MemTable.
//   - When the MemTable exceeds a size threshold, it's flushed to an
//     immutable, sorted SSTable file on disk (Level 0).
//   - When L0 has too many files, compaction merges L0 and L1 into a single
//     new L1 file, dropping overwritten values and tombstones.
//
// Reads check MemTable → L0 (newest first) → L1. The first entry found for
// a key wins, *including* a tombstone: a delete in a newer layer must hide
// any value in an older one.
//
// Durability: every write is appended to a write-ahead log and fsynced
// before it reaches the MemTable (Options.NoSync relaxes this), so an
// acknowledged write survives a crash even before it is flushed. Each
// MemTable generation has its own WAL file (wal-NNNNNN.log); once the
// MemTable is safely in an SSTable, its WAL is deleted. SSTables are
// written atomically (tmp → fsync → rename → fsync dir). Open reloads the
// SSTables and replays any WALs left by a crash. See docs/ARCHITECTURE.md
// "LSM Engine".
package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"kvengine/internal/wal"
)

const (
	defaultMemTableSize = 4 * 1024 * 1024 // 4 MiB before flush
	maxL0Tables         = 4               // compact L0→L1 when L0 has this many SSTables

	// indexInterval: the sparse index records the file offset of every
	// indexInterval-th entry. A lookup binary-searches the index, seeks to
	// the nearest preceding indexed entry, and scans at most indexInterval
	// entries. Memory cost is ~1/indexInterval of the keys.
	indexInterval = 16

	kindPut       byte = 1
	kindTombstone byte = 2

	entryHeaderSize = 9 // [1]kind [4]keyLen [4]valLen
)

// ErrClosed is returned by operations on a closed Engine.
var ErrClosed = errors.New("lsm: engine closed")

// entry is one key-value pair (or tombstone) in memory or on disk.
type entry struct {
	key     string
	value   string
	deleted bool
}

// MemTable is an in-memory write buffer. It is only accessed with the
// Engine's lock held, so it has no lock of its own.
type MemTable struct {
	data map[string]entry
	size int // approximate byte count
}

func newMemTable() *MemTable {
	return &MemTable{data: make(map[string]entry)}
}

func (m *MemTable) set(e entry) {
	m.size += len(e.key) + len(e.value) + entryHeaderSize
	m.data[e.key] = e
}

// sorted returns all entries in key order (for flushing to an SSTable).
func (m *MemTable) sorted() []entry {
	out := make([]entry, 0, len(m.data))
	for _, e := range m.data {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// SSTable is an immutable, sorted file of entries on disk.
//
// File format: a sequence of entries, sorted by key, each encoded as
//
//	[1]byte  kind     (1 = put, 2 = tombstone)
//	[4]byte  keyLen   (little-endian)
//	[4]byte  valLen   (little-endian; 0 for tombstones)
//	[N]byte  key
//	[M]byte  value
//
// The sparse index and key range are rebuilt in memory when the file is
// written or opened; they are not stored in the file.
type SSTable struct {
	id     int
	level  int
	path   string
	f      *os.File
	size   int64
	minKey string
	maxKey string
	index  []indexEntry // every indexInterval-th entry
}

type indexEntry struct {
	key string
	off int64
}

func sstName(level, id int) string { return fmt.Sprintf("l%d-%06d.sst", level, id) }

// writeSSTable atomically writes entries (which must be sorted) to a new
// SSTable file and returns it opened for reading.
func writeSSTable(dir string, level, id int, entries []entry) (*SSTable, error) {
	path := filepath.Join(dir, sstName(level, id))
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("lsm: create sstable: %w", err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		if err := writeEntry(w, e); err != nil {
			f.Close()
			return nil, fmt.Errorf("lsm: write sstable: %w", err)
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
	if err := os.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("lsm: rename sstable: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return openSSTable(path, level, id)
}

// openSSTable opens an existing SSTable file and builds its sparse index.
func openSSTable(path string, level, id int) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: open sstable: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: stat sstable: %w", err)
	}
	sst := &SSTable{id: id, level: level, path: path, f: f, size: info.Size()}
	r := bufio.NewReader(io.NewSectionReader(f, 0, sst.size))
	var off int64
	for n := 0; ; n++ {
		e, sz, err := readEntry(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("lsm: %s: corrupt at offset %d: %w", path, off, err)
		}
		if n == 0 {
			sst.minKey = e.key
		}
		sst.maxKey = e.key
		if n%indexInterval == 0 {
			sst.index = append(sst.index, indexEntry{key: e.key, off: off})
		}
		off += sz
	}
	return sst, nil
}

func writeEntry(w io.Writer, e entry) error {
	var hdr [entryHeaderSize]byte
	hdr[0] = kindPut
	if e.deleted {
		hdr[0] = kindTombstone
	}
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(e.key)))
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(e.value)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, e.key); err != nil {
		return err
	}
	_, err := io.WriteString(w, e.value)
	return err
}

// readEntry decodes one entry and returns it with its encoded size.
// Returns io.EOF only at a clean entry boundary.
func readEntry(r *bufio.Reader) (entry, int64, error) {
	var hdr [entryHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return entry{}, 0, err
		}
		return entry{}, 0, err // io.EOF at a boundary
	}
	kind := hdr[0]
	if kind != kindPut && kind != kindTombstone {
		return entry{}, 0, fmt.Errorf("bad entry kind %d", kind)
	}
	kl := binary.LittleEndian.Uint32(hdr[1:5])
	vl := binary.LittleEndian.Uint32(hdr[5:9])
	buf := make([]byte, int(kl)+int(vl))
	if _, err := io.ReadFull(r, buf); err != nil {
		return entry{}, 0, io.ErrUnexpectedEOF
	}
	return entry{
		key:     string(buf[:kl]),
		value:   string(buf[kl:]),
		deleted: kind == kindTombstone,
	}, int64(entryHeaderSize) + int64(kl) + int64(vl), nil
}

// get looks up key. found reports whether this table has an entry for key
// at all — a tombstone counts as found, with e.deleted set, so the caller
// stops searching older tables.
func (sst *SSTable) get(key string) (e entry, found bool, err error) {
	if len(sst.index) == 0 || key < sst.minKey || key > sst.maxKey {
		return entry{}, false, nil
	}
	// Last index entry with key <= target.
	i := sort.Search(len(sst.index), func(i int) bool { return sst.index[i].key > key }) - 1
	if i < 0 {
		return entry{}, false, nil
	}
	start := sst.index[i].off
	r := bufio.NewReader(io.NewSectionReader(sst.f, start, sst.size-start))
	for n := 0; n < indexInterval; n++ {
		e, _, err := readEntry(r)
		if err == io.EOF {
			return entry{}, false, nil
		}
		if err != nil {
			return entry{}, false, err
		}
		if e.key == key {
			return e, true, nil
		}
		if e.key > key {
			return entry{}, false, nil // sorted: key is not in this table
		}
	}
	return entry{}, false, nil
}

// readAll reads every entry from the SSTable in order (for compaction).
func (sst *SSTable) readAll() ([]entry, error) {
	r := bufio.NewReader(io.NewSectionReader(sst.f, 0, sst.size))
	var entries []entry
	for {
		e, _, err := readEntry(r)
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return nil, fmt.Errorf("lsm: read %s: %w", sst.path, err)
		}
		entries = append(entries, e)
	}
}

func (sst *SSTable) close() error { return sst.f.Close() }

// Options configure an Engine. The zero value is the safe default.
type Options struct {
	// MemTableSize is the approximate MemTable size in bytes that triggers
	// a flush to L0. Default 4 MiB.
	MemTableSize int
	// NoSync skips the per-write fsync of the WAL. Writes still reach the
	// OS before returning, so they survive a process crash, but the most
	// recent ones can be lost on power failure or kernel crash.
	NoSync bool
}

// Engine is the top-level LSM storage engine. Safe for concurrent use.
//
// Locking: writes use leader-based group commit, as in internal/store.
// Writers join wq; the first one to find no commit in progress leads: it
// takes the queued batch, writes it to the WAL with one fsync, applies it
// to the MemTable in queue order, wakes the batch's writers, and hands
// leadership to the next waiter. One leader at a time keeps WAL order equal
// to MemTable order (memory must never disagree with the log). wmu is held
// by the leader while committing and by Flush/Close, so they never
// interleave. mu guards the in-memory structures; readers hold it only for
// lookups, so they never wait on a writer's fsync.
// Lock order: wmu, then mu; qmu is a leaf.
type Engine struct {
	wmu        sync.Mutex
	qmu        sync.Mutex
	wq         []*writeReq
	committing bool
	mu         sync.RWMutex
	dir        string
	opts       Options
	memtable   *MemTable
	l0         []*SSTable // Level 0: recently flushed, oldest first, may overlap
	l1         []*SSTable // Level 1: output of the last compaction (0 or 1 file)
	nextID     int
	closed     bool

	wal      *wal.WAL // WAL for the current MemTable
	walFiles []string // WAL files whose contents are in the current MemTable
}

func walName(id int) string { return fmt.Sprintf("wal-%06d.log", id) }

type writeReq struct {
	en   entry
	done chan writeResult // buffered(1)
}

// writeResult tells a waiting writer either that its write was committed
// (lead=false, err) or that it is now the leader (lead=true).
type writeResult struct {
	err  error
	lead bool
}

// maxBatch bounds one group commit, so a writer arriving during a huge
// burst isn't stuck behind an unbounded batch.
const maxBatch = 1024

// Open opens the engine in dir, creating it if needed, and reloads any
// SSTables left by a previous run.
//
// Recovery relies on IDs being allocated in increasing order: a compaction
// always writes its L1 output with a higher ID than every file it merged,
// and makes it visible (rename) before deleting the inputs. So if a crash
// interrupts the deletions, any L0 or L1 file with an ID below the newest
// L1's is a leftover input that has already been merged; Open deletes it.
//
// WALs left by a crash are replayed in order into the MemTable, which is
// then flushed so recovery starts from a clean slate.
func Open(dir string) (*Engine, error) { return OpenWithOptions(dir, Options{}) }

// OpenWithOptions is Open with explicit options.
func OpenWithOptions(dir string, opts Options) (*Engine, error) {
	if opts.MemTableSize <= 0 {
		opts.MemTableSize = defaultMemTableSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir: %w", err)
	}
	e := &Engine{dir: dir, opts: opts, memtable: newMemTable()}

	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("lsm: read dir: %w", err)
	}
	type found struct{ level, id int }
	var files []found
	var wals []int
	newestL1 := -1
	for _, d := range names {
		name := d.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(dir, name)) // incomplete write from a crash
			continue
		}
		var walID int
		if n, _ := fmt.Sscanf(name, "wal-%d.log", &walID); n == 1 && walName(walID) == name {
			wals = append(wals, walID)
			if walID > e.nextID {
				e.nextID = walID
			}
			continue
		}
		var level, id int
		if n, _ := fmt.Sscanf(name, "l%d-%d.sst", &level, &id); n != 2 || sstName(level, id) != name {
			continue
		}
		files = append(files, found{level, id})
		if level == 1 && id > newestL1 {
			newestL1 = id
		}
		if id > e.nextID {
			e.nextID = id
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].id < files[j].id })

	for _, f := range files {
		path := filepath.Join(dir, sstName(f.level, f.id))
		if f.id < newestL1 {
			os.Remove(path) // already merged into the newest L1
			continue
		}
		sst, err := openSSTable(path, f.level, f.id)
		if err != nil {
			e.closeTables()
			return nil, err
		}
		if f.level == 1 {
			e.l1 = append(e.l1, sst)
		} else {
			e.l0 = append(e.l0, sst)
		}
	}

	// Replay WALs oldest first, so later writes win.
	sort.Ints(wals)
	for _, id := range wals {
		path := filepath.Join(dir, walName(id))
		recs, err := wal.Replay(path)
		if err != nil {
			e.closeTables()
			return nil, fmt.Errorf("lsm: replay %s: %w", path, err)
		}
		for _, r := range recs {
			en := entry{key: string(r.Key), value: string(r.Value), deleted: r.Type == wal.RecordDelete}
			e.memtable.set(en)
		}
		e.walFiles = append(e.walFiles, path)
	}
	// Flush what was recovered (or, if nothing, just retire the old WALs
	// and open a fresh one).
	if err := e.flushLocked(); err != nil {
		e.closeTables()
		return nil, err
	}
	return e, nil
}

// Put writes key=value. Writes always go to the MemTable; if the MemTable
// is full, it's flushed to L0.
func (e *Engine) Put(key, value []byte) error {
	return e.write(entry{key: string(key), value: string(value)})
}

// Delete records a tombstone for key. Same flush logic as Put.
func (e *Engine) Delete(key []byte) error {
	return e.write(entry{key: string(key), deleted: true})
}

func (e *Engine) write(en entry) error {
	req := &writeReq{en: en, done: make(chan writeResult, 1)}
	e.qmu.Lock()
	e.wq = append(e.wq, req)
	if e.committing {
		e.qmu.Unlock()
		r := <-req.done
		if !r.lead {
			return r.err
		}
		e.qmu.Lock()
	} else {
		e.committing = true
	}

	// Leader: our request is at the front of the queue.
	n := len(e.wq)
	if n > maxBatch {
		n = maxBatch
	}
	batch := e.wq[:n:n]
	e.wq = e.wq[n:]
	e.qmu.Unlock()

	e.wmu.Lock()
	err := e.commitBatch(batch)
	e.wmu.Unlock()
	for _, r := range batch {
		r.done <- writeResult{err: err}
	}

	e.qmu.Lock()
	if len(e.wq) > 0 {
		e.wq[0].done <- writeResult{lead: true}
	} else {
		e.committing = false
		e.wq = nil
	}
	e.qmu.Unlock()
	return (<-req.done).err
}

func (e *Engine) commitBatch(batch []*writeReq) error {
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return ErrClosed
	}

	// Write-ahead: the batch is in the log (and, by default, on disk)
	// before the MemTable — and therefore any reader — can see it.
	for _, r := range batch {
		rec := wal.Record{Type: wal.RecordPut, Key: []byte(r.en.key), Value: []byte(r.en.value)}
		if r.en.deleted {
			rec = wal.Record{Type: wal.RecordDelete, Key: []byte(r.en.key)}
		}
		if err := e.wal.Append(rec); err != nil {
			return fmt.Errorf("lsm: wal append: %w", err)
		}
	}
	sync := e.wal.Sync
	if e.opts.NoSync {
		sync = e.wal.Flush
	}
	if err := sync(); err != nil {
		return fmt.Errorf("lsm: wal sync: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range batch {
		e.memtable.set(r.en)
	}
	if e.memtable.size < e.opts.MemTableSize {
		return nil
	}
	// The writes are durable and visible either way; a flush error is
	// still reported so callers learn the engine can't flush.
	return e.flushLocked()
}

// Get looks up key. Search order: MemTable → L0 (newest first) → L1. The
// first layer that has an entry for key decides the answer.
func (e *Engine) Get(key []byte) ([]byte, bool) {
	v, ok, _ := e.GetErr(key)
	return v, ok
}

// GetErr is Get, but also reports I/O or corruption errors instead of
// treating them as "not found".
func (e *Engine) GetErr(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, false, ErrClosed
	}
	k := string(key)
	if en, ok := e.memtable.data[k]; ok {
		return result(en)
	}
	for i := len(e.l0) - 1; i >= 0; i-- {
		en, found, err := e.l0[i].get(k)
		if err != nil {
			return nil, false, err
		}
		if found {
			return result(en)
		}
	}
	for i := len(e.l1) - 1; i >= 0; i-- {
		en, found, err := e.l1[i].get(k)
		if err != nil {
			return nil, false, err
		}
		if found {
			return result(en)
		}
	}
	return nil, false, nil
}

func result(en entry) ([]byte, bool, error) {
	if en.deleted {
		return nil, false, nil
	}
	return []byte(en.value), true, nil
}

// flushLocked writes the MemTable to a new L0 SSTable, switches to a new
// WAL, deletes the WAL(s) the SSTable now covers, and compacts if L0 is
// full. Called with wmu and mu held (or during Open).
//
// The SSTable is durable before any WAL is deleted, so a crash at any
// point leaves every write in at least one of them. A WAL that survives
// alongside its SSTable is replayed on Open; that only re-applies values
// the SSTable already has, in order, which is harmless.
func (e *Engine) flushLocked() error {
	flushed := false
	if len(e.memtable.data) > 0 {
		e.nextID++
		sst, err := writeSSTable(e.dir, 0, e.nextID, e.memtable.sorted())
		if err != nil {
			return err
		}
		e.l0 = append(e.l0, sst)
		e.memtable = newMemTable()
		flushed = true
	}
	// A new WAL is needed after a flush, on first open, and after
	// recovery (when several old WALs fed the MemTable).
	if flushed || e.wal == nil || len(e.walFiles) != 1 {
		if err := e.rotateWAL(); err != nil {
			return err
		}
	}
	if len(e.l0) >= maxL0Tables {
		return e.compact()
	}
	return nil
}

// rotateWAL opens a fresh WAL for the (now empty) MemTable and deletes the
// WAL files whose contents have been flushed.
func (e *Engine) rotateWAL() error {
	e.nextID++
	path := filepath.Join(e.dir, walName(e.nextID))
	w, err := wal.Open(path)
	if err != nil {
		return fmt.Errorf("lsm: open wal: %w", err)
	}
	if e.wal != nil {
		e.wal.Close()
	}
	for _, old := range e.walFiles {
		os.Remove(old)
	}
	e.wal, e.walFiles = w, []string{path}
	return nil
}

// compact merges all L0 SSTables with L1 into one new L1 SSTable.
// Called with e.mu held.
//
// L1 is the bottom level, so tombstones are dropped here: once merged,
// there is nothing older left for them to shadow. The output file is
// written even if empty — its ID is what tells Open that every input with
// a lower ID is obsolete (see Open).
func (e *Engine) compact() error {
	merged := make(map[string]entry)
	// Oldest to newest, so newer entries overwrite older ones.
	for _, sst := range append(append([]*SSTable{}, e.l1...), e.l0...) {
		entries, err := sst.readAll()
		if err != nil {
			return fmt.Errorf("lsm: compact: %w", err)
		}
		for _, en := range entries {
			merged[en.key] = en
		}
	}
	entries := make([]entry, 0, len(merged))
	for _, en := range merged {
		if !en.deleted {
			entries = append(entries, en)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	e.nextID++
	out, err := writeSSTable(e.dir, 1, e.nextID, entries)
	if err != nil {
		return fmt.Errorf("lsm: compact write l1: %w", err)
	}
	old := append(e.l1, e.l0...)
	e.l0 = nil
	e.l1 = []*SSTable{out}
	for _, sst := range old {
		sst.close()
		os.Remove(sst.path)
	}
	return nil
}

// Flush forces the current MemTable to disk immediately, regardless of size.
func (e *Engine) Flush() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if len(e.memtable.data) == 0 {
		return nil
	}
	return e.flushLocked()
}

// Close flushes the MemTable and releases all open files.
func (e *Engine) Close() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	err := e.flushLocked()
	e.closed = true
	e.closeTables()
	if e.wal != nil {
		e.wal.Close()
		if err == nil {
			for _, f := range e.walFiles {
				os.Remove(f) // empty after the final flush
			}
		}
	}
	return err
}

func (e *Engine) closeTables() {
	for _, sst := range e.l0 {
		sst.close()
	}
	for _, sst := range e.l1 {
		sst.close()
	}
}

// Stats returns a snapshot of engine internals for debugging.
func (e *Engine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return map[string]interface{}{
		"memtable_bytes": e.memtable.size,
		"l0_files":       len(e.l0),
		"l1_files":       len(e.l1),
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("lsm: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("lsm: fsync dir: %w", err)
	}
	return nil
}
