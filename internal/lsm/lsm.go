// Package lsm implements a Log-Structured Merge-tree (LSM) storage engine
// as an alternative to the WAL + in-memory map design in internal/store:
//
//   - Writes go to an in-memory MemTable.
//   - When the MemTable exceeds a size threshold, it's flushed to an
//     immutable, sorted SSTable file on disk (Level 0).
//   - A background goroutine compacts levels that are over budget (leveled
//     compaction, levels.go), dropping overwritten values and tombstones.
//     Writers don't wait for it unless L0 backs up to l0StopWrites files.
//
// Reads check MemTable → L0 (newest first) → one file per deeper level. The
// first entry found for a key wins, *including* a tombstone: a delete in a
// newer layer must hide any value in an older one.
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
	"sync/atomic"
	"time"

	"kvengine/internal/wal"
)

const (
	defaultMemTableSize = 4 * 1024 * 1024 // 4 MiB before flush
	maxL0Tables         = 4               // compact L0→L1 when L0 has this many SSTables
	// l0StopWrites: a write that flushes waits for background compaction
	// while L0 has this many files, so reads (which check every L0 file)
	// can't degrade without bound when writes outpace compaction. LevelDB
	// uses the same 4/12 thresholds.
	l0StopWrites = 3 * maxL0Tables
	// compactRetryDelay: after a failed background compaction (disk full,
	// I/O error), wait this long before retrying.
	compactRetryDelay = time.Second

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
	filter *bloom       // all keys in the table, tombstones included
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
	var hashes [][2]uint32
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
		h1, h2 := bloomHash(e.key)
		hashes = append(hashes, [2]uint32{h1, h2})
		off += sz
	}
	sst.filter = newBloom(hashes)
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
	if len(sst.index) == 0 || key < sst.minKey || key > sst.maxKey || !sst.filter.mayContain(key) {
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
	// TargetFileSize is the approximate size of each SSTable written by
	// compaction into levels 1 and below. Default 2 MiB.
	TargetFileSize int
	// LevelSizeBase is the byte budget of L1; each deeper level gets 10x
	// the previous one. Default 10 MiB.
	LevelSizeBase int
	// Logf receives background compaction errors. Default: discard (they
	// are also reported by Stats and to writers that stall on them).
	Logf func(format string, args ...any)

	// compactHook, if set, runs at the start of every compaction; an
	// error fails it. Tests use it to block or break compaction.
	compactHook func() error
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
// lookups, so they never wait on a writer's fsync, a flush's SSTable write,
// or a compaction's merge — those run without mu and take it only to
// install their result.
//
// cmu serialises compactions: the background goroutine and Compact /
// WaitForCompactions. Only compactions change levels 1 and below (flushes
// only add to L0), so a compaction holding cmu can plan against a copy of
// those levels and know it is still accurate when it installs its output.
//
// Lock order: wmu, then cmu, then mu; qmu is a leaf.
type Engine struct {
	wmu        sync.Mutex
	qmu        sync.Mutex
	wq         []*writeReq
	committing bool
	cmu        sync.Mutex
	mu         sync.RWMutex
	l0Cond     *sync.Cond // on &mu; signalled when L0 shrinks or bgErr changes
	dir        string
	opts       Options
	memtable   *MemTable
	// levels[0] is L0: flushed MemTables, oldest first, may overlap.
	// levels[1:] are sorted by key with disjoint ranges (levels.go).
	levels     [numLevels][]*SSTable
	compactPtr [numLevels]string // round-robin compaction cursor per level; cmu
	nextID     int
	closed     bool
	bgErr      error // last background compaction error, nil once one succeeds

	manifest      *wal.WAL // live-file log (levels.go)
	manifestEdits int

	bytesWritten atomic.Int64 // SSTable bytes written (flushes + compactions)
	compactions  atomic.Int64 // compactions completed
	writeStalls  atomic.Int64 // flushes that waited for compaction

	compactCh   chan struct{} // buffered(1): "there may be work"
	stopping    atomic.Bool   // set by Close; aborts an in-progress merge
	stopCh      chan struct{}
	compactDone chan struct{}

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

// Open opens the engine in dir, creating it if needed, and reloads the
// SSTables the MANIFEST lists as live (see levels.go); any other SSTable
// is a leftover from an interrupted flush or compaction and is deleted.
// WALs left by a crash are replayed in order into the MemTable, which is
// then flushed so recovery starts from a clean slate. Open starts the
// background compaction goroutine; Close stops it.
func Open(dir string) (*Engine, error) { return OpenWithOptions(dir, Options{}) }

// OpenWithOptions is Open with explicit options.
func OpenWithOptions(dir string, opts Options) (*Engine, error) {
	if opts.MemTableSize <= 0 {
		opts.MemTableSize = defaultMemTableSize
	}
	if opts.TargetFileSize <= 0 {
		opts.TargetFileSize = defaultTargetFileSize
	}
	if opts.LevelSizeBase <= 0 {
		opts.LevelSizeBase = defaultLevelSizeBase
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir: %w", err)
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	e := &Engine{
		dir: dir, opts: opts, memtable: newMemTable(),
		compactCh:   make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		compactDone: make(chan struct{}),
	}
	e.l0Cond = sync.NewCond(&e.mu)

	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("lsm: read dir: %w", err)
	}
	files := map[int]int{} // id → level
	var wals []int
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
		if n, _ := fmt.Sscanf(name, "l%d-%d.sst", &level, &id); n != 2 || sstName(level, id) != name || level >= numLevels {
			continue
		}
		files[id] = level
		if id > e.nextID {
			e.nextID = id
		}
	}
	if err := e.loadTables(files); err != nil {
		e.closeTables()
		return nil, err
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
	if err := e.flush(); err != nil {
		e.closeTables()
		return nil, err
	}
	go e.compactLoop()
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
	for _, r := range batch {
		e.memtable.set(r.en)
	}
	full := e.memtable.size >= e.opts.MemTableSize
	e.mu.Unlock()
	if !full {
		return nil
	}
	// The writes are durable and visible either way; a flush error is
	// still reported so callers learn the engine can't flush.
	if err := e.flush(); err != nil {
		return err
	}
	return e.waitForL0()
}

// waitForL0 stalls the writer (wmu is held, so all writers) while L0 has
// l0StopWrites or more files, until background compaction catches up.
// Returns the compaction error if compaction is failing, rather than
// waiting indefinitely.
func (e *Engine) waitForL0() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.levels[0]) < l0StopWrites {
		return nil
	}
	e.writeStalls.Add(1)
	for len(e.levels[0]) >= l0StopWrites && !e.closed && e.bgErr == nil {
		e.l0Cond.Wait()
	}
	if len(e.levels[0]) >= l0StopWrites && e.bgErr != nil {
		return fmt.Errorf("lsm: L0 is full and compaction is failing: %w", e.bgErr)
	}
	return nil
}

// Get looks up key. Search order: MemTable → L0 (newest first) → one file
// per deeper level. The first layer that has an entry for key decides the
// answer (a tombstone means "deleted").
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
	l0 := e.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		en, found, err := l0[i].get(k)
		if err != nil {
			return nil, false, err
		}
		if found {
			return result(en)
		}
	}
	for lvl := 1; lvl < numLevels; lvl++ {
		f := findFile(e.levels[lvl], k)
		if f == nil {
			continue
		}
		en, found, err := f.get(k)
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

// flush writes the MemTable to a new L0 SSTable, switches to a new WAL,
// deletes the WAL(s) the SSTable now covers, and wakes the compactor.
// Called with wmu held and mu not held (or during Open). Holding wmu keeps
// the MemTable unchanged while the SSTable is written without mu, so
// readers carry on reading the MemTable in the meantime.
//
// The SSTable is durable before any WAL is deleted, so a crash at any
// point leaves every write in at least one of them. A WAL that survives
// alongside its SSTable is replayed on Open; that only re-applies values
// the SSTable already has, in order, which is harmless.
func (e *Engine) flush() error {
	e.mu.Lock()
	var entries []entry
	var id int
	if len(e.memtable.data) > 0 {
		entries = e.memtable.sorted()
		e.nextID++
		id = e.nextID
	}
	e.mu.Unlock()

	var sst *SSTable
	if entries != nil {
		var err error
		if sst, err = writeSSTable(e.dir, 0, id, entries); err != nil {
			return err
		}
		e.bytesWritten.Add(sst.size)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if sst != nil {
		if err := e.applyEdit([]*SSTable{sst}, nil); err != nil {
			return discard([]*SSTable{sst}, err)
		}
		e.memtable = newMemTable()
	}
	// A new WAL is needed after a flush, on first open, and after
	// recovery (when several old WALs fed the MemTable).
	if sst != nil || e.wal == nil || len(e.walFiles) != 1 {
		if err := e.rotateWAL(); err != nil {
			return err
		}
	}
	e.signalCompaction()
	return nil
}

// rotateWAL opens a fresh WAL for the (now empty) MemTable and deletes the
// WAL files whose contents have been flushed. Called with mu held.
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

// Flush forces the current MemTable to disk immediately, regardless of size.
func (e *Engine) Flush() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.mu.RLock()
	closed, empty := e.closed, len(e.memtable.data) == 0
	e.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if empty {
		return nil
	}
	return e.flush()
}

// Close flushes the MemTable, stops background compaction (abandoning a
// merge in progress; its partial output is never installed) and releases
// all open files.
func (e *Engine) Close() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return nil
	}
	err := e.flush()

	e.stopping.Store(true)
	e.mu.Lock()
	e.closed = true
	e.l0Cond.Broadcast()
	e.mu.Unlock()
	close(e.stopCh)
	<-e.compactDone
	e.cmu.Lock() // wait out a Compact or WaitForCompactions call
	defer e.cmu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
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
	for lvl := range e.levels {
		for _, sst := range e.levels[lvl] {
			sst.close()
		}
	}
	if e.manifest != nil {
		e.manifest.Close()
	}
}

// Stats returns a snapshot of engine internals for debugging.
func (e *Engine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	files, bytes := make([]int, numLevels), make([]int64, numLevels)
	for lvl := range e.levels {
		files[lvl], bytes[lvl] = len(e.levels[lvl]), levelBytes(e.levels[lvl])
	}
	var bgErr string
	if e.bgErr != nil {
		bgErr = e.bgErr.Error()
	}
	return map[string]interface{}{
		"memtable_bytes":   e.memtable.size,
		"l0_files":         files[0],
		"l1_files":         files[1],
		"level_files":      files,
		"level_bytes":      bytes,
		"bytes_written":    e.bytesWritten.Load(),
		"compactions":      e.compactions.Load(),
		"write_stalls":     e.writeStalls.Load(),
		"compaction_error": bgErr,
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
