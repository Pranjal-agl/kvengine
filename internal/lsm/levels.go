package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"kvengine/internal/wal"
)

// Leveled compaction (the LevelDB design).
//
// L0 holds flushed MemTables; its files may overlap, so a lookup checks
// them all, newest first. Levels 1..numLevels-1 each hold files with
// disjoint key ranges, sorted by key, each about TargetFileSize bytes, and
// each level may hold about levelMultiplier times as many bytes as the one
// above (L1: LevelSizeBase). A lookup checks at most one file per level.
//
// Compaction runs on a background goroutine (see "compaction" below):
//   - When L0 reaches maxL0Tables files, all of L0 is merged with the L1
//     files it overlaps into new L1 files.
//   - When level i (i >= 1) exceeds its size budget, one of its files —
//     chosen round-robin through the key space — is merged with the
//     level i+1 files it overlaps into new level i+1 files.
//
// Each compaction therefore rewrites one file's worth of data plus the
// ~levelMultiplier files below it, instead of the whole level, keeping
// write amplification roughly proportional to the number of levels rather
// than to the total data size.
//
// A tombstone is dropped during compaction only when no deeper level has a
// file whose range covers its key; otherwise it must keep shadowing the
// older value down there.
//
// The MANIFEST records which files are live, as a log of atomic edits
// (files added, files deleted), each one a checksummed internal/wal record.
// The order of every change is: write new SSTables (durable) → append the
// edit (durable) → delete the old files. On Open, the MANIFEST decides
// what is live; any other .sst file is a leftover from a crash and is
// removed.

const (
	numLevels       = 7
	levelMultiplier = 10

	defaultTargetFileSize = 2 << 20  // 2 MiB per file in L1+
	defaultLevelSizeBase  = 10 << 20 // L1 budget: 10 MiB

	manifestName = "MANIFEST"
	// The manifest is rewritten from scratch (just the live set) once it
	// holds this many edits.
	maxManifestEdits = 1000
)

func (e *Engine) maxBytes(level int) int64 {
	b := int64(e.opts.LevelSizeBase)
	for i := 1; i < level; i++ {
		b *= levelMultiplier
	}
	return b
}

func levelBytes(files []*SSTable) int64 {
	var n int64
	for _, f := range files {
		n += f.size
	}
	return n
}

// ---- manifest ----

type edit struct {
	adds []*SSTable
	dels []*SSTable
}

func encodeEdit(adds, dels []*SSTable) []byte {
	b := binary.AppendUvarint(nil, uint64(len(adds)))
	for _, f := range adds {
		b = binary.AppendUvarint(b, uint64(f.level))
		b = binary.AppendUvarint(b, uint64(f.id))
	}
	b = binary.AppendUvarint(b, uint64(len(dels)))
	for _, f := range dels {
		b = binary.AppendUvarint(b, uint64(f.id))
	}
	return b
}

// replayManifest returns the live files (id → level) recorded in path.
func replayManifest(path string) (map[int]int, error) {
	recs, err := wal.Replay(path)
	if err != nil {
		return nil, err
	}
	live := map[int]int{}
	for i, r := range recs {
		b := r.Value
		next := func() (int, bool) {
			v, n := binary.Uvarint(b)
			if n <= 0 {
				return 0, false
			}
			b = b[n:]
			return int(v), true
		}
		nAdd, ok := next()
		for j := 0; ok && j < nAdd; j++ {
			var level, id int
			if level, ok = next(); ok {
				if id, ok = next(); ok {
					live[id] = level
				}
			}
		}
		var nDel int
		if ok {
			nDel, ok = next()
		}
		for j := 0; ok && j < nDel; j++ {
			var id int
			if id, ok = next(); ok {
				delete(live, id)
			}
		}
		if !ok {
			return nil, fmt.Errorf("lsm: manifest record %d is malformed", i)
		}
	}
	return live, nil
}

// writeManifest replaces the manifest with a single edit adding every live
// file. Called with mu held (or during Open).
func (e *Engine) writeManifest() error {
	path := filepath.Join(e.dir, manifestName)
	tmp := path + ".tmp"
	os.Remove(tmp)
	w, err := wal.Open(tmp)
	if err != nil {
		return err
	}
	var all []*SSTable
	for lvl := range e.levels {
		all = append(all, e.levels[lvl]...)
	}
	err = w.Append(wal.Record{Type: wal.RecordPut, Key: []byte("edit"), Value: encodeEdit(all, nil)})
	if err == nil {
		err = w.Sync()
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("lsm: write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("lsm: install manifest: %w", err)
	}
	if err := syncDir(e.dir); err != nil {
		return err
	}
	if e.manifest != nil {
		e.manifest.Close()
	}
	e.manifest, err = wal.Open(path)
	e.manifestEdits = 0
	return err
}

// applyEdit durably records adds/dels in the manifest, updates the
// in-memory levels, then deletes the dropped files. Called with mu held.
func (e *Engine) applyEdit(adds, dels []*SSTable) error {
	rec := wal.Record{Type: wal.RecordPut, Key: []byte("edit"), Value: encodeEdit(adds, dels)}
	if err := e.manifest.Append(rec); err != nil {
		return fmt.Errorf("lsm: manifest append: %w", err)
	}
	if err := e.manifest.Sync(); err != nil {
		return fmt.Errorf("lsm: manifest sync: %w", err)
	}
	e.manifestEdits++

	gone := map[*SSTable]bool{}
	for _, f := range dels {
		gone[f] = true
	}
	for lvl := range e.levels {
		kept := e.levels[lvl][:0]
		for _, f := range e.levels[lvl] {
			if !gone[f] {
				kept = append(kept, f)
			}
		}
		e.levels[lvl] = kept
	}
	for _, f := range adds {
		e.levels[f.level] = append(e.levels[f.level], f)
	}
	sort.Slice(e.levels[0], func(i, j int) bool { return e.levels[0][i].id < e.levels[0][j].id })
	for lvl := 1; lvl < numLevels; lvl++ {
		files := e.levels[lvl]
		sort.Slice(files, func(i, j int) bool { return files[i].minKey < files[j].minKey })
	}
	for _, f := range dels {
		f.close()
		os.Remove(f.path)
	}
	if e.manifestEdits >= maxManifestEdits {
		return e.writeManifest()
	}
	return nil
}

// ---- compaction ----

// A compaction runs in three steps, so that neither readers nor writers
// wait on its I/O:
//
//  1. plan (cmu held, mu read-locked): choose the input files and copy the
//     levels below the output level, for the tombstone rule;
//  2. merge (cmu held, mu not held): read the inputs and write the output
//     files. SSTables are immutable and are only closed by a compaction's
//     own edit, so reading them unlocked is safe;
//  3. install (cmu and mu held): the manifest edit swaps outputs for inputs.
//
// Flushes may add L0 files during step 2. That's fine: they are newer than
// every input, stay in L0 (above the output), and the edit removes inputs
// by identity, not "all of L0".

var errCompactionAborted = errors.New("lsm: compaction abandoned: engine closing")

type compaction struct {
	upper    []*SSTable // newer inputs, oldest first
	lower    []*SSTable // older inputs, at outLevel
	outLevel int
	deeper   [numLevels][]*SSTable // copies of the levels below outLevel
}

// signalCompaction wakes the background compactor. Never blocks.
func (e *Engine) signalCompaction() {
	select {
	case e.compactCh <- struct{}{}:
	default:
	}
}

// compactLoop is the background compactor: on each signal it compacts
// until every level is within budget. A failed compaction is retried after
// compactRetryDelay; the error is kept in bgErr until one succeeds, so
// stalled writers and Stats can see it.
func (e *Engine) compactLoop() {
	defer close(e.compactDone)
	for {
		select {
		case <-e.stopCh:
			return
		case <-e.compactCh:
		}
		for !e.stopping.Load() {
			e.cmu.Lock()
			did, err := e.compactOnce()
			e.cmu.Unlock()
			if errors.Is(err, errCompactionAborted) {
				return
			}
			e.mu.Lock()
			e.bgErr = err
			e.l0Cond.Broadcast()
			e.mu.Unlock()
			if err != nil {
				e.opts.Logf("lsm: background compaction failed (retrying in %v): %v", compactRetryDelay, err)
				select {
				case <-e.stopCh:
					return
				case <-time.After(compactRetryDelay):
				}
				continue
			}
			if !did {
				break
			}
		}
	}
}

// compactOnce runs the most urgent compaction, if any is needed. Called
// with cmu held.
func (e *Engine) compactOnce() (bool, error) {
	e.mu.RLock()
	c := e.pickCompaction()
	e.mu.RUnlock()
	if c == nil {
		return false, nil
	}
	return true, e.runCompaction(c)
}

// pickCompaction plans the next compaction: the level furthest over its
// budget, scored as in LevelDB — L0 by file count (files/maxL0Tables),
// deeper levels by bytes (bytes/budget). Returns nil if no score reaches 1.
//
// Scoring matters once compaction runs concurrently with writes: if L0
// always went first, a steady write load would keep L0 at maxL0Tables and
// L1 would never be compacted down, growing far past its budget until
// every L0 merge rewrote all of it (write amplification went from 13.7x to
// 34x on the 1.8M-key benchmark before this).
//
// Called with cmu held and mu at least read-locked.
func (e *Engine) pickCompaction() *compaction {
	best, bestScore := -1, 1.0
	if s := float64(len(e.levels[0])) / maxL0Tables; s >= bestScore {
		best, bestScore = 0, s
	}
	for i := 1; i < numLevels-1; i++ {
		if s := float64(levelBytes(e.levels[i])) / float64(e.maxBytes(i)); s > bestScore {
			best, bestScore = i, s
		}
	}
	switch {
	case best < 0:
		return nil
	case best == 0:
		return e.planL0()
	default:
		return e.planLevel(best)
	}
}

func (e *Engine) planL0() *compaction {
	upper := append([]*SSTable(nil), e.levels[0]...) // oldest first
	lo, hi := keyRange(upper)
	return e.plan(upper, e.overlapping(1, lo, hi), 1)
}

func (e *Engine) planLevel(lvl int) *compaction {
	files := e.levels[lvl]
	// Round-robin through the key space so every part of the level gets
	// compacted in turn: the first file past the last one compacted, or
	// the first file once the end is reached.
	pick := files[0]
	for _, f := range files {
		if f.minKey > e.compactPtr[lvl] {
			pick = f
			break
		}
	}
	e.compactPtr[lvl] = pick.maxKey
	return e.plan([]*SSTable{pick}, e.overlapping(lvl+1, pick.minKey, pick.maxKey), lvl+1)
}

func (e *Engine) plan(upper, lower []*SSTable, outLevel int) *compaction {
	c := &compaction{upper: upper, lower: lower, outLevel: outLevel}
	for i := outLevel + 1; i < numLevels; i++ {
		c.deeper[i] = append([]*SSTable(nil), e.levels[i]...)
	}
	return c
}

// Compact merges all of L0 into L1, then runs whatever compactions that
// makes necessary, before returning. Background compaction makes calling
// it unnecessary; it is for tests and tools that want a settled tree.
func (e *Engine) Compact() error {
	e.cmu.Lock()
	defer e.cmu.Unlock()
	e.mu.RLock()
	closed := e.closed
	var c *compaction
	if len(e.levels[0]) > 0 {
		c = e.planL0()
	}
	e.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if c != nil {
		if err := e.runCompaction(c); err != nil {
			return err
		}
	}
	return e.settle()
}

// WaitForCompactions returns once no level needs compacting, running any
// outstanding compactions itself rather than waiting for the background
// goroutine. For tests and benchmarks that check the tree's shape.
func (e *Engine) WaitForCompactions() error {
	e.cmu.Lock()
	defer e.cmu.Unlock()
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return e.settle()
}

// settle compacts until nothing is over budget. Called with cmu held.
func (e *Engine) settle() error {
	for {
		did, err := e.compactOnce()
		if err != nil || !did {
			return err
		}
	}
}

func keyRange(files []*SSTable) (lo, hi string) {
	for i, f := range files {
		if i == 0 || f.minKey < lo {
			lo = f.minKey
		}
		if i == 0 || f.maxKey > hi {
			hi = f.maxKey
		}
	}
	return lo, hi
}

// overlapping returns the files in level lvl whose range intersects [lo, hi].
func (e *Engine) overlapping(lvl int, lo, hi string) []*SSTable {
	var out []*SSTable
	for _, f := range e.levels[lvl] {
		if f.maxKey >= lo && f.minKey <= hi {
			out = append(out, f)
		}
	}
	return out
}

// runCompaction merges c.upper (newer) with c.lower (older) into new files
// at c.outLevel, then installs them. Called with cmu held and mu not held.
func (e *Engine) runCompaction(c *compaction) error {
	if e.opts.compactHook != nil {
		if err := e.opts.compactHook(); err != nil {
			return err
		}
	}
	merged := map[string]entry{}
	for _, f := range append(append([]*SSTable(nil), c.lower...), c.upper...) {
		entries, err := f.readAll()
		if err != nil {
			return fmt.Errorf("lsm: compact: %w", err)
		}
		for _, en := range entries {
			merged[en.key] = en // later (newer) files win
		}
	}
	keys := make([]string, 0, len(merged))
	for k, en := range merged {
		if en.deleted && !c.deeperOverlaps(k) {
			continue // nothing older left to shadow
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var outs []*SSTable
	var batch []entry
	var batchBytes int
	flushOut := func() error {
		if len(batch) == 0 {
			return nil
		}
		if e.stopping.Load() {
			return errCompactionAborted
		}
		e.mu.Lock()
		e.nextID++
		id := e.nextID
		e.mu.Unlock()
		f, err := writeSSTable(e.dir, c.outLevel, id, batch)
		if err != nil {
			return err
		}
		e.bytesWritten.Add(f.size)
		outs = append(outs, f)
		batch, batchBytes = nil, 0
		return nil
	}
	for _, k := range keys {
		en := merged[k]
		batch = append(batch, en)
		batchBytes += entryHeaderSize + len(en.key) + len(en.value)
		if batchBytes >= e.opts.TargetFileSize {
			if err := flushOut(); err != nil {
				return discard(outs, err)
			}
		}
	}
	if err := flushOut(); err != nil {
		return discard(outs, err)
	}
	if e.stopping.Load() {
		return discard(outs, errCompactionAborted)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.applyEdit(outs, append(append([]*SSTable(nil), c.upper...), c.lower...)); err != nil {
		return discard(outs, err)
	}
	e.compactions.Add(1)
	e.l0Cond.Broadcast()
	return nil
}

// discard removes output files of a compaction that failed before its
// manifest edit, and returns err.
func discard(outs []*SSTable, err error) error {
	for _, f := range outs {
		f.close()
		os.Remove(f.path)
	}
	return err
}

// deeperOverlaps reports whether any level below the output level has a
// file whose key range covers key.
func (c *compaction) deeperOverlaps(key string) bool {
	for i := c.outLevel + 1; i < numLevels; i++ {
		if f := findFile(c.deeper[i], key); f != nil {
			return true
		}
	}
	return false
}

// findFile returns the file in a sorted, non-overlapping level whose range
// contains key, or nil.
func findFile(files []*SSTable, key string) *SSTable {
	i := sort.Search(len(files), func(i int) bool { return files[i].maxKey >= key })
	if i < len(files) && files[i].minKey <= key {
		return files[i]
	}
	return nil
}

// ---- recovery ----

// loadTables opens the live SSTables in dir: those listed in the MANIFEST,
// or, for directories written before the manifest existed, those the old
// two-level recovery rule keeps (L1 is the newest l1 file; L0 files newer
// than it). Every other .sst file is deleted.
func (e *Engine) loadTables(files map[int]int) error {
	mpath := filepath.Join(e.dir, manifestName)
	var live map[int]int
	if _, err := os.Stat(mpath); err == nil {
		if live, err = replayManifest(mpath); err != nil {
			return err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		live = legacyLive(files)
	} else {
		return err
	}
	for id, level := range files {
		path := filepath.Join(e.dir, sstName(level, id))
		lvl, ok := live[id]
		if !ok || lvl != level {
			os.Remove(path) // not live: a leftover from an interrupted change
			continue
		}
		sst, err := openSSTable(path, level, id)
		if err != nil {
			return err
		}
		e.levels[level] = append(e.levels[level], sst)
	}
	sort.Slice(e.levels[0], func(i, j int) bool { return e.levels[0][i].id < e.levels[0][j].id })
	for lvl := 1; lvl < numLevels; lvl++ {
		files := e.levels[lvl]
		sort.Slice(files, func(i, j int) bool { return files[i].minKey < files[j].minKey })
	}
	return e.writeManifest()
}

func legacyLive(files map[int]int) map[int]int {
	newestL1 := -1
	for id, lvl := range files {
		if lvl == 1 && id > newestL1 {
			newestL1 = id
		}
	}
	live := map[int]int{}
	for id, lvl := range files {
		if (lvl == 0 || lvl == 1) && id >= newestL1 {
			live[id] = lvl
		}
	}
	return live
}
