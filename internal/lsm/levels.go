package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

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
// Compaction:
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

// maybeCompact runs compactions until every level is within budget.
// Called with mu held.
func (e *Engine) maybeCompact() error {
	for {
		if len(e.levels[0]) >= maxL0Tables {
			if err := e.compactL0(); err != nil {
				return err
			}
			continue
		}
		lvl := 0
		for i := 1; i < numLevels-1; i++ {
			if levelBytes(e.levels[i]) > e.maxBytes(i) {
				lvl = i
				break
			}
		}
		if lvl == 0 {
			return nil
		}
		if err := e.compactLevel(lvl); err != nil {
			return err
		}
	}
}

// compact merges all of L0 into L1. Called with mu held. (Also used
// directly by tests.)
func (e *Engine) compact() error {
	if len(e.levels[0]) == 0 {
		return nil
	}
	return e.compactL0()
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

func (e *Engine) compactL0() error {
	upper := append([]*SSTable{}, e.levels[0]...) // oldest first
	lo, hi := keyRange(upper)
	lower := e.overlapping(1, lo, hi)
	return e.merge(upper, lower, 1)
}

func (e *Engine) compactLevel(lvl int) error {
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
	lower := e.overlapping(lvl+1, pick.minKey, pick.maxKey)
	return e.merge([]*SSTable{pick}, lower, lvl+1)
}

// merge combines upper (newer; oldest first within itself) with lower
// (older) into new files at outLevel, then swaps them in atomically.
func (e *Engine) merge(upper, lower []*SSTable, outLevel int) error {
	merged := map[string]entry{}
	for _, f := range append(append([]*SSTable{}, lower...), upper...) {
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
		if en.deleted && !e.deeperOverlaps(outLevel, k) {
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
		e.nextID++
		f, err := writeSSTable(e.dir, outLevel, e.nextID, batch)
		if err != nil {
			return err
		}
		e.bytesWritten += f.size
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
	if err := e.applyEdit(outs, append(append([]*SSTable{}, upper...), lower...)); err != nil {
		return discard(outs, err)
	}
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

// deeperOverlaps reports whether any level below lvl has a file whose key
// range covers key.
func (e *Engine) deeperOverlaps(lvl int, key string) bool {
	for i := lvl + 1; i < numLevels; i++ {
		if f := findFile(e.levels[i], key); f != nil {
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
