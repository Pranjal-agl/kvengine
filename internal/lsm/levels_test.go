package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// tiny returns options that force many levels with little data.
func tiny() Options {
	return Options{MemTableSize: 4 << 10, TargetFileSize: 4 << 10, LevelSizeBase: 16 << 10, NoSync: true}
}

func checkLevelInvariants(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.WaitForCompactions(); err != nil {
		t.Fatal(err)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for lvl := 1; lvl < numLevels; lvl++ {
		files := e.levels[lvl]
		for i, f := range files {
			if f.minKey > f.maxKey {
				t.Fatalf("L%d file %d: min %q > max %q", lvl, f.id, f.minKey, f.maxKey)
			}
			if i > 0 && files[i-1].maxKey >= f.minKey {
				t.Fatalf("L%d files %d and %d overlap: [%q,%q] [%q,%q]", lvl,
					files[i-1].id, f.id, files[i-1].minKey, files[i-1].maxKey, f.minKey, f.maxKey)
			}
		}
		if lvl < numLevels-1 && levelBytes(files) > e.maxBytes(lvl) {
			t.Fatalf("L%d holds %d bytes, over its %d budget", lvl, levelBytes(files), e.maxBytes(lvl))
		}
	}
	if len(e.levels[0]) >= maxL0Tables {
		t.Fatalf("L0 has %d files after compaction", len(e.levels[0]))
	}
}

func TestLeveledCompactionInvariants(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenWithOptions(dir, tiny())
	const n = 20000
	for i := 0; i < n; i++ {
		e.Put([]byte(fmt.Sprintf("k%06d", (i*7919)%n)), []byte(fmt.Sprintf("v%d", i)))
	}
	checkLevelInvariants(t, e)
	stats := e.Stats()
	deepest := 0
	for lvl, f := range stats["level_files"].([]int) {
		if f > 0 {
			deepest = lvl
		}
	}
	if deepest < 3 {
		t.Fatalf("expected data in at least L3, levels = %v", stats["level_files"])
	}
	t.Logf("level files %v, bytes %v", stats["level_files"], stats["level_bytes"])
	e.Close()

	e, _ = OpenWithOptions(dir, tiny())
	defer e.Close()
	checkLevelInvariants(t, e)
	for i := 0; i < n; i++ {
		k := (i * 7919) % n
		want := fmt.Sprintf("v%d", i)
		if v, ok := e.Get([]byte(fmt.Sprintf("k%06d", k))); !ok || string(v) != want {
			t.Fatalf("k%06d = (%q, %v), want %q", k, v, ok, want)
		}
	}
}

// A tombstone must survive compaction until nothing older remains below
// it; dropping it early would resurrect the value in the deeper level.
func TestTombstoneKeptUntilNothingOlderBelow(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenWithOptions(dir, tiny())
	e.Put([]byte("victim"), []byte("old"))
	filler := func(round int) {
		for i := 0; i < 3000; i++ {
			e.Put([]byte(fmt.Sprintf("f%d-%05d", round, i)), []byte("xxxxxxxxxxxxxxxx"))
		}
	}
	filler(0) // pushes "victim" down several levels
	e.Delete([]byte("victim"))
	for r := 1; r < 6; r++ {
		filler(r) // many compactions carry the tombstone down
		if v, ok := e.Get([]byte("victim")); ok {
			t.Fatalf("deleted key came back after round %d: %q", r, v)
		}
	}
	e.Close()
	e, _ = OpenWithOptions(dir, tiny())
	defer e.Close()
	if v, ok := e.Get([]byte("victim")); ok {
		t.Fatalf("deleted key came back after reopen: %q", v)
	}
}

// Randomized model check: puts, deletes, flushes, clean reopens and
// crashes (abandon without Close) against a map.
func TestLSMMatchesModel(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		r := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		e, _ := OpenWithOptions(dir, tiny())
		model := map[string]string{}
		for op := 0; op < 12000; op++ {
			k := fmt.Sprintf("k%04d", r.Intn(1500))
			switch x := r.Intn(100); {
			case x < 60:
				v := fmt.Sprintf("v%d-%d", seed, op)
				e.Put([]byte(k), []byte(v))
				model[k] = v
			case x < 85:
				e.Delete([]byte(k))
				delete(model, k)
			case x < 97:
				got, ok := e.Get([]byte(k))
				want, wok := model[k]
				if ok != wok || string(got) != want {
					t.Fatalf("seed %d op %d: %s = (%q, %v), model (%q, %v)", seed, op, k, got, ok, want, wok)
				}
			case x < 98:
				e.Flush()
			case x < 99:
				e.Close()
				e, _ = OpenWithOptions(dir, tiny())
			default:
				crash(e) // durable writes only (NoSync still reaches the OS)
				e, _ = OpenWithOptions(dir, tiny())
			}
		}
		checkLevelInvariants(t, e)
		for i := 0; i < 1500; i++ {
			k := fmt.Sprintf("k%04d", i)
			got, ok := e.Get([]byte(k))
			want, wok := model[k]
			if ok != wok || string(got) != want {
				t.Fatalf("seed %d final: %s = (%q, %v), model (%q, %v)", seed, k, got, ok, want, wok)
			}
		}
		e.Close()
	}
}

// An SSTable written by a compaction that crashed before its manifest
// edit is not live: Open must delete it rather than serve from it.
func TestOrphanSSTableIgnored(t *testing.T) {
	dir := t.TempDir()
	e, _ := Open(dir)
	e.Put([]byte("k"), []byte("real"))
	e.Close()
	orphan, err := writeSSTable(dir, 0, 999999, []entry{{key: "k", value: "orphan"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	orphan.close()
	e, _ = Open(dir)
	defer e.Close()
	if v, _ := e.Get([]byte("k")); string(v) != "real" {
		t.Fatalf("k = %q, read from an orphan SSTable", v)
	}
	if _, err := os.Stat(orphan.path); !os.IsNotExist(err) {
		t.Fatal("orphan SSTable not removed")
	}
}

func TestTornManifestTail(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenWithOptions(dir, tiny())
	for i := 0; i < 2000; i++ {
		e.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}
	crash(e)
	f, _ := os.OpenFile(filepath.Join(dir, manifestName), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01})
	f.Close()
	e, err := OpenWithOptions(dir, tiny())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < 2000; i++ {
		if _, ok := e.Get([]byte(fmt.Sprintf("k%05d", i))); !ok {
			t.Fatalf("k%05d lost", i)
		}
	}
}

// Directories written before the manifest existed are opened with the old
// two-level recovery rule, then get a manifest.
func TestPreManifestDirectoryMigrates(t *testing.T) {
	dir := t.TempDir()
	// Old layout: one L1 file plus a newer L0 file, and a stale L0 file
	// that the old L1 already absorbed.
	for _, f := range []struct {
		level, id int
		ents      []entry
	}{
		{0, 1, []entry{{key: "a", value: "stale"}}},
		{1, 2, []entry{{key: "a", value: "1"}, {key: "b", value: "2"}}},
		{0, 3, []entry{{key: "b", value: "3"}}},
	} {
		s, err := writeSSTable(dir, f.level, f.id, f.ents, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.close()
	}
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := e.Get([]byte("a")); string(v) != "1" {
		t.Fatalf("a = %q", v)
	}
	if v, _ := e.Get([]byte("b")); string(v) != "3" {
		t.Fatalf("b = %q", v)
	}
	e.Close()
	if _, err := os.Stat(filepath.Join(dir, manifestName)); err != nil {
		t.Fatal("no manifest written for migrated directory")
	}
	e, _ = Open(dir)
	defer e.Close()
	if v, _ := e.Get([]byte("b")); string(v) != "3" {
		t.Fatalf("after reopen b = %q", v)
	}
}
