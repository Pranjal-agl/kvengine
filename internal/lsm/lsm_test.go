package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPutGet(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e.Put([]byte("a"), []byte("1"))
	e.Put([]byte("b"), []byte("2"))

	v, ok := e.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Errorf("Get(a): got (%q,%v), want (1,true)", v, ok)
	}
	_, ok = e.Get([]byte("missing"))
	if ok {
		t.Error("Get(missing) should be false")
	}
}

func TestOverwrite(t *testing.T) {
	e, _ := Open(t.TempDir())
	e.Put([]byte("k"), []byte("v1"))
	e.Put([]byte("k"), []byte("v2"))
	v, ok := e.Get([]byte("k"))
	if !ok || string(v) != "v2" {
		t.Errorf("overwrite: got (%q,%v), want (v2,true)", v, ok)
	}
}

func TestDelete(t *testing.T) {
	e, _ := Open(t.TempDir())
	e.Put([]byte("k"), []byte("v"))
	e.Delete([]byte("k"))
	_, ok := e.Get([]byte("k"))
	if ok {
		t.Error("key should be gone after Delete")
	}
}

func TestFlushAndRead(t *testing.T) {
	e, _ := Open(t.TempDir())
	for i := 0; i < 100; i++ {
		e.Put([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i)))
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	stats := e.Stats()
	if stats["l0_files"].(int) == 0 {
		t.Fatal("expected L0 file after Flush")
	}
	t.Logf("after flush: %v", stats)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i)
		want := fmt.Sprintf("val%04d", i)
		v, ok := e.Get([]byte(key))
		if !ok || string(v) != want {
			t.Errorf("after flush: Get(%s)=(%q,%v), want (%s,true)", key, v, ok, want)
		}
	}
}

func TestTombstoneAcrossLevels(t *testing.T) {
	e, _ := Open(t.TempDir())
	e.Put([]byte("ghost"), []byte("present"))
	e.Flush()
	e.Delete([]byte("ghost")) // MemTable tombstone shadows L0 value
	_, ok := e.Get([]byte("ghost"))
	if ok {
		t.Error("MemTable tombstone must shadow L0 value")
	}
}

// TestTombstoneInSSTableShadowsOlderSSTable: a delete that has already been
// flushed to a newer L0 file must still hide the value in an older L0 file.
// Before the fix, SSTable.get reported a tombstone as "not found", so the
// search fell through to the older file and the deleted value came back.
func TestTombstoneInSSTableShadowsOlderSSTable(t *testing.T) {
	e, _ := Open(t.TempDir())
	e.Put([]byte("ghost"), []byte("present"))
	e.Flush() // L0 #1: ghost=present
	e.Delete([]byte("ghost"))
	e.Flush() // L0 #2: ghost=<tombstone>
	if v, ok := e.Get([]byte("ghost")); ok {
		t.Fatalf("deleted key resurrected from older SSTable: %q", v)
	}
}

// TestTombstoneInL0ShadowsL1 covers the same rule across levels.
func TestTombstoneInL0ShadowsL1(t *testing.T) {
	e, _ := Open(t.TempDir())
	e.Put([]byte("ghost"), []byte("present"))
	e.Flush()
	e.mu.Lock()
	if err := e.compact(); err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	e.mu.Unlock() // ghost=present now lives in L1
	e.Delete([]byte("ghost"))
	e.Flush() // tombstone in L0
	if v, ok := e.Get([]byte("ghost")); ok {
		t.Fatalf("deleted key resurrected from L1: %q", v)
	}
}

func TestCompaction(t *testing.T) {
	e, _ := Open(t.TempDir())

	for i := 0; i < 50; i++ {
		e.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
	e.Flush()

	for i := 0; i < 25; i++ {
		e.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("updated"))
	}
	for i := 25; i < 50; i++ {
		e.Delete([]byte(fmt.Sprintf("k%03d", i)))
	}
	e.Flush()
	e.Flush()

	e.mu.Lock()
	if err := e.compact(); err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	e.mu.Unlock()

	stats := e.Stats()
	t.Logf("after compaction: %v", stats)
	if stats["l0_files"].(int) != 0 {
		t.Errorf("expected 0 L0 files after compaction, got %d", stats["l0_files"].(int))
	}

	for i := 0; i < 25; i++ {
		v, ok := e.Get([]byte(fmt.Sprintf("k%03d", i)))
		if !ok || string(v) != "updated" {
			t.Errorf("k%03d: got (%q,%v), want (updated,true)", i, v, ok)
		}
	}
	for i := 25; i < 50; i++ {
		_, ok := e.Get([]byte(fmt.Sprintf("k%03d", i)))
		if ok {
			t.Errorf("k%03d: should be deleted after compaction", i)
		}
	}
}

func TestSSTableFileCountBounded(t *testing.T) {
	dir := t.TempDir()
	e, _ := Open(dir)

	for round := 0; round < 20; round++ {
		for i := 0; i < 50; i++ {
			e.Put([]byte(fmt.Sprintf("r%d-k%d", round, i)), []byte("v"))
		}
		e.Flush()
		e.mu.Lock()
		if len(e.l0) >= maxL0Tables {
			e.compact()
		}
		e.mu.Unlock()
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	t.Logf("SSTable files after 20 rounds: %d", len(files))
	if len(files) > maxL0Tables+2 {
		t.Errorf("too many SSTable files: %d — compaction not working", len(files))
	}
}

func BenchmarkLSMPut(b *testing.B) {
	e, _ := Open(b.TempDir())
	key := []byte("benchkey")
	val := []byte("benchvalue")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Put(key, val)
	}
}

func BenchmarkLSMGet(b *testing.B) {
	e, _ := Open(b.TempDir())
	e.Put([]byte("k"), []byte("v"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get([]byte("k"))
	}
}

func BenchmarkLSMGetAfterFlush(b *testing.B) {
	e, _ := Open(b.TempDir())
	for i := 0; i < 1000; i++ {
		e.Put([]byte(fmt.Sprintf("k%06d", i)), []byte("v"))
	}
	e.Flush()
	target := []byte("k000500")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get(target)
	}
}

// TestReopenRecoversSSTables: flushed data (including tombstones and
// compacted data) survives Close + Open.
func TestReopenRecoversSSTables(t *testing.T) {
	dir := t.TempDir()
	e, _ := Open(dir)
	for i := 0; i < 200; i++ {
		e.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
		if i%40 == 39 {
			e.Flush() // 5 flushes → forces one compaction along the way
		}
	}
	e.Delete([]byte("k007"))
	e.Put([]byte("k008"), []byte("new"))
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%03d", i)
		v, ok := e.Get([]byte(k))
		switch {
		case i == 7:
			if ok {
				t.Errorf("%s: deleted key came back after reopen: %q", k, v)
			}
		case i == 8:
			if string(v) != "new" {
				t.Errorf("%s: got %q, want new", k, v)
			}
		default:
			if !ok || string(v) != fmt.Sprintf("v%03d", i) {
				t.Errorf("%s: got (%q, %v) after reopen", k, v, ok)
			}
		}
	}
}

// TestReopenAfterInterruptedCompaction simulates a crash after compaction
// wrote its new L1 file but before it deleted the merged inputs. Open must
// recognise the leftovers as obsolete rather than letting a stale value or
// a dropped tombstone's victim reappear.
func TestReopenAfterInterruptedCompaction(t *testing.T) {
	dir := t.TempDir()
	e, _ := Open(dir)
	e.Put([]byte("gone"), []byte("old"))
	e.Put([]byte("k"), []byte("v1"))
	e.Flush()
	e.Delete([]byte("gone"))
	e.Put([]byte("k"), []byte("v2"))
	e.Flush()

	// Copy the L0 inputs aside, compact, then put them back as if the
	// deletions never happened.
	saved := map[string][]byte{}
	files, _ := filepath.Glob(filepath.Join(dir, "l0-*.sst"))
	for _, f := range files {
		saved[f], _ = os.ReadFile(f)
	}
	e.mu.Lock()
	if err := e.compact(); err != nil {
		t.Fatal(err)
	}
	e.mu.Unlock()
	e.Close()
	for f, b := range saved {
		os.WriteFile(f, b, 0o644)
	}

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if v, ok := e.Get([]byte("gone")); ok {
		t.Errorf("deleted key resurrected from stale L0 leftover: %q", v)
	}
	if v, _ := e.Get([]byte("k")); string(v) != "v2" {
		t.Errorf("k = %q, want v2", v)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "l0-*.sst")); len(left) != 0 {
		t.Errorf("stale L0 files not cleaned up: %v", left)
	}
}

// TestValueThatLooksLikeOldTombstone: the old format used a magic string as
// the tombstone value, so storing that string deleted the key.
func TestValueThatLooksLikeOldTombstone(t *testing.T) {
	e, _ := Open(t.TempDir())
	defer e.Close()
	e.Put([]byte("k"), []byte("\x00tombstone"))
	e.Flush()
	if v, ok := e.Get([]byte("k")); !ok || string(v) != "\x00tombstone" {
		t.Fatalf("got (%q, %v)", v, ok)
	}
}

// TestSparseIndexLookups checks every key (and gaps between keys) in a
// table much larger than indexInterval, so lookups land at every position
// relative to an index point.
func TestSparseIndexLookups(t *testing.T) {
	e, _ := Open(t.TempDir())
	defer e.Close()
	for i := 0; i < 1000; i += 2 {
		e.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	e.Flush()
	for i := -1; i <= 1001; i++ {
		v, ok := e.Get([]byte(fmt.Sprintf("k%05d", i)))
		want := i >= 0 && i < 1000 && i%2 == 0
		if ok != want || (ok && string(v) != fmt.Sprintf("v%d", i)) {
			t.Fatalf("k%05d: got (%q, %v), want present=%v", i, v, ok, want)
		}
	}
}

// BenchmarkLSMGetLargeSSTable: point lookups in a 100k-entry SSTable. With a
// linear scan this grows with table size; with the sparse index it doesn't.
func BenchmarkLSMGetLargeSSTable(b *testing.B) {
	e, _ := Open(b.TempDir())
	defer e.Close()
	for i := 0; i < 100000; i++ {
		e.Put([]byte(fmt.Sprintf("k%07d", i)), []byte("value"))
	}
	e.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get([]byte(fmt.Sprintf("k%07d", (i*7919)%100000)))
	}
}

// BenchmarkLSMPutNoSync: WAL appended and handed to the OS, not fsynced.
func BenchmarkLSMPutNoSync(b *testing.B) {
	e, _ := OpenWithOptions(b.TempDir(), Options{NoSync: true})
	defer e.Close()
	key, val := []byte("benchkey"), []byte("benchvalue")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Put(key, val)
	}
}

// BenchmarkLSMPutConcurrentWriters: durable Puts as the writer count grows.
// With group commit, writers arriving during an fsync share the next one.
func BenchmarkLSMPutConcurrentWriters(b *testing.B) {
	for _, writers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			e, _ := Open(b.TempDir())
			defer e.Close()
			var next int64
			var wg sync.WaitGroup
			b.ResetTimer()
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := atomic.AddInt64(&next, 1)
						if i > int64(b.N) {
							return
						}
						if err := e.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
							b.Error(err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
