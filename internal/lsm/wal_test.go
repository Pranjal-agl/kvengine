package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// crash abandons an engine without Close, as if the process died: nothing
// is flushed, and only what reached the WAL survives.
func crash(e *Engine) {}

func reopen(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func expect(t *testing.T, e *Engine, key, want string, present bool) {
	t.Helper()
	v, ok := e.Get([]byte(key))
	if ok != present || (ok && string(v) != want) {
		t.Fatalf("%s = (%q, %v), want (%q, %v)", key, v, ok, want, present)
	}
}

// Writes that never reached an SSTable used to be lost on crash.
func TestUnflushedWritesSurviveCrash(t *testing.T) {
	dir := t.TempDir()
	e := reopen(t, dir)
	e.Put([]byte("a"), []byte("1"))
	e.Put([]byte("b"), []byte("2"))
	e.Delete([]byte("a"))
	crash(e)

	e = reopen(t, dir)
	defer e.Close()
	expect(t, e, "a", "", false)
	expect(t, e, "b", "2", true)
}

// A crash after a flush wrote its SSTable but before it deleted the old
// WAL leaves two WALs. They must replay oldest first, and the newer WAL
// must win over both the old WAL and the SSTable.
func TestReplayOrderAfterInterruptedFlush(t *testing.T) {
	dir := t.TempDir()
	e := reopen(t, dir)
	e.Put([]byte("k"), []byte("v1"))
	old, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	saved, _ := os.ReadFile(old[0])
	e.Flush()                        // k=v1 into an SSTable, old WAL deleted
	e.Put([]byte("k"), []byte("v2")) // only in the new WAL
	e.Delete([]byte("gone"))
	crash(e)
	os.WriteFile(old[0], saved, 0o644) // "the deletion never happened"

	e = reopen(t, dir)
	defer e.Close()
	expect(t, e, "k", "v2", true)
}

func TestTornWALTailIgnored(t *testing.T) {
	dir := t.TempDir()
	e := reopen(t, dir)
	e.Put([]byte("k"), []byte("v"))
	crash(e)
	wals, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	f, _ := os.OpenFile(wals[0], os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9})
	f.Close()

	e = reopen(t, dir)
	defer e.Close()
	expect(t, e, "k", "v", true)
}

// Flushed WALs are deleted, so they don't accumulate.
func TestWALFilesDoNotAccumulate(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenWithOptions(dir, Options{MemTableSize: 512})
	for i := 0; i < 500; i++ {
		e.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value-value-value"))
	}
	wals, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(wals) != 1 {
		t.Fatalf("%d WAL files after many flushes, want 1", len(wals))
	}
	e.Close()
	wals, _ = filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(wals) != 0 {
		t.Fatalf("%d WAL files left after Close, want 0", len(wals))
	}
	e = reopen(t, dir)
	defer e.Close()
	for i := 0; i < 500; i++ {
		expect(t, e, fmt.Sprintf("k%03d", i), "value-value-value", true)
	}
}

// NoSync writes still survive a process crash (they reach the OS).
func TestNoSyncSurvivesProcessCrash(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenWithOptions(dir, Options{NoSync: true})
	e.Put([]byte("k"), []byte("v"))
	crash(e)
	e = reopen(t, dir)
	defer e.Close()
	expect(t, e, "k", "v", true)
}

// Concurrent writers and readers with frequent flushes and compactions;
// run under -race. Each writer owns its keys, so final values are known.
func TestConcurrentReadWriteWithFlushes(t *testing.T) {
	e, _ := OpenWithOptions(t.TempDir(), Options{MemTableSize: 1024})
	defer e.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Put([]byte(fmt.Sprintf("w%d-k%d", w, i%20)), []byte(fmt.Sprintf("%d", i)))
			}
		}(w)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Get([]byte(fmt.Sprintf("w%d-k%d", w, i%20)))
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < 4; w++ {
		for k := 0; k < 20; k++ {
			expect(t, e, fmt.Sprintf("w%d-k%d", w, k), fmt.Sprintf("%d", 180+k), true)
		}
	}
}
