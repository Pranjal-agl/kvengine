package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestWritesAfterTornTailSurviveSecondRestart reproduces the "crash twice"
// scenario: a crash leaves a torn record at the end of the WAL, the store
// restarts and acknowledges new writes, and then the store restarts again.
// Every write acknowledged after the first restart must survive the second.
//
// Before the fix, Open replayed up to the torn record but then appended new
// records *after* the torn bytes. The next replay stopped at the torn
// record, silently losing every acknowledged write that followed it.
func TestWritesAfterTornTailSurviveSecondRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put([]byte("before"), []byte("crash")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Simulate a crash mid-append: half a record's worth of garbage header.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x40, 0x00}) // torn 8-byte header
	f.Close()

	// First restart: recover, then acknowledge new writes.
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get([]byte("before")); !ok || string(v) != "crash" {
		t.Fatalf("pre-crash write lost: got (%q, %v)", v, ok)
	}
	for i := 0; i < 10; i++ {
		if err := s.Put([]byte(fmt.Sprintf("after-%d", i)), []byte("acked")); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	// Second restart: every acknowledged write must still be there.
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("after-%d", i)
		if v, ok := s.Get([]byte(k)); !ok || string(v) != "acked" {
			t.Fatalf("acknowledged write %q lost after second restart: got (%q, %v)", k, v, ok)
		}
	}
}

// TestConcurrentSameKeyMemoryMatchesLog checks that the in-memory state a
// client observes before a restart is the same state the WAL rebuilds after
// it. Many goroutines overwrite the same small set of keys concurrently; we
// then snapshot memory, reopen from the WAL, and compare.
//
// Before the fix, Put appended to the log and updated the map under
// different locks, so two concurrent Puts to one key could hit the log in
// order A,B but the map in order B,A — memory said A, the log said B.
func TestConcurrentSameKeyMemoryMatchesLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "order.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	const writers, perWriter, keys = 16, 40, 3
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("k%d", i%keys))
				if (w+i)%7 == 0 {
					s.Delete(k)
				} else {
					s.Put(k, []byte(fmt.Sprintf("w%d-i%d", w, i)))
				}
			}
		}(w)
	}
	wg.Wait()

	before := map[string]string{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if v, ok := s.Get([]byte(k)); ok {
			before[k] = string(v)
		}
	}
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		v, ok := s.Get([]byte(k))
		bv, bok := before[k]
		if ok != bok || string(v) != bv {
			t.Fatalf("key %s: memory before restart = (%q, %v), WAL after restart = (%q, %v)", k, bv, bok, v, ok)
		}
	}
}

// TestRestoreSnapshotReplacesState checks that RestoreSnapshot replaces the
// in-memory state instead of merging into it. A key that exists locally but
// not in the snapshot must be gone afterwards.
func TestRestoreSnapshotReplacesState(t *testing.T) {
	src, err := Open(filepath.Join(t.TempDir(), "src.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	src.Put([]byte("kept"), []byte("1"))
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	dst, err := Open(filepath.Join(t.TempDir(), "dst.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	dst.Put([]byte("stale"), []byte("x"))
	if err := dst.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if _, ok := dst.Get([]byte("stale")); ok {
		t.Fatal("key absent from snapshot survived RestoreSnapshot")
	}
	if v, ok := dst.Get([]byte("kept")); !ok || string(v) != "1" {
		t.Fatalf("snapshot key missing: got (%q, %v)", v, ok)
	}
}

// TestSnapshotBinarySafe: keys and values that are not valid UTF-8 must
// survive Snapshot → RestoreSnapshot byte-for-byte.
func TestSnapshotBinarySafe(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "bin.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	key, val := []byte{0xff, 0xfe, 'k'}, []byte{0x00, 0xc3, 0x28, 0xff}
	s.Put(key, val)
	snap, _ := s.Snapshot()
	s.Delete(key)
	if err := s.RestoreSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get(key); !ok || string(v) != string(val) {
		t.Fatalf("binary key/value corrupted by snapshot: got (%x, %v), want %x", v, ok, val)
	}
}

// TestRestoreLegacySnapshot: snapshots written by the old map-based format
// must still load.
func TestRestoreLegacySnapshot(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "legacy.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RestoreSnapshot([]byte(`{"a":"MQ=="}`)); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("legacy snapshot: got (%q, %v)", v, ok)
	}
}

// TestWriteAfterClose: writes after Close fail cleanly instead of panicking.
func TestWriteAfterClose(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "closed.wal"))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := s.Put([]byte("k"), []byte("v")); err != ErrClosed {
		t.Fatalf("Put after Close: got %v, want ErrClosed", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
