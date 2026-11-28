package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestCompactShrinksWALAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.wal")
	s, _ := OpenWithOptions(path, Options{CompactMinBytes: -1}) // manual only
	for i := 0; i < 5000; i++ {
		s.Put([]byte(fmt.Sprintf("k%d", i%50)), []byte(fmt.Sprintf("value-%d", i)))
	}
	for i := 0; i < 10; i++ {
		s.Delete([]byte(fmt.Sprintf("k%d", i)))
	}
	before := s.WALSize()
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after := s.WALSize()
	if after*20 > before {
		t.Fatalf("WAL %d -> %d bytes: not compacted", before, after)
	}
	s.Put([]byte("post"), []byte("compaction")) // appends go to the new log
	s.Close()

	s, _ = Open(path)
	defer s.Close()
	if s.Len() != 41 {
		t.Fatalf("Len = %d after reopen, want 41", s.Len())
	}
	if v, ok := s.Get([]byte("k49")); !ok || string(v) != "value-4999" {
		t.Fatalf("k49 = (%q, %v)", v, ok)
	}
	if _, ok := s.Get([]byte("k3")); ok {
		t.Fatal("deleted key back after compaction")
	}
	if v, _ := s.Get([]byte("post")); string(v) != "compaction" {
		t.Fatal("write after compaction lost")
	}
	t.Logf("WAL %d -> %d bytes", before, after)
}

func TestAutoCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto.wal")
	s, _ := OpenWithOptions(path, Options{CompactMinBytes: 4096, CompactRatio: 4})
	defer s.Close()
	for i := 0; i < 3000; i++ {
		s.Put([]byte("hot"), []byte(fmt.Sprintf("v%d", i)))
	}
	if size := s.WALSize(); size > 4096*2 {
		t.Fatalf("WAL is %d bytes; automatic compaction never ran", size)
	}
	if v, _ := s.Get([]byte("hot")); string(v) != "v2999" {
		t.Fatalf("hot = %q", v)
	}
}

// Followers resume by byte offset, so a replicated WAL must not be rewritten.
func TestCompactionOffWhileReplicating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.wal")
	s, _ := OpenWithOptions(path, Options{CompactMinBytes: 1024})
	defer s.Close()
	_ = s.WAL()
	for i := 0; i < 500; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := s.Compact(); err != ErrReplicating {
		t.Fatalf("Compact = %v, want ErrReplicating", err)
	}
	if s.WALSize() < 500*10 {
		t.Fatal("replicated WAL was compacted automatically")
	}
}

// A compaction interrupted before its rename leaves a temp file; the old
// log is still complete and the temp file is cleaned up.
func TestInterruptedCompactionLeftover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "i.wal")
	s, _ := Open(path)
	s.Put([]byte("k"), []byte("v"))
	s.Close()
	os.WriteFile(compactTmp(path), []byte("half-written garbage"), 0o644)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, _ := s.Get([]byte("k")); string(v) != "v" {
		t.Fatalf("k = %q", v)
	}
	if _, err := os.Stat(compactTmp(path)); !os.IsNotExist(err) {
		t.Fatal("stale compaction temp file not removed")
	}
}

// Concurrent writers with compaction triggering constantly: memory before
// a restart must equal recovery after it.
func TestConcurrentWritesDuringCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cc.wal")
	s, _ := OpenWithOptions(path, Options{CompactMinBytes: 2048, CompactRatio: 2})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				k := []byte(fmt.Sprintf("k%d", i%7))
				if (w+i)%5 == 0 {
					s.Delete(k)
				} else {
					s.Put(k, []byte(fmt.Sprintf("w%d-%d", w, i)))
				}
				if i%50 == 0 {
					s.Compact()
				}
				s.Get(k)
			}
		}(w)
	}
	wg.Wait()
	before := map[string]string{}
	for i := 0; i < 7; i++ {
		k := fmt.Sprintf("k%d", i)
		if v, ok := s.Get([]byte(k)); ok {
			before[k] = string(v)
		}
	}
	s.Close()
	s, _ = Open(path)
	defer s.Close()
	for i := 0; i < 7; i++ {
		k := fmt.Sprintf("k%d", i)
		v, ok := s.Get([]byte(k))
		if bv, bok := before[k]; ok != bok || string(v) != bv {
			t.Fatalf("%s: before restart (%q, %v), after (%q, %v)", k, bv, bok, v, ok)
		}
	}
}
