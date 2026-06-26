package lsm

import (
	"fmt"
	"path/filepath"
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
