package store

import (
	"path/filepath"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, ok := s.Get([]byte("k")); ok {
		t.Fatalf("expected missing key to return ok=false")
	}
	if err := s.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok := s.Get([]byte("k"))
	if !ok || string(v) != "v1" {
		t.Fatalf("Get after Put: got (%q, %v), want (v1, true)", v, ok)
	}
	if err := s.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	v, _ = s.Get([]byte("k"))
	if string(v) != "v2" {
		t.Fatalf("Get after overwrite: got %q, want v2", v)
	}
	if err := s.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := s.Get([]byte("k")); ok {
		t.Fatalf("expected key gone after Delete")
	}
}

// TestRestartRecoversState is the core crash-recovery integration test:
// write data, close (simulating shutdown — a real crash is covered by the
// WAL-level torn-write test and the fault injection harness), reopen, and
// confirm the state survived via WAL replay.
func TestRestartRecoversState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s1.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if _, ok := s2.Get([]byte("a")); ok {
		t.Errorf("key 'a' should have stayed deleted after restart")
	}
	if v, ok := s2.Get([]byte("b")); !ok || string(v) != "2" {
		t.Errorf("key 'b': got (%q, %v), want (2, true)", v, ok)
	}
	if v, ok := s2.Get([]byte("c")); !ok || string(v) != "3" {
		t.Errorf("key 'c': got (%q, %v), want (3, true)", v, ok)
	}
	if s2.Len() != 2 {
		t.Errorf("expected 2 keys after recovery, got %d", s2.Len())
	}
}
