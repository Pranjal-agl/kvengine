package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	want := []Record{
		{Type: RecordPut, Key: []byte("a"), Value: []byte("1")},
		{Type: RecordPut, Key: []byte("b"), Value: []byte("2")},
		{Type: RecordDelete, Key: []byte("a")},
		{Type: RecordPut, Key: []byte(""), Value: []byte("")}, // empty key/value edge case
	}
	for _, rec := range want {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != want[i].Type ||
			string(got[i].Key) != string(want[i].Key) ||
			string(got[i].Value) != string(want[i].Value) {
			t.Errorf("record %d mismatch: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestReplayMissingFile(t *testing.T) {
	got, err := Replay(filepath.Join(t.TempDir(), "does-not-exist.wal"))
	if err != nil {
		t.Fatalf("Replay on missing file should not error, got: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil records for missing file, got %v", got)
	}
}

// TestReplayTornTail simulates a crash mid-write: two full records are
// written and synced, then a third record is appended but truncated
// part-way through (as if the process died mid-write before fsync). Replay
// must return exactly the two complete records and silently drop the torn
// one, per the documented recovery semantics in docs/ARCHITECTURE.md.
func TestReplayTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	full := []Record{
		{Type: RecordPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: RecordPut, Key: []byte("k2"), Value: []byte("v2")},
	}
	for _, rec := range full {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Simulate a torn write: encode a third record but only write the first
	// half of its bytes directly to the file, bypassing the WAL's own
	// buffering, then close without a clean Sync — mimicking a crash.
	torn := encode(Record{Type: RecordPut, Key: []byte("k3"), Value: []byte("v3-this-wont-survive")})
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen for torn write: %v", err)
	}
	half := len(torn) / 2
	if _, err := f.Write(torn[:half]); err != nil {
		t.Fatalf("torn write: %v", err)
	}
	f.Close()
	w.Close()

	got, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay should tolerate torn tail, got error: %v", err)
	}
	if len(got) != len(full) {
		t.Fatalf("got %d records after torn-tail replay, want %d (torn record should be dropped)", len(got), len(full))
	}
	for i := range full {
		if string(got[i].Key) != string(full[i].Key) {
			t.Errorf("record %d: got key %q, want %q", i, got[i].Key, full[i].Key)
		}
	}
}
