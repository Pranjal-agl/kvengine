package linearcheck

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kvengine/internal/store"
)

// TestCheckLinearizable: a simple history that IS linearizable.
// Sequential: Put(a,1) then Get(a)=1 — trivially valid.
func TestCheckLinearizable(t *testing.T) {
	start := time.Now()
	ops := []Op{
		{ID: 1, Type: OpPut, Key: "a", Value: "1", Start: start, End: start.Add(1 * time.Millisecond)},
		{ID: 2, Type: OpGet, Key: "a", Value: "1", Found: true, Start: start.Add(2 * time.Millisecond), End: start.Add(3 * time.Millisecond)},
	}
	if err := Check(ops); err != nil {
		t.Errorf("expected linearizable, got: %v", err)
	}
}

// TestCheckNotLinearizable: a history that is NOT linearizable.
// Both Get ops overlap with a single Put but return inconsistent results.
// Get(a)="1" and Get(a)="2" cannot both be correct for one sequential Put.
func TestCheckNotLinearizable(t *testing.T) {
	start := time.Now()
	// A single write window that two reads overlap with but return different values
	// with no other writes — impossible to linearize.
	ops := []Op{
		// Put a=1, finishes at t=5ms
		{ID: 1, Type: OpPut, Key: "a", Value: "1",
			Start: start, End: start.Add(5 * time.Millisecond)},
		// Get a returns "1" — concurrent with Put, ok
		{ID: 2, Type: OpGet, Key: "a", Value: "1", Found: true,
			Start: start.Add(1 * time.Millisecond), End: start.Add(6 * time.Millisecond)},
		// Get a returns "2" — but there's no Put(a=2), so this is impossible
		{ID: 3, Type: OpGet, Key: "a", Value: "2", Found: true,
			Start: start.Add(2 * time.Millisecond), End: start.Add(7 * time.Millisecond)},
	}
	if err := Check(ops); err == nil {
		t.Errorf("expected non-linearizable history to be detected, but Check returned nil")
	} else {
		t.Logf("correctly detected non-linearizable history: %v", err)
	}
}

// TestCheckDeleteLinearizable: Put → Delete → Get(notFound) is linearizable.
func TestCheckDeleteLinearizable(t *testing.T) {
	start := time.Now()
	ops := []Op{
		{ID: 1, Type: OpPut, Key: "k", Value: "v", Start: start, End: start.Add(1 * time.Millisecond)},
		{ID: 2, Type: OpDelete, Key: "k", Start: start.Add(2 * time.Millisecond), End: start.Add(3 * time.Millisecond)},
		{ID: 3, Type: OpGet, Key: "k", Value: "", Found: false, Start: start.Add(4 * time.Millisecond), End: start.Add(5 * time.Millisecond)},
	}
	if err := Check(ops); err != nil {
		t.Errorf("expected linearizable: %v", err)
	}
}

// TestConcurrentStoreLinearizable runs many goroutines concurrently
// against a real Store, records every operation, and then verifies the
// recorded history is linearizable. This is the integration test that
// connects the linearizability checker to the actual storage engine.
func TestConcurrentStoreLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping linearizability check with -short")
	}

	walPath := filepath.Join(t.TempDir(), "lin.wal")
	s, err := store.Open(walPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	rec := &Recorder{}
	const goroutines = 6
	const opsEach = 8
	const keySpace = 3 // tiny key space: forces maximum concurrency on same keys

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("k%d", (gid+i)%keySpace)
				switch (gid + i) % 3 {
				case 0:
					val := fmt.Sprintf("v-%d-%d", gid, i)
					id, start := rec.Begin()
					_ = s.Put([]byte(key), []byte(val))
					rec.RecordPut(id, start, key, val)
				case 1:
					id, start := rec.Begin()
					v, found := s.Get([]byte(key))
					rec.RecordGet(id, start, key, string(v), found)
				case 2:
					id, start := rec.Begin()
					_ = s.Delete([]byte(key))
					rec.RecordDelete(id, start, key)
				}
			}
		}(g)
	}
	wg.Wait()

	ops := rec.Ops()
	t.Logf("checking linearizability of %d concurrent operations", len(ops))

	if err := Check(ops); err != nil {
		t.Errorf("Store operations are NOT linearizable: %v", err)
		for i, op := range ops {
			t.Logf("  op[%d]: %s %s=%q found=%v [%v → %v]",
				i, op.Type, op.Key, op.Value, op.Found,
				op.Start.Format("15:04:05.000000"),
				op.End.Format("15:04:05.000000"))
		}
	} else {
		t.Logf("linearizability check PASSED for %d operations", len(ops))
	}
}
