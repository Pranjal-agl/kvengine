package store

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentStress hammers Put/Get/Delete from many goroutines on an
// overlapping, small key space (so collisions are frequent, not avoided) and
// must be run with -race. This is the "concurrency correctness" must-have:
// proving no data race exists, not assuming it from code review.
func TestConcurrentStress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stress.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const goroutines = 50
	const opsPerGoroutine = 200
	const keySpace = 20 // small on purpose: forces frequent overlap/contention

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("key-%d", r.Intn(keySpace)))
				switch r.Intn(3) {
				case 0:
					val := []byte(fmt.Sprintf("v-%d-%d", seed, i))
					if err := s.Put(key, val); err != nil {
						t.Errorf("Put: %v", err)
					}
				case 1:
					if err := s.Delete(key); err != nil {
						t.Errorf("Delete: %v", err)
					}
				case 2:
					s.Get(key) // result not checked here; just exercising concurrent reads
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// No assertion on final values needed here — TestConcurrentStress's job
	// is purely to be race-clean under `go test -race`. Value-correctness
	// under concurrency is checked separately by TestLinearizability below,
	// which is more precise about *what* the right answer should be.
}

// event records one completed write, in the order it was observed to
// complete. Because Store serializes all writes through a single WAL mutex
// (see internal/wal.WAL.Append/Sync), the order in which Put/Delete calls
// *return* is a valid total order of the writes that actually happened —
// this is exactly what we replay against a sequential reference model.
type event struct {
	seq    int64
	delete bool
	key    string
	value  string
}

// TestLinearizability runs many goroutines issuing random Put/Delete calls
// against the real Store, recording each operation's completion order via a
// global atomic sequence counter. It then replays that exact order against
// a trivial single-threaded reference map and asserts the reference's final
// state matches the real Store's final state for every key touched.
//
// This is the property-based check for "concurrent code doesn't corrupt
// shared state": it doesn't just check for crashes/races (TestConcurrentStress
// does that), it checks that the *values* concurrency produces are exactly
// what a correct sequential execution would have produced.
func TestLinearizability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "linearizability.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const goroutines = 30
	const opsPerGoroutine = 150
	const keySpace = 8 // deliberately tiny: maximizes contention on the same keys

	var seq int64
	var eventsMu sync.Mutex
	var events []event

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + 1000))
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("k%d", r.Intn(keySpace))
				if r.Intn(4) == 0 {
					if err := s.Delete([]byte(key)); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
					n := atomic.AddInt64(&seq, 1)
					eventsMu.Lock()
					events = append(events, event{seq: n, delete: true, key: key})
					eventsMu.Unlock()
				} else {
					val := fmt.Sprintf("v-%d-%d", seed, i)
					if err := s.Put([]byte(key), []byte(val)); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
					n := atomic.AddInt64(&seq, 1)
					eventsMu.Lock()
					events = append(events, event{seq: n, key: key, value: val})
					eventsMu.Unlock()
				}
			}
		}(int64(g))
	}
	wg.Wait()

	// Replay in completion order against a plain map (the reference model).
	sort.Slice(events, func(i, j int) bool { return events[i].seq < events[j].seq })
	reference := make(map[string]string)
	for _, e := range events {
		if e.delete {
			delete(reference, e.key)
		} else {
			reference[e.key] = e.value
		}
	}

	for key, wantVal := range reference {
		gotVal, ok := s.Get([]byte(key))
		if !ok {
			t.Errorf("key %q: reference has %q, real store has nothing", key, wantVal)
			continue
		}
		if string(gotVal) != wantVal {
			t.Errorf("key %q: reference says %q, real store says %q", key, wantVal, gotVal)
		}
	}
	// And nothing in the real store that the reference doesn't also have.
	for k := 0; k < keySpace; k++ {
		key := fmt.Sprintf("k%d", k)
		_, refOK := reference[key]
		_, realOK := s.Get([]byte(key))
		if refOK != realOK {
			t.Errorf("key %q: presence mismatch — reference=%v real=%v", key, refOK, realOK)
		}
	}
}
