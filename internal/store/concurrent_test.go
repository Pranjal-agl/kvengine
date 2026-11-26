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

// event records one completed write with the logical times at which it
// was invoked and at which it returned, taken from one global atomic
// counter. Two writes whose [start, end] intervals overlap were concurrent,
// and a linearizable store may order them either way. (Using completion
// order alone as "the" order is not sound: with group commit, writes that
// share an fsync return at the same moment, in arbitrary goroutine order.)
type event struct {
	start, end int64
	delete     bool
	key        string
	value      string
}

// TestLinearizability runs many goroutines issuing random Put/Delete calls
// against the real Store on a tiny key space, then checks a necessary
// condition of linearizability for every key: the final state must be the
// effect of some write that no other write on that key strictly follows in
// real time (i.e. no other write was invoked after it returned). Any other
// final value means the store reordered non-overlapping writes or lost one.
//
// internal/linearcheck runs the full Wing-Gong check (including reads) on
// smaller histories; this test trades completeness for a much larger,
// higher-contention write history.
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

	var clock int64
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
				ev := event{key: key}
				ev.start = atomic.AddInt64(&clock, 1)
				var err error
				if r.Intn(4) == 0 {
					ev.delete = true
					err = s.Delete([]byte(key))
				} else {
					ev.value = fmt.Sprintf("v-%d-%d", seed, i)
					err = s.Put([]byte(key), []byte(ev.value))
				}
				ev.end = atomic.AddInt64(&clock, 1)
				if err != nil {
					t.Errorf("write: %v", err)
					return
				}
				eventsMu.Lock()
				events = append(events, ev)
				eventsMu.Unlock()
			}
		}(int64(g))
	}
	wg.Wait()

	byKey := map[string][]event{}
	for _, e := range events {
		byKey[e.key] = append(byKey[e.key], e)
	}
	for k := 0; k < keySpace; k++ {
		key := fmt.Sprintf("k%d", k)
		evs := byKey[key]
		got, present := s.Get([]byte(key))
		if len(evs) == 0 {
			if present {
				t.Errorf("key %q: never written, but store has %q", key, got)
			}
			continue
		}
		var latestStart int64
		for _, e := range evs {
			if e.start > latestStart {
				latestStart = e.start
			}
		}
		// Candidates: writes not strictly followed by another write, i.e.
		// those still in flight when the last write on this key began.
		ok := false
		var candidates []string
		for _, e := range evs {
			if e.end < latestStart {
				continue
			}
			desc := e.value
			if e.delete {
				desc = "<deleted>"
			}
			candidates = append(candidates, desc)
			if e.delete && !present || !e.delete && present && string(got) == e.value {
				ok = true
			}
		}
		if !ok {
			sort.Strings(candidates)
			t.Errorf("key %q: final state (%q, present=%v) is not the effect of any final write; candidates %v",
				key, got, present, candidates)
		}
	}
}
