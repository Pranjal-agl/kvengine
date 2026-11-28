package lsm

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gate blocks compactions until opened, and can make them fail.
type gate struct {
	mu      sync.Mutex
	once    sync.Once
	open    chan struct{}
	fail    error
	entered atomic.Int32
}

func newGate() *gate { return &gate{open: make(chan struct{})} }

func (g *gate) hook() error {
	g.entered.Add(1)
	<-g.open
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fail
}

func (g *gate) release() { g.once.Do(func() { close(g.open) }) }

// fillL0 writes until L0 has n files, flushing after each batch.
func fillL0(t *testing.T, e *Engine, n int, tag string) {
	t.Helper()
	for i := 0; l0Files(e) < n; i++ {
		if err := e.Put([]byte(fmt.Sprintf("%s-%05d", tag, i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if i%20 == 19 {
			if err := e.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func l0Files(e *Engine) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.levels[0])
}

// Writes carry on while a compaction is running; they used to wait for it.
func TestWritesDontWaitForCompaction(t *testing.T) {
	g := newGate()
	opts := tiny()
	opts.compactHook = g.hook
	e, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer g.release() // before Close, which waits for the compactor

	fillL0(t, e, maxL0Tables, "a") // triggers a compaction, which blocks
	waitFor(t, func() bool { return g.entered.Load() > 0 }, "compaction to start")

	// Flushes keep adding L0 files while the compaction is stuck, up to
	// the stop limit, and every write in between completes.
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for i := 0; l0Files(e) < l0StopWrites-1; i++ {
			if err := e.Put([]byte(fmt.Sprintf("b-%05d", i)), []byte("v")); err != nil {
				done <- err
				return
			}
			if i%20 == 19 {
				e.Flush()
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked behind a running compaction")
	}
	if v, ok := e.Get([]byte("a-00000")); !ok || string(v) != "v" {
		t.Fatalf("read during compaction: (%q, %v)", v, ok)
	}

	g.release()
	checkLevelInvariants(t, e)
	if s := e.Stats(); s["write_stalls"].(int64) != 0 {
		t.Fatalf("write_stalls = %v, want 0 below the L0 limit", s["write_stalls"])
	}
}

// Once L0 reaches l0StopWrites, a flushing write waits for compaction to
// bring it back down, so reads can't degrade without bound.
func TestWritesStallAtL0Limit(t *testing.T) {
	g := newGate()
	opts := tiny()
	opts.compactHook = g.hook
	e, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer g.release() // before Close, which waits for the compactor

	fillL0(t, e, l0StopWrites, "a")
	// L0 is at the limit: a write that fills the MemTable must wait before
	// it can swap in a new one. (Flush itself never stalls.)
	big := make([]byte, opts.MemTableSize)
	putDone := make(chan error, 1)
	go func() { putDone <- e.Put([]byte("big"), big) }()
	select {
	case err := <-putDone:
		t.Fatalf("write returned (%v) with L0 at %d files and compaction blocked", err, l0Files(e))
	case <-time.After(300 * time.Millisecond):
	}
	g.release()
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write still stalled after compaction resumed")
	}
	if s := e.Stats(); s["write_stalls"].(int64) == 0 {
		t.Fatal("write_stalls = 0")
	}
	checkLevelInvariants(t, e)
}

// A failing compaction must not hang writers at the L0 limit: they get
// the error. Once compaction succeeds again, writes go through.
func TestCompactionErrorReachesStalledWriters(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	opts := tiny()
	opts.compactHook = func() error {
		if fail.Load() {
			return errors.New("disk full")
		}
		return nil
	}
	e, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var werr error
	for i := 0; i < 100000 && werr == nil; i++ {
		werr = e.Put([]byte(fmt.Sprintf("k%06d", i)), []byte("vvvvvvvvvvvvvvvv"))
	}
	if werr == nil || !errors.Is(werr, errors.Unwrap(werr)) || l0Files(e) < l0StopWrites {
		t.Fatalf("writes never failed (err %v, L0 %d files)", werr, l0Files(e))
	}
	t.Logf("write error: %v", werr)
	if s := e.Stats(); s["compaction_error"] != "disk full" {
		t.Fatalf("Stats compaction_error = %q", s["compaction_error"])
	}

	fail.Store(false)
	waitFor(t, func() bool { return l0Files(e) < maxL0Tables }, "compaction to recover")
	if err := e.Put([]byte("after"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if s := e.Stats(); s["compaction_error"] != "" {
		t.Fatalf("compaction_error still %q after recovery", s["compaction_error"])
	}
}

// Readers and writers run against continuous background compaction; every
// read returns the latest acknowledged value for its key.
func TestReadsDuringBackgroundCompaction(t *testing.T) {
	e, err := OpenWithOptions(t.TempDir(), tiny())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const writers, keysPer = 4, 200
	rounds := 30
	if raceEnabled {
		rounds = 6 // the race detector makes this ~30x slower
	}
	var latest [writers][keysPer]atomic.Int64 // last acknowledged round, -1 = none
	for w := range latest {
		for k := range latest[w] {
			latest[w][k].Store(-1)
		}
	}
	key := func(w, k int) []byte { return []byte(fmt.Sprintf("w%d-k%04d", w, k)) }

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errc := make(chan error, 16)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				for k := 0; k < keysPer; k++ {
					if err := e.Put(key(w, k), []byte(fmt.Sprintf("%08d", r))); err != nil {
						errc <- err
						return
					}
					latest[w][k].Store(int64(r))
				}
			}
		}(w)
	}
	var rg sync.WaitGroup
	for rd := 0; rd < 4; rd++ {
		rg.Add(1)
		go func(rd int) {
			defer rg.Done()
			for i := rd; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				w, k := i%writers, (i/writers)%keysPer
				before := latest[w][k].Load()
				v, ok, err := e.GetErr(key(w, k))
				if err != nil {
					errc <- err
					return
				}
				if before < 0 {
					continue
				}
				var got int64 = -1
				if ok {
					fmt.Sscanf(string(v), "%d", &got)
				}
				if got < before {
					errc <- fmt.Errorf("%s: read round %d (found=%v) after round %d was acknowledged", key(w, k), got, ok, before)
					return
				}
			}
		}(rd)
	}
	wg.Wait()
	close(stop)
	rg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	s := e.Stats()
	if s["compactions"].(int64) == 0 {
		t.Fatal("no compactions ran; the test exercised nothing")
	}
	t.Logf("compactions %v, levels %v, stalls %v", s["compactions"], s["level_files"], s["write_stalls"])
	checkLevelInvariants(t, e)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// pickCompaction compacts the level furthest over budget, not L0 whenever
// it is full: otherwise a steady write load starves L1 (see pickCompaction).
func TestPickCompactionScoresLevels(t *testing.T) {
	e := &Engine{opts: Options{LevelSizeBase: 1000}}
	files := func(n int, size int64) []*SSTable {
		var out []*SSTable
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%03d", i)
			out = append(out, &SSTable{id: i, size: size, minKey: k, maxKey: k})
		}
		return out
	}
	cases := []struct {
		l0, l1Bytes, l2Bytes int64
		want                 int // -1: nothing
	}{
		{0, 0, 0, -1},
		{3, 900, 9000, -1},    // everything under budget
		{4, 900, 0, 0},        // L0 full
		{4, 5000, 0, 1},       // L1 at 5x beats L0 at 1x
		{8, 1500, 0, 0},       // L0 at 2x beats L1 at 1.5x
		{4, 2000, 50000, 2},   // L2 at 5x is the worst
		{3, 1000, 0, -1},      // exactly at budget is not over
		{12, 11000, 20000, 1}, // L1 11x > L0 3x > L2 2x
	}
	for i, c := range cases {
		e.levels = [numLevels][]*SSTable{}
		e.levels[0] = files(int(c.l0), 10)
		if c.l1Bytes > 0 {
			e.levels[1] = files(1, c.l1Bytes)
		}
		if c.l2Bytes > 0 {
			e.levels[2] = files(1, c.l2Bytes)
		}
		got := -1
		if p := e.pickCompaction(); p != nil {
			got = p.outLevel - 1
		}
		if got != c.want {
			t.Errorf("case %d (L0 %d files, L1 %dB, L2 %dB): compacted L%d, want L%d", i, c.l0, c.l1Bytes, c.l2Bytes, got, c.want)
		}
	}
}

// An engine reopened with L0 already backed up compacts straight away. It
// used to wait for the next flush to wake the compactor, so a write that
// stalled on L0 waited forever.
func TestReopenWithFullL0StartsCompacting(t *testing.T) {
	dir := t.TempDir()
	opts := tiny()
	opts.compactHook = func() error { return errors.New("compaction off") }
	e, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; l0Files(e) < l0StopWrites+2; i++ {
		e.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
		if i%20 == 19 {
			e.Flush()
		}
	}
	e.Close()

	e, err = OpenWithOptions(dir, tiny())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	waitFor(t, func() bool { return l0Files(e) < maxL0Tables }, "compaction to start after reopen")
	done := make(chan error, 1)
	go func() { done <- e.Put([]byte("after"), make([]byte, tiny().MemTableSize)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write hung after reopen")
	}
}
