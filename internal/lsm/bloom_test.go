package lsm

import (
	"fmt"
	"testing"
)

func TestBloomNoFalseNegativesAndLowFPRate(t *testing.T) {
	const n = 20000
	var hashes [][2]uint32
	for i := 0; i < n; i++ {
		h1, h2 := bloomHash(fmt.Sprintf("in-%d", i))
		hashes = append(hashes, [2]uint32{h1, h2})
	}
	b := newBloom(hashes)
	for i := 0; i < n; i++ {
		if !b.mayContain(fmt.Sprintf("in-%d", i)) {
			t.Fatalf("false negative for in-%d", i)
		}
	}
	fp := 0
	for i := 0; i < n; i++ {
		if b.mayContain(fmt.Sprintf("out-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / n
	t.Logf("false positive rate: %.2f%% (%d bits for %d keys)", rate*100, len(b.bits)*64, n)
	if rate > 0.02 {
		t.Fatalf("false positive rate %.2f%%, want ~1%%", rate*100)
	}
}

// Misses inside every table's key range must be answered without reading
// any table, and hits (including tombstones) must still be found.
func TestBloomSkipsTablesOnMiss(t *testing.T) {
	e := missEngine(t)
	defer e.Close()
	skipped, total := 0, 0
	e.mu.RLock()
	for i := 1; i < 6000; i += 2 {
		k := fmt.Sprintf("k%07d", i)
		for _, sst := range e.l0 {
			total++
			if !sst.filter.mayContain(k) {
				skipped++
			}
		}
	}
	e.mu.RUnlock()
	if float64(skipped)/float64(total) < 0.97 {
		t.Fatalf("bloom filters skipped only %d of %d absent-key table probes", skipped, total)
	}
	e.Delete([]byte("k0000000"))
	e.Flush()
	if _, ok := e.Get([]byte("k0000000")); ok {
		t.Fatal("tombstone in a newer table was skipped by its filter")
	}
	if v, ok := e.Get([]byte("k0000006")); !ok || string(v) != "value" {
		t.Fatalf("present key not found: (%q, %v)", v, ok)
	}
}
