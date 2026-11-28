package linearcheck

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// genHistory builds a history that is linearizable by construction: each op
// gets a linearization point in increasing order, its result is computed by
// running the ops sequentially in that order, and its [Start, End] interval
// is stretched randomly around the point so many ops overlap.
func genHistory(seed int64, n, keys int, maxStretch time.Duration) []Op {
	r := rand.New(rand.NewSource(seed))
	base := time.Unix(0, 0)
	state := map[string]string{}
	ops := make([]Op, n)
	for i := range ops {
		point := base.Add(time.Duration(i) * time.Microsecond)
		key := fmt.Sprintf("k%d", r.Intn(keys))
		op := Op{ID: int64(i), Key: key,
			Start: point.Add(-time.Duration(r.Int63n(int64(maxStretch)))),
			End:   point.Add(time.Duration(r.Int63n(int64(maxStretch))))}
		switch r.Intn(4) {
		case 0:
			op.Type, op.Value = OpPut, fmt.Sprintf("v%d", i)
			state[key] = op.Value
		case 1:
			op.Type = OpDelete
			delete(state, key)
		default:
			op.Type = OpGet
			op.Value, op.Found = state[key]
		}
		ops[i] = op
	}
	r.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
	return ops
}

// TestCheckScalesToLargeHistories: 3,000 ops with each op overlapping ~20
// others. The unmemoised checker took minutes on a 60-op history from the
// raftkv tests; this must take well under a second per seed.
func TestCheckScalesToLargeHistories(t *testing.T) {
	for seed := int64(0); seed < 5; seed++ {
		ops := genHistory(seed, 3000, 3, 10*time.Microsecond)
		start := time.Now()
		if err := Check(ops); err != nil {
			t.Fatalf("seed %d: linearizable-by-construction history rejected: %v", seed, err)
		}
		if d := time.Since(start); d > 10*time.Second { // generous for slow CI; the old checker never finished
			t.Fatalf("seed %d: Check took %v", seed, d)
		}
	}
}

// TestCheckFindsViolationInLargeHistory: corrupting one Get anywhere in a
// large history must be detected — memoisation must not cause false passes.
func TestCheckFindsViolationInLargeHistory(t *testing.T) {
	for seed := int64(0); seed < 5; seed++ {
		ops := genHistory(seed, 2000, 3, 10*time.Microsecond)
		for i := range ops {
			if ops[i].Type == OpGet {
				ops[i].Value, ops[i].Found = "never-written", true
				break
			}
		}
		if err := Check(ops); err == nil {
			t.Fatalf("seed %d: history with an impossible read passed", seed)
		}
	}
}

// TestCheckFindsStaleRead: a read that returns a value overwritten before
// the read began is a classic violation that only real-time order exposes.
func TestCheckFindsStaleRead(t *testing.T) {
	at := func(us int) time.Time { return time.Unix(0, 0).Add(time.Duration(us) * time.Microsecond) }
	ops := []Op{
		{Type: OpPut, Key: "k", Value: "old", Start: at(0), End: at(1)},
		{Type: OpPut, Key: "k", Value: "new", Start: at(2), End: at(3)},
		{Type: OpGet, Key: "k", Value: "old", Found: true, Start: at(4), End: at(5)},
	}
	if err := Check(ops); err == nil {
		t.Fatal("stale read passed")
	}
	ops[2].Start = at(2) // now overlaps the second Put: "old" is legal
	if err := Check(ops); err != nil {
		t.Fatalf("concurrent read rejected: %v", err)
	}
}
