package raftkv

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"kvengine/internal/linearcheck"
	"kvengine/internal/raft"
)

// A follower read issued right after a write is acknowledged must see the
// write. With local (stale) reads this regularly fails, because the
// follower hasn't applied the entry yet.
func TestFollowerReadsSeeAcknowledgedWrites(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	l := c.leader()
	f := (l + 1) % 3
	waitFor(t, "follower to learn leader", func() bool { return c.nodes[f].State().LeaderId != "" })
	for i := 0; i < 50; i++ {
		v := []byte(fmt.Sprintf("v%d", i))
		if err := c.kvs[l].Put([]byte("k"), v); err != nil {
			t.Fatal(err)
		}
		got, ok, err := c.kvs[f].GetErr([]byte("k"))
		if err != nil {
			t.Fatalf("follower read %d: %v", i, err)
		}
		if !ok || string(got) != string(v) {
			t.Fatalf("follower read %d returned %q, want %q (stale read)", i, got, v)
		}
	}
}

// A leader cut off by a partition still thinks it is leader for a while,
// and once the majority elects a new leader and writes, its local state is
// stale. A linearizable read on it must fail rather than return old data.
func TestDeposedLeaderCannotServeStaleRead(t *testing.T) {
	c := newTestCluster(t, 3, Options{ReadTimeout: 500 * time.Millisecond, WriteTimeout: time.Second})
	l := c.leader()
	if err := c.kvs[l].Put([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	c.isolate(l)

	var nl int
	waitFor(t, "new leader on majority side", func() bool {
		for i, n := range c.nodes {
			if i != l && n.State().Role == raft.Leader {
				nl = i
				return true
			}
		}
		return false
	})
	if err := c.kvs[nl].Put([]byte("k"), []byte("new")); err != nil {
		t.Fatal(err)
	}

	// The old node's local state is stale; a stale read shows it.
	if v, _ := c.kvs[l].Get([]byte("k")); string(v) != "old" {
		t.Skipf("old leader already caught up (%q); partition didn't hold", v)
	}
	v, _, err := c.kvs[l].GetErr([]byte("k"))
	if err == nil {
		t.Fatalf("deposed leader served a read (%q) without a quorum", v)
	}
	t.Logf("deposed leader refused the read: %v", err)

	// The new leader and its follower see the new value.
	for i := range c.kvs {
		if i == l {
			continue
		}
		v, _, err := c.kvs[i].GetErr([]byte("k"))
		if err != nil || string(v) != "new" {
			t.Fatalf("node %d: got (%q, %v), want new", i, v, err)
		}
	}
}

func TestStaleReadsOption(t *testing.T) {
	c := newTestCluster(t, 3, Options{StaleReads: true})
	l := c.leader()
	c.isolate(l)
	// With StaleReads, even an isolated node answers from local state.
	if _, _, err := c.kvs[l].GetErr([]byte("anything")); err != nil {
		t.Fatalf("stale read failed: %v", err)
	}
}

// With the rest of the cluster down, a follower read must fail promptly
// rather than hang or fall back to (stale) local state.
func TestFollowerReadWithNoQuorum(t *testing.T) {
	c := newTestCluster(t, 3, Options{ReadTimeout: 300 * time.Millisecond})
	l := c.leader()
	f := (l + 1) % 3
	waitFor(t, "follower to learn leader", func() bool { return c.nodes[f].State().LeaderId != "" })
	c.stop(l)
	c.stop((l + 2) % 3)
	start := time.Now()
	if _, _, err := c.kvs[f].GetErr([]byte("k")); err == nil {
		t.Fatal("read succeeded with no quorum")
	} else {
		t.Logf("refused: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("read took %v to fail", d)
	}
	c.start(l) // so cleanup has live nodes to stop
	c.start((l + 2) % 3)
}

// TestLinearizableHistory records a concurrent workload (writes through the
// leader, reads from every node) and checks the whole history with the
// Wing-Gong checker in internal/linearcheck.
func TestLinearizableHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	for round := 0; round < 3; round++ {
		c := newTestCluster(t, 3, Options{})
		l := c.leader()
		for i := range c.nodes {
			waitFor(t, "followers to learn leader", func() bool { return c.nodes[i].State().LeaderId != "" })
		}

		rec := &linearcheck.Recorder{}
		const clients, opsEach, keys = 6, 10, 2
		var wg sync.WaitGroup
		var failed sync.Once
		for g := 0; g < clients; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(round*100 + g)))
				for i := 0; i < opsEach; i++ {
					key := fmt.Sprintf("k%d", r.Intn(keys))
					if r.Intn(3) == 0 {
						val := fmt.Sprintf("v%d-%d", g, i)
						id, start := rec.Begin()
						if err := c.kvs[l].Put([]byte(key), []byte(val)); err != nil {
							// An unacknowledged write might still apply, which the
							// checker can't model; a healthy cluster shouldn't fail.
							failed.Do(func() { t.Errorf("Put: %v", err) })
							return
						}
						rec.RecordPut(id, start, key, val)
					} else {
						node := r.Intn(len(c.kvs))
						id, start := rec.Begin()
						v, ok, err := c.kvs[node].GetErr([]byte(key))
						if err != nil {
							continue // a failed read has no effect; leave it out
						}
						rec.RecordGet(id, start, key, string(v), ok)
					}
				}
			}(g)
		}
		wg.Wait()
		ops := rec.Ops()
		if err := linearcheck.Check(ops); err != nil {
			t.Fatalf("round %d: history of %d ops is not linearizable: %v", round, len(ops), err)
		}
		t.Logf("round %d: %d ops linearizable", round, len(ops))
	}
}
