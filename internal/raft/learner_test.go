package raft

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// Adding a server that never answers used to put it in the configuration
// straight away. In a one-node cluster that made the majority two, so
// nothing could commit until the newcomer appeared. Now the leader tries
// to catch it up as a learner, gives up, and the cluster never stops
// committing.
func TestAddUnreachableServerDoesNotStallCluster(t *testing.T) {
	c := newMCluster(t, 1, "")
	c.propose("before")
	_, l := c.leader()

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		_, _, err := l.AddServerTimeout("ghost", "", 600*time.Millisecond)
		done <- result{err, time.Since(start)}
	}()

	// While the catch-up is pending, writes commit with the old majority.
	time.Sleep(50 * time.Millisecond)
	idx, term, ok := l.Propose([]byte("during"))
	if !ok {
		t.Fatal("leader refused a proposal during catch-up")
	}
	if err := l.WaitCommitted(idx, term, 300*time.Millisecond); err != nil {
		t.Fatalf("write during catch-up: %v (the learner is counted toward the majority?)", err)
	}
	if m := l.Members(); len(m) != 1 {
		t.Fatalf("members during catch-up = %v; the learner must not be a member yet", m)
	}

	r := <-done
	if !errors.Is(r.err, ErrCatchUpTimeout) {
		t.Fatalf("AddServer(ghost) = %v, want ErrCatchUpTimeout", r.err)
	}
	if r.elapsed < 500*time.Millisecond || r.elapsed > 2*time.Second {
		t.Fatalf("AddServer gave up after %v, want ~600ms", r.elapsed)
	}
	if m := l.Members(); len(m) != 1 {
		t.Fatalf("members after a failed add = %v", m)
	}
	c.propose("after")

	// The leader no longer replicates to the abandoned learner, and a
	// new membership change can start.
	l.mu.Lock()
	_, tracked := l.nextIndex["ghost"]
	l.mu.Unlock()
	if tracked {
		t.Fatal("leader still tracks the abandoned learner")
	}
	c.start("m1", Membership{})
	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m1", "") })
	c.propose("with-m1")
	c.waitApplied("m1", []string{"before", "during", "after", "with-m1"})
}

// A new server far behind (here: cut off at first) is caught up before it
// becomes a voter; the config entry is only proposed once it has the log.
func TestAddServerWaitsUntilCaughtUp(t *testing.T) {
	c := newMCluster(t, 3, "")
	var want []string
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("v%d", i)
		c.propose(v)
		want = append(want, v)
	}
	lid, l := c.leader()
	c.start("m3", Membership{})
	c.tr.Partition(lid, "m3")
	c.tr.Partition("m3", lid)

	type result struct {
		idx, term uint64
		err       error
	}
	done := make(chan result, 1)
	go func() {
		idx, term, err := l.AddServerTimeout("m3", "", 5*time.Second)
		done <- result{idx, term, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("AddServer returned (%+v) while the new server was unreachable", r)
	case <-time.After(500 * time.Millisecond):
	}
	if _, ok := l.Members()["m3"]; ok {
		t.Fatal("m3 became a member before catching up")
	}
	// A second change is refused while the first is catching up.
	if _, _, err := l.RemoveServer("m2"); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("RemoveServer during catch-up: %v, want ErrConfigChangeInProgress", err)
	}

	c.tr.HealAll()
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	// When the change is proposed, m3 already has everything before it.
	l.mu.Lock()
	match := l.matchIndex["m3"]
	l.mu.Unlock()
	if match+1 < r.idx {
		t.Fatalf("config entry at %d proposed with m3 only at %d", r.idx, match)
	}
	if err := l.WaitCommitted(r.idx, r.term, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	c.propose("last")
	c.waitApplied("m3", append(want, "last"))
}

// If the leader loses leadership during the catch-up, AddServer says so
// and the new leader knows nothing of the learner.
func TestAddServerFailsWhenLeaderChanges(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, l := c.leader()
	c.start("m3", Membership{})
	c.tr.Partition(lid, "m3") // keep the learner from catching up

	done := make(chan error, 1)
	go func() {
		_, _, err := l.AddServerTimeout("m3", "", 5*time.Second)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	// Isolate the leader: CheckQuorum steps it down.
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		if id != lid {
			c.tr.Partition(lid, id)
			c.tr.Partition(id, lid)
		}
	}
	select {
	case err := <-done:
		var nle *NotLeaderError
		if !errors.As(err, &nle) {
			t.Fatalf("AddServer = %v, want NotLeaderError", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AddServer kept waiting after the leader lost its quorum")
	}
	c.tr.HealAll()
	nl, n := c.leader()
	if _, ok := n.Members()["m3"]; ok {
		t.Fatalf("m3 is a member on %s", nl)
	}
}
