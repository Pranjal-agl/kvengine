package raft

import (
	"errors"
	"testing"
	"time"
)

// TestReadIndexOnLeaderAndFollower: both paths return an index covering
// every write committed before the call.
func TestReadIndexOnLeaderAndFollower(t *testing.T) {
	c := newCluster(t, 3)
	l := c.assertOneLeader(t)
	var last uint64
	for i := 0; i < 5; i++ {
		last = c.propose(t, l, []byte("w"))
	}
	c.waitApplied(t, l, 5, 3*time.Second)

	for i, n := range c.nodes {
		if i != l {
			// Make sure the follower knows who the leader is.
			deadline := time.Now().Add(2 * time.Second)
			for n.State().LeaderId == "" && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
		}
		idx, err := n.ReadIndex(time.Second)
		if err != nil {
			t.Fatalf("node %d ReadIndex: %v", i, err)
		}
		if idx < last {
			t.Fatalf("node %d: read index %d is behind committed write at %d", i, idx, last)
		}
	}
}

// TestReadIndexFailsOnIsolatedLeader: the core safety property. A leader
// cut off from the majority still believes it is leader for a while, and
// its local state is stale once the majority elects someone else. It must
// not hand out a read index.
func TestReadIndexFailsOnIsolatedLeader(t *testing.T) {
	c := newCluster(t, 3)
	l := c.assertOneLeader(t)
	c.propose(t, l, []byte("w"))
	c.waitApplied(t, l, 1, 3*time.Second)

	for i := range c.nodes {
		if i != l {
			c.transport.Partition(c.nodes[l].id, c.nodes[i].id)
			c.transport.Partition(c.nodes[i].id, c.nodes[l].id)
		}
	}
	if c.nodes[l].State().Role != Leader {
		t.Skip("leader stepped down before the read; nothing to test")
	}
	start := time.Now()
	idx, err := c.nodes[l].ReadIndex(300 * time.Millisecond)
	if err == nil {
		t.Fatalf("isolated leader returned read index %d", idx)
	}
	if !errors.Is(err, ErrReadTimeout) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("unexpected error: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("ReadIndex took %v, should respect its timeout", time.Since(start))
	}
}

// TestReadIndexWaitsForCurrentTermCommit: a brand-new leader must not use
// its commitIndex until it has committed an entry from its own term.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	tr := NewMemTransport()
	ch := make(chan ApplyMsg, 8)
	n, err := NewNode("solo", nil, tr, ch, "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for n.State().Role != Leader && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := n.ReadIndex(time.Second); err != nil {
		t.Fatalf("single-node ReadIndex: %v", err)
	}
	n.mu.Lock()
	term := n.termAt(n.commitIndex)
	cur := n.currentTerm
	n.mu.Unlock()
	if term != cur {
		t.Fatalf("ReadIndex returned before an entry of term %d committed (commit term %d)", cur, term)
	}
}

// TestFollowerReadIndexWithUnknownLeader fails fast instead of hanging.
func TestFollowerReadIndexWithUnknownLeader(t *testing.T) {
	tr := NewMemTransport()
	n, err := NewNode("f", []string{"a", "b"}, tr, make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if _, err := n.ReadIndex(time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v, want ErrNotLeader", err)
	}
}

// TestReadIndexSkipsNoOps: right after an election the commit index is the
// leader's no-op, which is never delivered on applyCh. The read index must
// point at the last real command so callers watching applyCh can reach it.
func TestReadIndexSkipsNoOps(t *testing.T) {
	c := newCluster(t, 3)
	l := c.assertOneLeader(t)
	last := c.propose(t, l, []byte("w"))
	c.waitApplied(t, l, 1, 3*time.Second)

	// Force a new election so the newest committed entry is a no-op.
	old := c.nodes[l]
	for i := range c.nodes {
		if i != l {
			c.transport.Partition(old.id, c.nodes[i].id)
			c.transport.Partition(c.nodes[i].id, old.id)
		}
	}
	var nl int
	deadline := time.Now().Add(5 * time.Second)
	for nl = -1; nl < 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for i, n := range c.nodes {
			if i != l && n.State().Role == Leader {
				nl = i
			}
		}
	}
	if nl < 0 {
		t.Fatal("no new leader")
	}
	idx, err := c.nodes[nl].ReadIndex(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if idx != last {
		t.Fatalf("read index = %d, want %d (last real command, skipping the new no-op)", idx, last)
	}
}
