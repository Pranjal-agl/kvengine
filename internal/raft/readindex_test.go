package raft

import (
	"errors"
	"sync/atomic"
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

// TestConcurrentReadsShareRounds: reads that arrive while a confirmation
// round is in flight must share the next round, not each broadcast their
// own. Hold the round open with a brief partition (shorter than an
// election timeout), fire many reads, heal, and count rounds.
func TestConcurrentReadsShareRounds(t *testing.T) {
	c := newCluster(t, 3)
	l := c.assertOneLeader(t)
	c.propose(t, l, []byte("w"))
	c.waitApplied(t, l, 1, 3*time.Second)
	leader := c.nodes[l]

	for i := range c.nodes {
		if i != l {
			c.transport.Partition(leader.id, c.nodes[i].id)
		}
	}
	leader.mu.Lock()
	before := leader.hbRound
	leader.mu.Unlock()

	const reads = 20
	errs := make(chan error, reads)
	for i := 0; i < reads; i++ {
		go func() {
			_, err := leader.ReadIndex(2 * time.Second)
			errs <- err
		}()
	}
	time.Sleep(80 * time.Millisecond) // all reads arrive while round 1 is stuck
	c.transport.HealAll()
	for i := 0; i < reads; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	leader.mu.Lock()
	rounds := leader.hbRound - before
	leader.mu.Unlock()
	if rounds > 3 {
		t.Fatalf("%d reads used %d rounds; want them batched into ~2", reads, rounds)
	}
	t.Logf("%d concurrent reads used %d confirmation rounds", reads, rounds)
}

// After a membership change the newest committed entry is a config entry,
// which (like a no-op) is never delivered on applyCh. The read index must
// skip it, or every linearizable read would hang until the next write.
func TestReadIndexSkipsConfigEntries(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("w")
	c.start("m3", Membership{})
	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m3", "") })
	_, l := c.leader()
	idx, err := l.ReadIndex(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	e := l.entryAt(idx)
	l.mu.Unlock()
	if !e.delivered() {
		t.Fatalf("read index %d points at an undelivered entry (kind %d)", idx, e.Kind)
	}
}

// hangingReadIndex delays ReadIndex RPCs while hang is set, like a leader
// that is frozen (SIGSTOP, GC pause, overloaded) with its connections open.
type hangingReadIndex struct {
	*MemTransport
	hang    atomic.Bool
	release chan struct{}
}

func (h *hangingReadIndex) ReadIndex(peer string, args ReadIndexArgs) (ReadIndexReply, error) {
	if h.hang.Load() {
		<-h.release
	}
	return h.MemTransport.ReadIndex(peer, args)
}

// TestFollowerReadIndexHonoursTimeout: a follower's ReadIndex returns by
// its deadline even when the RPC to the leader doesn't. It used to wait for
// the transport's own timeout (10 s over TCP, with its retry).
func TestFollowerReadIndexHonoursTimeout(t *testing.T) {
	tr := &hangingReadIndex{MemTransport: NewMemTransport(), release: make(chan struct{})}
	ids := []string{"a", "b", "c"}
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		n, err := NewNode(id, peers, tr, make(chan ApplyMsg, 256), "")
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		close(tr.release)
		for _, n := range nodes {
			n.Stop()
		}
	})

	var follower *Node
	deadline := time.Now().Add(3 * time.Second)
	for follower == nil && time.Now().Before(deadline) {
		for _, n := range nodes {
			if st := n.State(); st.Role != Leader && st.LeaderId != "" {
				follower = n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if follower == nil {
		t.Fatal("no follower with a known leader")
	}
	if _, err := follower.ReadIndex(time.Second); err != nil {
		t.Fatalf("ReadIndex before the hang: %v", err)
	}

	tr.hang.Store(true)
	start := time.Now()
	_, err := follower.ReadIndex(200 * time.Millisecond)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrReadTimeout) {
		t.Fatalf("err = %v, want ErrReadTimeout", err)
	}
	if elapsed > time.Second {
		t.Fatalf("ReadIndex(200ms) took %v", elapsed)
	}
}
