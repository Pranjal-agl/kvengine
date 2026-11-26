package raft

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestVoteNotResetBySameTermHeartbeat: once a node has voted in term T, a
// heartbeat from the leader of term T must not erase that vote. Otherwise a
// delayed RequestVote from a second candidate in the same term could win a
// second vote — two leaders in one term, violating Election Safety.
func TestVoteNotResetBySameTermHeartbeat(t *testing.T) {
	tr := NewMemTransport()
	n, err := NewNode("x", []string{"a", "b"}, tr, make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	if r := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateId: "a"}); !r.VoteGranted {
		t.Fatalf("first vote in term 5 should be granted")
	}
	// "a" won and sends its first heartbeat in term 5.
	n.HandleAppendEntries(AppendEntriesArgs{Term: 5, LeaderId: "a"})
	// A delayed RequestVote from "b", also in term 5.
	if r := n.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateId: "b"}); r.VoteGranted {
		t.Fatalf("node voted twice in term 5 (for a, then b): Election Safety violated")
	}
}

// --- Snapshot harness ---------------------------------------------------
//
// smNode pairs a Raft node with a tiny state machine (the ordered list of
// applied commands) and an apply goroutine that snapshots every
// snapshotEvery entries — the same pattern cmd/kvengine uses.

type smNode struct {
	mu      sync.Mutex
	applied []string // state machine: commands in apply order
	lastIdx uint64
	err     error
}

func (s *smNode) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, _ := json.Marshal(s.applied)
	return b
}

func (s *smNode) get() ([]string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.applied...), s.lastIdx, s.err
}

type snapCluster struct {
	t         *testing.T
	dir       string
	ids       []string
	transport *MemTransport
	nodes     []*Node
	sms       []*smNode
	every     uint64
}

func newSnapCluster(t *testing.T, n int, every uint64) *snapCluster {
	c := &snapCluster{
		t:         t,
		dir:       t.TempDir(),
		transport: NewMemTransport(),
		nodes:     make([]*Node, n),
		sms:       make([]*smNode, n),
		every:     every,
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("s%d", i))
	}
	for i := range c.ids {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *snapCluster) start(i int) {
	id := c.ids[i]
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	ch := make(chan ApplyMsg, 64)
	sm := &smNode{}
	node, err := NewNode(id, peers, c.transport, ch,
		filepath.Join(c.dir, id+"-state.json"), filepath.Join(c.dir, id+"-snap.json"))
	if err != nil {
		c.t.Fatalf("NewNode: %v", err)
	}
	// Restore the state machine from the node's snapshot, as a real
	// application must on boot.
	if snap, _ := node.LatestSnapshot(); snap != nil {
		json.Unmarshal(snap.Data, &sm.applied)
		sm.lastIdx = snap.LastIncludedIndex
	}
	c.nodes[i], c.sms[i] = node, sm

	go func() {
		for msg := range ch {
			sm.mu.Lock()
			if msg.SnapshotValid {
				var st []string
				json.Unmarshal(msg.Snapshot, &st)
				sm.applied, sm.lastIdx = st, msg.SnapshotIndex
				sm.mu.Unlock()
				continue
			}
			if msg.CommandIndex != sm.lastIdx+1 && sm.err == nil {
				sm.err = fmt.Errorf("%s: applied index %d after %d (gap or reorder)", id, msg.CommandIndex, sm.lastIdx)
			}
			sm.applied = append(sm.applied, string(msg.Command))
			sm.lastIdx = msg.CommandIndex
			doSnap := sm.lastIdx%c.every == 0
			sm.mu.Unlock()
			if doSnap {
				node.TakeSnapshot(msg.CommandIndex, sm.snapshot())
			}
		}
	}()
}

func (c *snapCluster) leader() int {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if n.State().Role == Leader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

// proposeAll submits cmds through whichever node is leader, retrying if
// leadership moves.
func (c *snapCluster) proposeAll(cmds []string) {
	for _, cmd := range cmds {
		for attempt := 0; ; attempt++ {
			if _, _, ok := c.nodes[c.leader()].Propose([]byte(cmd)); ok {
				break
			}
			if attempt > 50 {
				c.t.Fatalf("could not propose %q", cmd)
			}
		}
	}
}

// waitState waits until every listed node's state machine equals want.
func (c *snapCluster) waitState(nodes []int, want []string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		allOK := true
		var last string
		for _, i := range nodes {
			got, idx, err := c.sms[i].get()
			if err != nil {
				c.t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				allOK = false
				last = fmt.Sprintf("node %d at index %d has %d entries, want %d", i, idx, len(got), len(want))
			}
		}
		if allOK {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("state machines did not converge: %s", last)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func cmds(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

// TestReplicationContinuesAfterSnapshot: every node snapshots every 5
// entries. The cluster must keep committing and applying long after the
// first snapshot. Before the fix, log positions were treated as absolute
// Raft indices, so after the first compaction commitIndex stopped advancing
// (or applyLoop indexed past the end of the log and panicked).
func TestReplicationContinuesAfterSnapshot(t *testing.T) {
	c := newSnapCluster(t, 3, 5)
	want := cmds("c", 37)
	c.proposeAll(want)
	c.waitState([]int{0, 1, 2}, want, 5*time.Second)

	// Indices returned by Propose must keep counting from the real log
	// position, not restart from the compacted slice.
	l := c.leader()
	idx, _, ok := c.nodes[l].Propose([]byte("next"))
	if !ok {
		t.Fatal("propose failed")
	}
	if idx != 38 {
		t.Fatalf("Propose after snapshots returned index %d, want 38", idx)
	}
}

// TestLaggingFollowerCatchesUpViaInstallSnapshot: a follower is cut off
// while the leader commits and compacts past the follower's position. On
// healing, the entries it needs no longer exist in the leader's log, so the
// only way to catch up is InstallSnapshot.
func TestLaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	c := newSnapCluster(t, 3, 5)
	c.proposeAll(cmds("a", 3))
	c.waitState([]int{0, 1, 2}, cmds("a", 3), 5*time.Second)

	l := c.leader()
	lag := (l + 1) % 3
	for _, other := range c.ids {
		if other != c.ids[lag] {
			c.transport.Partition(other, c.ids[lag])
			c.transport.Partition(c.ids[lag], other)
		}
	}

	want := append(cmds("a", 3), cmds("b", 30)...)
	c.proposeAll(cmds("b", 30))
	var live []int
	for i := range c.nodes {
		if i != lag {
			live = append(live, i)
		}
	}
	c.waitState(live, want, 5*time.Second)

	c.transport.HealAll()
	c.proposeAll([]string{"after-heal"})
	want = append(want, "after-heal")
	c.waitState([]int{0, 1, 2}, want, 10*time.Second)
}

// TestRestartWithSnapshotKeepsIndices: a node that restarts from a snapshot
// plus a persisted log tail must rebuild the same absolute indices, then
// rejoin and keep applying.
func TestRestartWithSnapshotKeepsIndices(t *testing.T) {
	c := newSnapCluster(t, 3, 5)
	want := cmds("r", 12)
	c.proposeAll(want)
	c.waitState([]int{0, 1, 2}, want, 5*time.Second)

	victim := (c.leader() + 1) % 3
	c.nodes[victim].Stop()
	c.transport.Unregister(c.ids[victim])
	c.start(victim)

	more := cmds("s", 9)
	c.proposeAll(more)
	want = append(want, more...)
	c.waitState([]int{0, 1, 2}, want, 10*time.Second)
}
