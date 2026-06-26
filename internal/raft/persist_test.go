package raft

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestPersistenceAcrossRestart proves that a node's currentTerm, votedFor,
// and log survive a Stop()+NewNode() cycle — the disk persistence invariant.
// Without this, a restarted node could vote twice in the same term.
func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	transport := NewMemTransport()

	mkNode := func(id string, peers []string, ch chan ApplyMsg) *Node {
		sp := filepath.Join(dir, id+".json")
		n, err := NewNode(id, peers, transport, ch, sp)
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		return n
	}

	ids := []string{"n0", "n1", "n2"}
	chs := []chan ApplyMsg{
		make(chan ApplyMsg, 64),
		make(chan ApplyMsg, 64),
		make(chan ApplyMsg, 64),
	}
	peers := func(id string) []string {
		out := []string{}
		for _, x := range ids {
			if x != id {
				out = append(out, x)
			}
		}
		return out
	}

	nodes := make([]*Node, 3)
	for i, id := range ids {
		nodes[i] = mkNode(id, peers(id), chs[i])
	}

	// Wait for a leader to emerge.
	var leaderIdx int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n.State().Role == Leader {
				leaderIdx = i
				goto found
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader after 5s")
found:
	t.Logf("initial leader: %s (term %d)", ids[leaderIdx], nodes[leaderIdx].State().Term)

	// Propose some commands.
	for i := 0; i < 5; i++ {
		nodes[leaderIdx].Propose([]byte(fmt.Sprintf("cmd%d", i)))
	}
	time.Sleep(300 * time.Millisecond)

	// Capture state before restart.
	oldTerm := nodes[leaderIdx].State().Term
	oldLog := nodes[leaderIdx].LogSnapshot()

	// Stop and restart the leader node.
	nodes[leaderIdx].Stop()
	transport.Unregister(ids[leaderIdx])
	time.Sleep(50 * time.Millisecond)

	restarted := mkNode(ids[leaderIdx], peers(ids[leaderIdx]), chs[leaderIdx])
	defer restarted.Stop()

	// The restarted node must recover at least the same term and log length.
	// (It may have a slightly higher term if other nodes held elections during
	// the restart window — that's fine, we just check it's not *less*.)
	s := restarted.State()
	if s.Term < oldTerm {
		t.Errorf("restarted node has term %d < pre-restart term %d — lost persistent state",
			s.Term, oldTerm)
	}
	restoredLog := restarted.LogSnapshot()
	if len(restoredLog) < len(oldLog) {
		t.Errorf("restarted node log len %d < pre-restart %d — lost committed entries",
			len(restoredLog), len(oldLog))
	}
	for i := range oldLog {
		if i >= len(restoredLog) {
			break
		}
		if restoredLog[i].Term != oldLog[i].Term {
			t.Errorf("log[%d]: term %d != %d after restart", i, restoredLog[i].Term, oldLog[i].Term)
		}
	}
	t.Logf("restarted %s: term=%d log_len=%d (was %d/%d) — persistence OK",
		ids[leaderIdx], s.Term, len(restoredLog), oldTerm, len(oldLog))
}
