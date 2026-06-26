package raft

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	transport := NewMemTransport()
	ids := []string{"s0", "s1", "s2"}
	chs := make([]chan ApplyMsg, 3)
	nodes := make([]*Node, 3)

	for i, id := range ids {
		chs[i] = make(chan ApplyMsg, 128)
		peers := []string{}
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		sp := filepath.Join(dir, id+"-state.json")
		snap := filepath.Join(dir, id+"-snap.json")
		var err error
		nodes[i], err = NewNode(id, peers, transport, chs[i], sp, snap)
		if err != nil {
			t.Fatalf("NewNode: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	// Elect a leader.
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
	t.Fatalf("no leader")
found:

	// Propose 10 commands and wait for all to commit.
	const N = 10
	for i := 0; i < N; i++ {
		if _, _, ok := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("c%d", i))); !ok {
			t.Fatalf("Propose %d failed", i)
		}
	}
	// Drain apply channels.
	time.Sleep(500 * time.Millisecond)
	for _, ch := range chs {
		for {
			select {
			case <-ch:
			default:
				goto done
			}
		}
	done:
	}

	// Confirm all nodes have commitIndex >= N.
	for i, n := range nodes {
		n.mu.Lock()
		ci := n.commitIndex
		n.mu.Unlock()
		if ci < N {
			t.Fatalf("node %s commitIndex=%d, want >=%d", ids[i], ci, N)
		}
	}

	// Take a snapshot at index 5 on the leader.
	stateData, _ := json.Marshal(map[string]string{"snap": "at-5"})
	if err := nodes[leaderIdx].TakeSnapshot(5, stateData); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// After snapshot at 5, the log should be shorter: entries 1-5 gone,
	// only sentinel + entries 6-N remain.
	nodes[leaderIdx].mu.Lock()
	logLen := len(nodes[leaderIdx].log)
	snapIdx := nodes[leaderIdx].snapshotIndex
	nodes[leaderIdx].mu.Unlock()

	// log[0] is sentinel, log[1..] are entries 6..N → length = 1 + (N-5)
	expectedLen := 1 + (N - 5)
	if logLen != expectedLen {
		t.Errorf("after TakeSnapshot(5): log len=%d, want %d (sentinel + entries 6..%d)",
			logLen, expectedLen, N)
	}
	if snapIdx != 5 {
		t.Errorf("snapshotIndex=%d, want 5", snapIdx)
	}
	t.Logf("snapshot OK: log truncated to len=%d (was %d)", logLen, N+1)

	// Verify the snapshot file is readable and has the right content.
	snap, err := nodes[leaderIdx].LatestSnapshot()
	if err != nil || snap == nil {
		t.Fatalf("LatestSnapshot: %v", err)
	}
	if snap.LastIncludedIndex != 5 {
		t.Errorf("snapshot.LastIncludedIndex=%d, want 5", snap.LastIncludedIndex)
	}
	t.Logf("snapshot file: lastIndex=%d lastTerm=%d data=%s",
		snap.LastIncludedIndex, snap.LastIncludedTerm, snap.Data)
}
