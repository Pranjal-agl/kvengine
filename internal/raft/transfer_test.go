package raft

import (
	"errors"
	"testing"
	"time"
)

// waitLeaderOther waits for a leader other than old and returns it with
// the time it took.
func waitLeaderOther(t *testing.T, c *mcluster, old string, start time.Time) (string, time.Duration) {
	t.Helper()
	for time.Since(start) < 5*time.Second {
		c.mu.Lock()
		for id, n := range c.nodes {
			if id != old && n.State().Role == Leader {
				c.mu.Unlock()
				return id, time.Since(start)
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no new leader")
	return "", 0
}

// A transfer makes the chosen follower leader in about a round trip,
// rather than after an election timeout, and loses nothing, six times
// over. (With followers ignoring transfer votes, transfers still complete,
// just ~25x slower, by a path through the old leader's next heartbeat;
// TestTransferVoteNotIgnoredWhileLeaderAlive checks the rule directly.)
func TestTransferLeadership(t *testing.T) {
	c := newMCluster(t, 3, "")
	want := []string{"a"}
	c.propose("a")
	for i := 0; i < 6; i++ {
		lid, l := c.leader()
		target := []string{"m0", "m1", "m2"}[i%3]
		if target == lid {
			target = []string{"m0", "m1", "m2"}[(i+1)%3]
		}
		start := time.Now()
		if err := l.TransferLeadership(target, time.Second); err != nil {
			t.Fatalf("transfer %d (%s -> %s): %v", i, lid, target, err)
		}
		took := time.Since(start)
		if s := c.node(target).State(); s.Role != Leader {
			t.Fatalf("%s is %v after the transfer", target, s.Role)
		}
		if took >= electionTimeoutMin {
			t.Fatalf("transfer %d took %v; an election timeout is %v", i, took, electionTimeoutMin)
		}
		t.Logf("transfer %s -> %s took %v", lid, target, took)
		cmd := string(rune('b' + i))
		c.propose(cmd)
		want = append(want, cmd)
	}
	for _, id := range []string{"m0", "m1", "m2"} {
		c.waitApplied(id, want)
	}
	// Transferring to the current leader is a no-op.
	lid, l := c.leader()
	if err := l.TransferLeadership(lid, time.Second); err != nil {
		t.Fatal(err)
	}
}

// The target is caught up first; meanwhile the leader refuses proposals
// and membership changes.
func TestTransferWaitsForTargetToCatchUp(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, l := c.leader()
	target := "m2"
	if lid == "m2" {
		target = "m1"
	}
	c.tr.Partition(lid, target)
	for i := 0; i < 20; i++ {
		c.propose("x")
	}
	done := make(chan error, 1)
	go func() { done <- l.TransferLeadership(target, 3*time.Second) }()
	time.Sleep(100 * time.Millisecond)
	if _, _, ok := l.Propose([]byte("during")); ok {
		t.Fatal("leader accepted a proposal during the transfer")
	}
	if _, _, err := l.RemoveServer(target); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("membership change during transfer: %v, want ErrTransferInProgress", err)
	}
	if err := l.TransferLeadership("", time.Second); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("second transfer: %v, want ErrTransferInProgress", err)
	}
	c.tr.HealAll()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.node(target).State().Role != Leader {
		t.Fatal("target is not leader")
	}
	c.propose("after")
}

// If the target can't take over in time, the transfer is abandoned and
// the old leader carries on.
func TestTransferTimesOut(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, l := c.leader()
	target := "m2"
	if lid == "m2" {
		target = "m1"
	}
	c.tr.Partition(lid, target)
	c.tr.Partition(target, lid)
	err := l.TransferLeadership(target, 150*time.Millisecond)
	if !errors.Is(err, ErrTransferTimeout) {
		t.Fatalf("transfer to an unreachable member: %v, want ErrTransferTimeout", err)
	}
	if l.State().Role != Leader {
		t.Fatal("old leader lost leadership")
	}
	idx, term, ok := l.Propose([]byte("after"))
	if !ok {
		t.Fatal("leader still refusing proposals after the transfer timed out")
	}
	if err := l.WaitCommitted(idx, term, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

// Picking the target automatically chooses an up-to-date member.
func TestTransferPicksUpToDateMember(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, l := c.leader()
	var behind string
	for _, id := range []string{"m0", "m1", "m2"} {
		if id != lid {
			behind = id
			break
		}
	}
	c.tr.Partition(lid, behind)
	for i := 0; i < 5; i++ {
		c.propose("x")
	}
	if err := l.TransferLeadership("", time.Second); err != nil {
		t.Fatal(err)
	}
	newID, _ := c.leader(lid)
	if newID == behind {
		t.Fatalf("transferred to %s, the member that was behind", behind)
	}
}

// A leader whose removal commits hands off at once, instead of leaving
// the cluster leaderless for an election timeout.
func TestRemovedLeaderHandsOff(t *testing.T) {
	for trial := 0; trial < 3; trial++ {
		c := newMCluster(t, 3, "")
		c.propose("a")
		oldID, old := c.leader()
		idx, term, err := old.RemoveServer(oldID)
		if err != nil {
			t.Fatal(err)
		}
		if err := old.WaitCommitted(idx, term, 3*time.Second); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		newID, gap := waitLeaderOther(t, c, oldID, start)
		if gap >= electionTimeoutMin {
			t.Fatalf("no leader for %v after the leader's removal; hand-off should take ~a round trip", gap)
		}
		t.Logf("trial %d: %s -> %s in %v", trial, oldID, newID, gap)
		c.propose("b", oldID)
	}
}

// The vote rule itself: a follower that hears from a live leader ignores
// an ordinary candidate (§4.2.3) but not one the leader asked to campaign.
func TestTransferVoteNotIgnoredWhileLeaderAlive(t *testing.T) {
	n, err := NewNodeWithMembers("f", Membership{"f": "", "L": "", "c": ""}, NewMemTransport(), make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	if r := n.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderId: "L"}); !r.Success {
		t.Fatalf("heartbeat from L rejected: %+v", r)
	}
	vote := RequestVoteArgs{Term: 2, CandidateId: "c"}
	if r := n.HandleRequestVote(vote); r.VoteGranted || r.Term != 1 {
		t.Fatalf("ordinary candidate while the leader is alive: %+v, want ignored at term 1", r)
	}
	vote.Transfer = true
	if r := n.HandleRequestVote(vote); !r.VoteGranted || r.Term != 2 {
		t.Fatalf("transfer candidate: %+v, want vote granted at term 2", r)
	}
	// Still one vote per term: another transfer candidate in term 2 loses.
	if r := n.HandleRequestVote(RequestVoteArgs{Term: 2, CandidateId: "L", Transfer: true}); r.VoteGranted {
		t.Fatal("granted a second vote in term 2")
	}
}
