package raft

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// mcluster is a test cluster whose membership can change. Every node's
// applied commands are recorded.
type mcluster struct {
	t   *testing.T
	tr  *MemTransport
	dir string // "" = in-memory nodes

	mu      sync.Mutex
	nodes   map[string]*Node
	applied map[string][]string
}

func newMCluster(t *testing.T, n int, dir string) *mcluster {
	c := &mcluster{t: t, tr: NewMemTransport(), dir: dir,
		nodes: map[string]*Node{}, applied: map[string][]string{}}
	members := Membership{}
	for i := 0; i < n; i++ {
		members[fmt.Sprintf("m%d", i)] = ""
	}
	for id := range members {
		c.start(id, members)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

// start launches node id with the given bootstrap membership (empty =
// joining node).
func (c *mcluster) start(id string, members Membership) *Node {
	ch := make(chan ApplyMsg, 1024)
	var sp, snap string
	if c.dir != "" {
		sp, snap = filepath.Join(c.dir, id+".state"), filepath.Join(c.dir, id+".snap")
	}
	n, err := NewNodeWithMembers(id, members, c.tr, ch, sp, snap)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.applied[id] = nil // a restarted node rebuilds its state machine from scratch
	c.mu.Unlock()
	go func() {
		for msg := range ch {
			c.mu.Lock()
			if msg.SnapshotValid {
				c.applied[id] = append([]string{}, fmt.Sprintf("<snapshot@%d>", msg.SnapshotIndex))
			} else {
				c.applied[id] = append(c.applied[id], string(msg.Command))
			}
			c.mu.Unlock()
		}
	}()
	return n
}

func (c *mcluster) node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *mcluster) stop(id string) {
	c.node(id).Stop()
	c.tr.Unregister(id)
}

func (c *mcluster) leader(exclude ...string) (string, *Node) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for id, n := range c.nodes {
			skip := false
			for _, x := range exclude {
				skip = skip || x == id
			}
			if !skip && n.State().Role == Leader {
				c.mu.Unlock()
				return id, n
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return "", nil
}

// propose submits cmd via the current leader and waits for it to commit.
func (c *mcluster) propose(cmd string, exclude ...string) {
	c.t.Helper()
	for attempt := 0; attempt < 20; attempt++ {
		_, l := c.leader(exclude...)
		idx, term, ok := l.Propose([]byte(cmd))
		if ok && l.WaitCommitted(idx, term, 3*time.Second) == nil {
			return
		}
	}
	c.t.Fatalf("could not commit %q", cmd)
}

// change runs a membership change via the leader, retrying while a new
// leader settles in, and waits for it to commit.
func (c *mcluster) change(do func(*Node) (uint64, uint64, error)) {
	c.t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		_, l := c.leader()
		idx, term, err := do(l)
		if err == nil {
			if err = l.WaitCommitted(idx, term, 3*time.Second); err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("membership change did not commit")
}

func (c *mcluster) waitApplied(id string, want []string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		got := append([]string{}, c.applied[id]...)
		c.mu.Unlock()
		// A node that caught up via snapshot starts from a marker.
		if len(got) > 0 && len(got[0]) > 0 && got[0][0] == '<' {
			got = got[1:]
			if len(got) <= len(want) && fmt.Sprint(got) == fmt.Sprint(want[len(want)-len(got):]) && len(got) > 0 {
				return
			}
		}
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s applied %v, want %v", id, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func members(n *Node) string {
	ids := n.Members().IDs()
	sort.Strings(ids)
	return fmt.Sprint(ids)
}

func TestAddServer(t *testing.T) {
	c := newMCluster(t, 3, "")
	want := []string{"a", "b"}
	for _, cmd := range want {
		c.propose(cmd)
	}
	joiner := c.start("m3", Membership{}) // empty: waits to be added
	time.Sleep(500 * time.Millisecond)
	if s := joiner.State(); s.Term != 0 || s.Role != Follower {
		t.Fatalf("joining node campaigned before being added: %+v", s)
	}

	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m3", "") })
	c.propose("c")
	want = append(want, "c")
	c.waitApplied("m3", want)
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		deadline := time.Now().Add(3 * time.Second)
		for members(c.node(id)) != "[m0 m1 m2 m3]" && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if got := members(c.node(id)); got != "[m0 m1 m2 m3]" {
			t.Fatalf("%s members = %s", id, got)
		}
	}

	// With 4 members a majority is 3: losing one node is fine...
	lid, _ := c.leader()
	var victim string
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		if id != lid {
			victim = id
			break
		}
	}
	c.stop(victim)
	c.propose("d", victim)
	// ...losing two is not.
	var victim2 string
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		if id != lid && id != victim {
			victim2 = id
			break
		}
	}
	c.stop(victim2)
	_, l := c.leader(victim, victim2)
	idx, term, ok := l.Propose([]byte("e"))
	if ok && l.WaitCommitted(idx, term, 700*time.Millisecond) == nil {
		t.Fatal("committed with only 2 of 4 members alive")
	}
}

func TestRemoveFollower(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, _ := c.leader()
	var gone string
	for _, id := range []string{"m0", "m1", "m2"} {
		if id != lid {
			gone = id
			break
		}
	}
	c.change(func(l *Node) (uint64, uint64, error) { return l.RemoveServer(gone) })

	// Two members left: both are needed, and the removed node (still
	// running, now cut off from heartbeats) must not disrupt them.
	termBefore := c.node(lid).State().Term
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprintf("x%d", i), gone)
	}
	time.Sleep(time.Second) // several of the removed node's election timeouts
	c.propose("y", gone)
	if got := c.node(lid).State(); got.Role != Leader || got.Term != termBefore {
		t.Fatalf("removed node disrupted the cluster: leader state now %+v (term was %d)", got, termBefore)
	}
	if s := c.node(gone).State(); s.Role == Leader {
		t.Fatal("removed node became leader")
	}
}

func TestRemoveLeader(t *testing.T) {
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
	// The old leader steps down once its removal commits; the other two
	// elect a leader between them and keep going.
	deadline := time.Now().Add(3 * time.Second)
	for old.State().Role == Leader && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if old.State().Role == Leader {
		t.Fatal("removed leader did not step down")
	}
	newID, _ := c.leader(oldID)
	c.propose("b", oldID)
	if got := members(c.node(newID)); got == "" || len(c.node(newID).Members()) != 2 {
		t.Fatalf("new leader's members = %s", got)
	}
	time.Sleep(600 * time.Millisecond)
	if s := old.State(); s.Role != Follower {
		t.Fatalf("removed leader is %v again", s.Role)
	}
}

func TestOneMembershipChangeAtATime(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	lid, l := c.leader()
	// Cut the leader off so its change can't commit.
	for _, id := range []string{"m0", "m1", "m2"} {
		if id != lid {
			c.tr.Partition(lid, id)
		}
	}
	if _, _, err := l.AddServer("m9", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.AddServer("m10", ""); !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("second concurrent change: got %v, want ErrConfigChangeInProgress", err)
	}
}

// A config entry takes effect on append; if a new leader truncates it, the
// follower must fall back to the previous configuration.
func TestUncommittedConfigRevertsOnTruncation(t *testing.T) {
	n, err := NewNodeWithMembers("f", Membership{"f": "", "L": ""}, NewMemTransport(), make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	cfg := []byte(`{"f":"","L":"","X":""}`)
	n.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderId: "L",
		Entries: []LogEntry{{Term: 3, Kind: EntryConfig, Command: cfg}}})
	if got := members(n); got != "[L X f]" {
		t.Fatalf("after appending config entry: %s", got)
	}
	n.HandleAppendEntries(AppendEntriesArgs{Term: 4, LeaderId: "L",
		Entries: []LogEntry{{Term: 4, Command: []byte("cmd")}}})
	if got := members(n); got != "[L f]" {
		t.Fatalf("after truncating it: %s, want [L f]", got)
	}
}

// Membership must survive restarts and log compaction, and reach a new
// node through InstallSnapshot when the config entry has been compacted.
func TestMembershipSurvivesSnapshotsAndRestart(t *testing.T) {
	c := newMCluster(t, 3, t.TempDir())
	c.propose("a")
	c.start("m3", Membership{})
	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m3", "") })
	c.propose("b")
	// Compact every node's log past the config entry.
	for _, id := range []string{"m0", "m1", "m2", "m3"} {
		n := c.node(id)
		deadline := time.Now().Add(3 * time.Second)
		for n.State().LastApplied < n.State().CommitIndex || n.State().CommitIndex == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%s never applied", id)
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := n.TakeSnapshot(n.State().LastApplied, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	// Restart a node: its membership now comes only from the snapshot.
	c.stop("m1")
	c.start("m1", Membership{"m0": "", "m1": "", "m2": ""}) // stale bootstrap flags must not win
	if got := members(c.node("m1")); got != "[m0 m1 m2 m3]" {
		t.Fatalf("restarted node members = %s", got)
	}
	// A fifth node joins after the config entries were compacted: it can
	// only learn the membership from the snapshot it is sent.
	c.start("m4", Membership{})
	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m4", "") })
	c.propose("c")
	deadline := time.Now().Add(5 * time.Second)
	for members(c.node("m4")) != "[m0 m1 m2 m3 m4]" {
		if time.Now().After(deadline) {
			t.Fatalf("new node members = %s", members(c.node("m4")))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// §4.2.3: while a node has recently heard from a leader, a RequestVote
// with a higher term is ignored outright — the term is not adopted.
func TestRequestVoteIgnoredWhileLeaderAlive(t *testing.T) {
	n, err := NewNodeWithMembers("f", Membership{"f": "", "L": "", "R": ""}, NewMemTransport(), make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	n.HandleAppendEntries(AppendEntriesArgs{Term: 5, LeaderId: "L"})
	r := n.HandleRequestVote(RequestVoteArgs{Term: 9, CandidateId: "R", LastLogIndex: 100, LastLogTerm: 9})
	if r.VoteGranted || n.State().Term != 5 {
		t.Fatalf("disruptive vote accepted: granted=%v term=%d", r.VoteGranted, n.State().Term)
	}
	time.Sleep(electionTimeoutMin + 50*time.Millisecond)
	r = n.HandleRequestVote(RequestVoteArgs{Term: 9, CandidateId: "R", LastLogIndex: 100, LastLogTerm: 9})
	if !r.VoteGranted {
		t.Fatal("vote refused after the leader went quiet")
	}
}

// An even-sized cluster split exactly in half has no majority on either
// side: neither half may elect a new leader or commit. (Random chaos
// rarely produces an exact 2/2 split, so this case gets its own test.)
func TestEvenSplitHasNoQuorum(t *testing.T) {
	c := newMCluster(t, 3, "")
	c.propose("a")
	c.start("m3", Membership{})
	c.change(func(l *Node) (uint64, uint64, error) { return l.AddServer("m3", "") })
	c.propose("b")

	lid, l := c.leader()
	ids := []string{"m0", "m1", "m2", "m3"}
	half := []string{lid}
	var other []string
	for _, id := range ids {
		if id == lid {
			continue
		}
		if len(half) < 2 {
			half = append(half, id)
		} else {
			other = append(other, id)
		}
	}
	for _, a := range half {
		for _, b := range other {
			c.tr.Partition(a, b)
			c.tr.Partition(b, a)
		}
	}
	idx, term, ok := l.Propose([]byte("split"))
	if ok && l.WaitCommitted(idx, term, 400*time.Millisecond) == nil {
		t.Fatal("leader's half (2 of 4) committed an entry")
	}
	// Give the other half several election timeouts.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, id := range other {
			if c.node(id).State().Role == Leader {
				t.Fatalf("%s became leader with only 2 of 4 votes", id)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if l.WaitCommitted(idx, term, 10*time.Millisecond) == nil {
		t.Fatal("2 of 4 committed an entry")
	}
}
