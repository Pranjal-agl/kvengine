package raftkv

import (
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kvengine/internal/raft"
)

type testCluster struct {
	t         *testing.T
	dir       string
	ids       []string
	transport *raft.MemTransport
	nodes     []*raft.Node
	kvs       []*KV
	opts      Options
}

func newTestCluster(t *testing.T, n int, opts Options) *testCluster {
	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}
	c := &testCluster{
		t:         t,
		dir:       t.TempDir(),
		transport: raft.NewMemTransport(),
		nodes:     make([]*raft.Node, n),
		kvs:       make([]*KV, n),
		opts:      opts,
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for i := range c.ids {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *testCluster) start(i int) {
	var peers []string
	for _, p := range c.ids {
		if p != c.ids[i] {
			peers = append(peers, p)
		}
	}
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNode(c.ids[i], peers, c.transport, ch,
		filepath.Join(c.dir, c.ids[i]+"-state.json"), filepath.Join(c.dir, c.ids[i]+"-snap.json"))
	if err != nil {
		c.t.Fatal(err)
	}
	kv, err := New(node, ch, c.opts)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[i], c.kvs[i] = node, kv
}

func (c *testCluster) stop(i int) {
	c.nodes[i].Stop()
	c.kvs[i].Close()
	c.transport.Unregister(c.ids[i])
}

func (c *testCluster) leader() int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if n.State().Role == raft.Leader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

func (c *testCluster) isolate(i int) {
	for j, id := range c.ids {
		if j != i {
			c.transport.Partition(id, c.ids[i])
			c.transport.Partition(c.ids[i], id)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A successful Put is visible on the leader immediately (it was applied
// before Put returned) and eventually on every follower.
func TestPutIsAppliedBeforeReturning(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	l := c.leader()
	for i := 0; i < 20; i++ {
		k, v := []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))
		if err := c.kvs[l].Put(k, v); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if got, ok := c.kvs[l].Get(k); !ok || string(got) != string(v) {
			t.Fatalf("leader does not see its own acknowledged write %s: (%q, %v)", k, got, ok)
		}
	}
	if err := c.kvs[l].Delete([]byte("k0")); err != nil {
		t.Fatal(err)
	}
	for i := range c.kvs {
		waitFor(t, fmt.Sprintf("node %d to converge", i), func() bool {
			_, has0 := c.kvs[i].Get([]byte("k0"))
			v, _ := c.kvs[i].Get([]byte("k19"))
			return !has0 && string(v) == "v19"
		})
	}
}

func TestFollowerRejectsWrites(t *testing.T) {
	c := newTestCluster(t, 3, Options{})
	l := c.leader()
	f := (l + 1) % 3
	waitFor(t, "follower to learn the leader", func() bool { return c.nodes[f].State().LeaderId != "" })
	err := c.kvs[f].Put([]byte("k"), []byte("v"))
	var nle *NotLeaderError
	if !errors.As(err, &nle) || nle.LeaderID != c.ids[l] {
		t.Fatalf("Put on follower: got %v, want NotLeaderError{%s}", err, c.ids[l])
	}
}

// The bug this package fixes: a leader cut off from its quorum must NOT
// acknowledge writes. The old CLI returned success as soon as the entry
// was in the leader's own log.
func TestIsolatedLeaderDoesNotAcknowledge(t *testing.T) {
	c := newTestCluster(t, 3, Options{WriteTimeout: 700 * time.Millisecond})
	l := c.leader()
	if err := c.kvs[l].Put([]byte("before"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	c.isolate(l)
	err := c.kvs[l].Put([]byte("during"), []byte("x"))
	if err == nil {
		t.Fatal("isolated leader acknowledged a write it could not replicate")
	}
	t.Logf("isolated leader correctly refused: %v", err)

	// The majority side elects a new leader and the unacknowledged write
	// never appears there.
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
	if err := c.kvs[nl].Put([]byte("after"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.kvs[nl].Get([]byte("during")); ok {
		t.Fatal("unacknowledged write from isolated leader became visible")
	}
	c.transport.HealAll()
	waitFor(t, "old leader to converge", func() bool {
		_, during := c.kvs[l].Get([]byte("during"))
		v, _ := c.kvs[l].Get([]byte("after"))
		return !during && string(v) == "2"
	})
}

// Binary keys and values must survive the Raft log and snapshots. The old
// CLI encoded commands as JSON strings, which mangles invalid UTF-8.
func TestBinarySafeThroughRaftAndSnapshots(t *testing.T) {
	c := newTestCluster(t, 3, Options{SnapshotEvery: 3})
	l := c.leader()
	key, val := []byte{0xff, 0x00, 'k'}, []byte{0xc3, 0x28, 0x00, 0xfe}
	if err := c.kvs[l].Put(key, val); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ { // force a few snapshots
		c.kvs[l].Put([]byte(fmt.Sprintf("pad%d", i)), []byte("x"))
	}
	if got, _ := c.kvs[l].Get(key); string(got) != string(val) {
		t.Fatalf("binary value corrupted through raft: got %x want %x", got, val)
	}
	// Restart the leader: state comes back from snapshot + log.
	c.stop(l)
	c.start(l)
	waitFor(t, "restarted node to restore binary value", func() bool {
		got, _ := c.kvs[l].Get(key)
		return string(got) == string(val)
	})
}

// After a full-cluster restart, state must come back without any new
// client write (needs the snapshot restore plus the leader's no-op to
// commit entries from the previous term).
func TestFullClusterRestartRecoversState(t *testing.T) {
	c := newTestCluster(t, 3, Options{SnapshotEvery: 5})
	l := c.leader()
	for i := 0; i < 23; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.start(i)
	}
	for i := range c.kvs {
		waitFor(t, fmt.Sprintf("node %d to recover all keys", i), func() bool {
			for k := 0; k < 23; k++ {
				v, _ := c.kvs[i].Get([]byte(fmt.Sprintf("k%d", k)))
				if string(v) != fmt.Sprintf("v%d", k) {
					return false
				}
			}
			return true
		})
	}
}

// A follower that misses enough writes to fall behind the leader's
// snapshot catches up through InstallSnapshot.
func TestLaggingFollowerCatchesUpWithSnapshot(t *testing.T) {
	c := newTestCluster(t, 3, Options{SnapshotEvery: 5})
	l := c.leader()
	lag := (l + 1) % 3
	c.isolate(lag)
	for i := 0; i < 30; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if c.nodes[l].SnapshotIndex() == 0 {
		t.Fatal("leader never snapshotted; test would not exercise InstallSnapshot")
	}
	c.transport.HealAll()
	waitFor(t, "lagging follower to catch up", func() bool { return c.kvs[lag].Len() == 30 })
}

// Growing a running cluster through the KV layer: a joining node catches
// up on existing data, then counts toward the majority.
func TestAddAndRemoveNode(t *testing.T) {
	c := newTestCluster(t, 3, Options{SnapshotEvery: 5})
	l := c.leader()
	for i := 0; i < 12; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Start a fourth node with no membership: it waits to be added.
	c.ids = append(c.ids, "n3")
	c.nodes = append(c.nodes, nil)
	c.kvs = append(c.kvs, nil)
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNodeWithMembers("n3", raft.Membership{}, c.transport, ch,
		filepath.Join(c.dir, "n3-state.json"), filepath.Join(c.dir, "n3-snap.json"))
	if err != nil {
		t.Fatal(err)
	}
	kv, err := New(node, ch, c.opts)
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[3], c.kvs[3] = node, kv

	if err := c.kvs[(l+1)%3].AddNode("n3", ""); err == nil {
		t.Fatal("AddNode on a follower should fail with NotLeaderError")
	}
	if err := c.kvs[l].AddNode("n3", ""); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// It catches up on data written before it joined (via snapshot).
	waitFor(t, "new node to catch up", func() bool {
		v, ok, err := c.kvs[3].GetErr([]byte("k11"))
		return err == nil && ok && string(v) == "v"
	})
	if info := c.kvs[l].MembersInfo(); !strings.Contains(info, "n3") || strings.Count(info, "\n") != 4 {
		t.Fatalf("MembersInfo = %q", info)
	}

	// Remove the original leader; the cluster keeps working without it.
	if err := c.kvs[l].RemoveNode(c.ids[l]); err != nil {
		t.Fatalf("RemoveNode(leader): %v", err)
	}
	var nl int
	waitFor(t, "new leader among remaining members", func() bool {
		for i, n := range c.nodes {
			if i != l && n.State().Role == raft.Leader {
				nl = i
				return true
			}
		}
		return false
	})
	if err := c.kvs[nl].Put([]byte("after"), []byte("ok")); err != nil {
		t.Fatalf("write after removing the old leader: %v", err)
	}
	waitFor(t, "new node sees post-removal write", func() bool {
		v, ok, err := c.kvs[3].GetErr([]byte("after"))
		return err == nil && ok && string(v) == "ok"
	})
}
