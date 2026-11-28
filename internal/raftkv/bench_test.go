package raftkv

import (
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kvengine/internal/raft"
)

// tcpCluster is a 3-node cluster in one process, but talking over real
// loopback TCP (TCPTransport) and persisting Raft state to disk, so the
// numbers include serialization, syscalls and fsync like a real deployment.
type tcpCluster struct {
	nodes      []*raft.Node
	kvs        []*KV
	transports []*raft.TCPTransport

	ids   []string
	addrs map[string]string
	dir   string
	opts  Options
}

func newTCPCluster(tb testing.TB, opts Options) *tcpCluster {
	tb.Helper()
	opts.Logger = log.New(io.Discard, "", 0)
	ids := []string{"b0", "b1", "b2"}
	addrs := map[string]string{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		addrs[id] = ln.Addr().String()
		ln.Close()
	}
	dir := tb.TempDir()
	c := &tcpCluster{ids: ids, addrs: addrs, dir: dir, opts: opts}
	for range ids {
		c.nodes = append(c.nodes, nil)
		c.kvs = append(c.kvs, nil)
		c.transports = append(c.transports, nil)
	}
	for i := range ids {
		c.restart(tb, i)
	}
	tb.Cleanup(func() {
		for i := range c.nodes {
			c.nodes[i].Stop()
			c.transports[i].Close()
			c.kvs[i].Close()
		}
	})
	return c
}

// restart (re)starts node i on its files and address.
func (c *tcpCluster) restart(tb testing.TB, i int) {
	tb.Helper()
	id := c.ids[i]
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	addrs := map[string]string{}
	for k, v := range c.addrs {
		addrs[k] = v
	}
	tr := raft.NewTCPTransport(addrs)
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.NewNode(id, peers, tr, ch,
		filepath.Join(c.dir, id+"-state.json"), filepath.Join(c.dir, id+"-snap.json"))
	if err != nil {
		tb.Fatal(err)
	}
	kv, err := New(node, ch, c.opts)
	if err != nil {
		tb.Fatal(err)
	}
	go tr.Serve(id)
	c.nodes[i], c.kvs[i], c.transports[i] = node, kv, tr
}

func (c *tcpCluster) leader(tb testing.TB) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range c.nodes {
			if n.State().Role == raft.Leader {
				if err := c.kvs[i].Put([]byte("warmup"), []byte("x")); err == nil {
					return i
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatal("no leader")
	return -1
}

func BenchmarkClusterPut(b *testing.B) {
	c := newTCPCluster(b, Options{SnapshotEvery: 1000})
	l := c.leader(b)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i%1000)), val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterPutConcurrent16(b *testing.B) {
	c := newTCPCluster(b, Options{SnapshotEvery: 1000})
	l := c.leader(b)
	runConcurrent(b, 16, func(i int64) error {
		return c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i%1000)), []byte("v"))
	})
}

func BenchmarkClusterGetLeader(b *testing.B) {
	c := newTCPCluster(b, Options{})
	l := c.leader(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.kvs[l].GetErr([]byte("warmup")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterGetFollower(b *testing.B) {
	c := newTCPCluster(b, Options{})
	l := c.leader(b)
	f := (l + 1) % 3
	for c.nodes[f].State().LeaderId == "" {
		time.Sleep(10 * time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.kvs[f].GetErr([]byte("warmup")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterGetLeaderConcurrent16(b *testing.B) {
	c := newTCPCluster(b, Options{})
	l := c.leader(b)
	runConcurrent(b, 16, func(int64) error {
		_, _, err := c.kvs[l].GetErr([]byte("warmup"))
		return err
	})
}

// runConcurrent runs b.N calls of fn spread over `workers` goroutines.
func runConcurrent(b *testing.B, workers int, fn func(i int64) error) {
	var next int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&next, 1)
				if i > int64(b.N) {
					return
				}
				if err := fn(i); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Same as BenchmarkClusterPut with a short log: isolates how much of the
// write cost comes from persisting a long log.
func BenchmarkClusterPutShortLog(b *testing.B) {
	c := newTCPCluster(b, Options{SnapshotEvery: 10})
	l := c.leader(b)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("k%d", i%1000)), val); err != nil {
			b.Fatal(err)
		}
	}
}

// A follower that falls far behind must be able to catch up even when the
// entries it's missing add up to more than one RPC may carry (32 MiB).
func TestFollowerCatchesUpAfterLargeBacklog(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~40 MB")
	}
	c := newTCPCluster(t, Options{SnapshotEvery: 0}) // no snapshots: catch-up must use the log
	l := c.leader(t)
	f := (l + 1) % 3
	c.transports[f].Close() // follower drops off the network
	c.nodes[f].Stop()

	val := make([]byte, 1<<20)
	for i := 0; i < 40; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("big%d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	// Bring the follower back on the same files and address.
	c.restart(t, f)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if v, ok := c.kvs[f].Get([]byte("big39")); ok && len(v) == len(val) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never caught up on a ~40 MB backlog (state %+v)", c.nodes[f].State())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Same, but the leader compacts its log meanwhile, so the follower must
// receive a snapshot bigger than the old 32 MiB message cap.
func TestFollowerCatchesUpWithLargeSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~40 MB")
	}
	c := newTCPCluster(t, Options{SnapshotEvery: 10})
	l := c.leader(t)
	f := (l + 1) % 3
	c.transports[f].Close()
	c.nodes[f].Stop()
	val := make([]byte, 1<<20)
	for i := 0; i < 40; i++ {
		if err := c.kvs[l].Put([]byte(fmt.Sprintf("big%d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	if c.nodes[l].SnapshotIndex() == 0 {
		t.Fatal("leader never snapshotted")
	}
	c.restart(t, f)
	deadline := time.Now().Add(30 * time.Second)
	for c.kvs[f].Len() < 41 { // 40 values + warmup key
		if time.Now().After(deadline) {
			t.Fatalf("follower has %d keys; never installed the large snapshot", c.kvs[f].Len())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
