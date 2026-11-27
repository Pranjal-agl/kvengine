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
	c := &tcpCluster{}
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		tr := raft.NewTCPTransport(addrs)
		ch := make(chan raft.ApplyMsg, 256)
		node, err := raft.NewNode(id, peers, tr, ch,
			filepath.Join(dir, id+"-state.json"), filepath.Join(dir, id+"-snap.json"))
		if err != nil {
			tb.Fatal(err)
		}
		kv, err := New(node, ch, opts)
		if err != nil {
			tb.Fatal(err)
		}
		go tr.Serve(id)
		c.nodes = append(c.nodes, node)
		c.kvs = append(c.kvs, kv)
		c.transports = append(c.transports, tr)
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
