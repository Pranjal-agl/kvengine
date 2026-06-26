package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestTCPTransportCluster builds a 3-node Raft cluster using real TCP
// connections over loopback — proving the TCPTransport wires up correctly,
// not just the in-memory MemTransport. Each node gets its own goroutine
// running TCPTransport.Serve(), then we verify election and basic
// replication work end-to-end over real sockets.
func TestTCPTransportCluster(t *testing.T) {
	// Pick 3 free loopback ports.
	ports := make([]int, 3)
	for i := range ports {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("find free port: %v", err)
		}
		ports[i] = ln.Addr().(*net.TCPAddr).Port
		ln.Close()
	}

	ids := []string{"tcp0", "tcp1", "tcp2"}
	addrs := map[string]string{
		"tcp0": fmt.Sprintf("127.0.0.1:%d", ports[0]),
		"tcp1": fmt.Sprintf("127.0.0.1:%d", ports[1]),
		"tcp2": fmt.Sprintf("127.0.0.1:%d", ports[2]),
	}
	dir := t.TempDir()

	transports := make([]*TCPTransport, 3)
	nodes := make([]*Node, 3)
	applyChs := make([]chan ApplyMsg, 3)

	for i, id := range ids {
		transports[i] = NewTCPTransport(addrs)
		applyChs[i] = make(chan ApplyMsg, 64)
		peers := []string{}
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		sp := filepath.Join(dir, id+".json")
		var err error
		nodes[i], err = NewNode(id, peers, transports[i], applyChs[i], sp)
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		go func(tr *TCPTransport, nodeID string) {
			if err := tr.Serve(nodeID); err != nil {
				// expected on cleanup
			}
		}(transports[i], id)
	}

	t.Cleanup(func() {
		for i := range nodes {
			nodes[i].Stop()
			transports[i].Close()
		}
	})

	// Wait for leader.
	var leaderIdx int
	found := false
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if n.State().Role == Leader {
				leaderIdx = i
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !found {
		t.Fatalf("no leader emerged over TCP transport after 6s")
	}
	t.Logf("TCP cluster leader: %s (term %d)", ids[leaderIdx], nodes[leaderIdx].State().Term)

	// Propose a few commands.
	for i := 0; i < 5; i++ {
		idx, term, ok := nodes[leaderIdx].Propose([]byte(fmt.Sprintf("tcp-cmd-%d", i)))
		if !ok {
			t.Fatalf("Propose %d rejected", i)
		}
		t.Logf("proposed cmd %d at index=%d term=%d", i, idx, term)
	}

	// Wait for all nodes to apply all 5 commands.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for _, ch := range applyChs {
			count := len(ch)
			if count >= 5 {
				done++
			}
		}
		if done == 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Drain and verify.
	applied := make([][]ApplyMsg, 3)
	for i, ch := range applyChs {
		for {
			select {
			case msg := <-ch:
				applied[i] = append(applied[i], msg)
			default:
				goto drained
			}
		}
	drained:
	}

	for i := range nodes {
		if len(applied[i]) < 5 {
			t.Errorf("node %s: only applied %d/5 commands", ids[i], len(applied[i]))
		}
	}
	// All nodes that applied commands must agree on order.
	ref := applied[leaderIdx]
	for i, a := range applied {
		for j := 0; j < len(ref) && j < len(a); j++ {
			if string(ref[j].Command) != string(a[j].Command) {
				t.Errorf("divergence at %d: leader has %q, node %s has %q",
					j, ref[j].Command, ids[i], a[j].Command)
			}
		}
	}
	t.Logf("TCP cluster: all nodes agreed on %d commands", len(ref))
}
