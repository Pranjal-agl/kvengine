package raft

import (
	"net"
	"testing"
	"time"
)

// freeAddr returns a loopback address that was free a moment ago.
func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// startServer runs a transport serving a standalone node with id "srv".
func startServer(t *testing.T, addrs map[string]string) (*TCPTransport, *Node) {
	tr := NewTCPTransport(addrs)
	n, err := NewNode("srv", []string{"cli"}, tr, make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	go tr.Serve("srv")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if c, err := net.Dial("tcp", addrs["srv"]); err == nil {
			c.Close()
			tr.accepted.Add(-1) // don't count the readiness probe
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return tr, n
}

func TestTCPTransportReusesConnections(t *testing.T) {
	addrs := map[string]string{"srv": freeAddr(t), "cli": freeAddr(t)}
	srv, n := startServer(t, addrs)
	defer n.Stop()
	defer srv.Close()
	cli := NewTCPTransport(addrs)
	defer cli.Close()

	for i := 0; i < 100; i++ {
		if _, err := cli.RequestVote("srv", RequestVoteArgs{Term: 0, CandidateId: "cli"}); err != nil {
			t.Fatalf("rpc %d: %v", i, err)
		}
	}
	if got := srv.accepted.Load(); got != 1 {
		t.Fatalf("100 sequential RPCs opened %d connections, want 1", got)
	}
}

// After the peer restarts, the pooled connection is dead. The next call
// must succeed transparently on a fresh connection.
func TestTCPTransportRetriesStaleConnection(t *testing.T) {
	addrs := map[string]string{"srv": freeAddr(t), "cli": freeAddr(t)}
	srv, n := startServer(t, addrs)
	cli := NewTCPTransport(addrs)
	defer cli.Close()
	if _, err := cli.RequestVote("srv", RequestVoteArgs{CandidateId: "cli"}); err != nil {
		t.Fatal(err)
	}
	n.Stop()
	srv.Close() // kills the pooled connection's server end

	srv2, n2 := startServer(t, addrs)
	defer n2.Stop()
	defer srv2.Close()
	if _, err := cli.RequestVote("srv", RequestVoteArgs{CandidateId: "cli"}); err != nil {
		t.Fatalf("call after peer restart failed: %v", err)
	}
}

// A closed transport must stop answering, including on connections that
// were already open — otherwise a "stopped" node keeps serving RPCs.
func TestTCPTransportCloseStopsExistingConnections(t *testing.T) {
	addrs := map[string]string{"srv": freeAddr(t), "cli": freeAddr(t)}
	srv, n := startServer(t, addrs)
	defer n.Stop()
	cli := NewTCPTransport(addrs)
	defer cli.Close()
	if _, err := cli.RequestVote("srv", RequestVoteArgs{CandidateId: "cli"}); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	if _, err := cli.RequestVote("srv", RequestVoteArgs{CandidateId: "cli"}); err == nil {
		t.Fatal("closed transport still answered an RPC")
	}
	if _, err := srv.RequestVote("cli", RequestVoteArgs{CandidateId: "srv"}); err == nil {
		t.Fatal("closed transport still sent an RPC")
	}
}
