package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// snapRecorder records InstallSnapshot calls and can fail chosen ones.
type snapRecorder struct {
	*MemTransport
	mu    sync.Mutex
	calls []InstallSnapshotArgs // Data omitted, only len kept in Offset/More checks
	sizes []int
	// fail returns an error to inject for a call, or nil to deliver it.
	fail func(args InstallSnapshotArgs, nth int) error
}

func (r *snapRecorder) InstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	r.mu.Lock()
	nth := len(r.calls)
	meta := args
	meta.Data = nil
	r.calls = append(r.calls, meta)
	r.sizes = append(r.sizes, len(args.Data))
	f := r.fail
	r.mu.Unlock()
	if f != nil {
		if err := f(args, nth); err != nil {
			return InstallSnapshotReply{}, err
		}
	}
	return r.MemTransport.InstallSnapshot(peer, args)
}

func (r *snapRecorder) log() ([]InstallSnapshotArgs, []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]InstallSnapshotArgs(nil), r.calls...), append([]int(nil), r.sizes...)
}

// bigSnapshot is valid JSON (a string) of about n bytes.
func bigSnapshot(n int) []byte {
	var b bytes.Buffer
	b.WriteByte('"')
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, "%08d", i)
	}
	b.WriteByte('"')
	return b.Bytes()
}

// snapshotToLagger builds a 3-node cluster on tr, cuts one follower off,
// makes the leader snapshot data, heals, and returns the follower's
// installed snapshot bytes.
func snapshotToLagger(t *testing.T, tr *snapRecorder, data []byte) []byte {
	t.Helper()
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	got := make(chan []byte, 4)
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		ch := make(chan ApplyMsg, 1024)
		n, err := NewNode(id, peers, tr, ch, "")
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
		id := id
		go func() {
			for m := range ch {
				if m.SnapshotValid && id == "c" {
					got <- m.Snapshot
				}
			}
		}()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for id, n := range nodes {
			if id != "c" && n.State().Role == Leader {
				leader = n
			}
		}
		if leader == nil {
			// Let c lose if it won; we want it to be the lagging follower.
			if nodes["c"].State().Role == Leader {
				tr.Partition("c", "a")
				tr.Partition("c", "b")
				time.Sleep(500 * time.Millisecond)
				tr.HealAll()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if leader == nil {
		t.Fatal("no leader among a, b")
	}
	for _, p := range []string{"a", "b"} {
		tr.Partition(p, "c")
		tr.Partition("c", p)
	}
	var last uint64
	for i := 0; i < 5; i++ {
		idx, _, ok := leader.Propose([]byte(fmt.Sprintf("cmd%d", i)))
		if !ok {
			t.Fatal("propose failed")
		}
		last = idx
	}
	deadline = time.Now().Add(3 * time.Second)
	for leader.State().LastApplied < last {
		if time.Now().After(deadline) {
			t.Fatal("leader did not apply")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := leader.TakeSnapshot(last, data); err != nil {
		t.Fatal(err)
	}
	tr.HealAll()
	select {
	case b := <-got:
		return b
	case <-time.After(10 * time.Second):
		t.Fatal("lagging follower never installed the snapshot")
		return nil
	}
}

// A snapshot larger than one chunk goes over in contiguous chunks of at
// most snapshotChunkSize, and arrives intact.
func TestChunkedSnapshotTransfer(t *testing.T) {
	data := bigSnapshot(3*snapshotChunkSize + 12345)
	tr := &snapRecorder{MemTransport: NewMemTransport()}
	if got := snapshotToLagger(t, tr, data); !bytes.Equal(got, data) {
		t.Fatalf("installed %d bytes, want the %d-byte snapshot", len(got), len(data))
	}
	calls, sizes := tr.log()
	var off uint64
	for i, c := range calls {
		if sizes[i] > snapshotChunkSize {
			t.Fatalf("call %d carried %d bytes, over the %d chunk size", i, sizes[i], snapshotChunkSize)
		}
		if c.Offset != off {
			t.Fatalf("call %d at offset %d, want %d (calls %+v)", i, c.Offset, off, calls)
		}
		off += uint64(sizes[i])
		if !c.More {
			break
		}
	}
	if off != uint64(len(data)) || len(calls) < 4 {
		t.Fatalf("%d calls covered %d of %d bytes", len(calls), off, len(data))
	}
}

// A failed chunk is resent from where the transfer stopped, not from 0.
func TestChunkedSnapshotResumesAfterFailure(t *testing.T) {
	data := bigSnapshot(4 * snapshotChunkSize)
	tr := &snapRecorder{MemTransport: NewMemTransport()}
	var failed sync.Once
	tr.fail = func(args InstallSnapshotArgs, nth int) error {
		var err error
		if args.Offset == 2*snapshotChunkSize {
			failed.Do(func() { err = errors.New("injected: connection reset") })
		}
		return err
	}
	if got := snapshotToLagger(t, tr, data); !bytes.Equal(got, data) {
		t.Fatal("snapshot corrupted across the failure")
	}
	calls, _ := tr.log()
	var offsets []uint64
	for _, c := range calls {
		offsets = append(offsets, c.Offset/snapshotChunkSize)
	}
	// Expect 0 1 2(fail) 2 3 — never back to 0 after the failure.
	sawFail := false
	for i, c := range calls {
		if c.Offset == 2*snapshotChunkSize && !sawFail {
			sawFail = true
			continue
		}
		if sawFail && c.Offset == 0 {
			t.Fatalf("restarted from 0 after the failure (call %d; chunks %v)", i, offsets)
		}
	}
	if !sawFail {
		t.Fatalf("failure never injected; chunks %v", offsets)
	}
	t.Logf("chunks sent: %v", offsets)
}

// A peer that doesn't understand chunks (an older version) gets the
// snapshot whole instead.
func TestChunkedSnapshotFallsBackToWholeForOldPeers(t *testing.T) {
	data := bigSnapshot(2*snapshotChunkSize + 7)
	tr := &snapRecorder{MemTransport: NewMemTransport()}
	tr.fail = func(args InstallSnapshotArgs, nth int) error {
		if args.Offset > 0 || args.More {
			return ErrSnapshotChunksUnsupported
		}
		return nil
	}
	if got := snapshotToLagger(t, tr, data); !bytes.Equal(got, data) {
		t.Fatal("whole-snapshot fallback delivered the wrong bytes")
	}
	calls, sizes := tr.log()
	for i, c := range calls {
		if c.Offset == 0 && !c.More && sizes[i] == len(data) {
			return
		}
	}
	t.Fatalf("no whole-snapshot call in %d calls", len(calls))
}

// The follower's reassembly rules, call by call.
func TestFollowerReassemblesChunks(t *testing.T) {
	n, err := NewNodeWithMembers("f", Membership{"f": "", "L": ""}, NewMemTransport(), make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	data := []byte(`"` + strings.Repeat("x", 30) + `"`) // 32 bytes of JSON
	chunk := func(idx, off uint64, end int, more bool) InstallSnapshotReply {
		return n.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderId: "L",
			LastIncludedIndex: idx, LastIncludedTerm: 1, Config: Membership{"f": "", "L": ""},
			Offset: off, Data: data[off:end], More: more})
	}
	check := func(r InstallSnapshotReply, next uint64, done bool, what string) {
		t.Helper()
		if r.Next != next || r.Done != done {
			t.Fatalf("%s: reply Next=%d Done=%v, want Next=%d Done=%v", what, r.Next, r.Done, next, done)
		}
	}
	check(chunk(10, 8, 16, true), 0, false, "first chunk not at offset 0")
	check(chunk(10, 0, 8, true), 8, false, "chunk 0")
	check(chunk(10, 16, 24, true), 8, false, "gap: ask for offset 8")
	check(chunk(10, 8, 16, true), 16, false, "chunk 1")
	check(chunk(10, 8, 16, true), 16, false, "resend of chunk 1")
	check(chunk(11, 16, 24, true), 0, false, "mid-transfer chunk of a different snapshot")
	check(chunk(10, 16, 24, true), 24, false, "a stray chunk doesn't discard the transfer in progress")
	check(chunk(10, 0, 8, true), 8, false, "restart")
	check(chunk(10, 8, 24, true), 24, false, "bigger chunk")
	check(chunk(10, 24, 32, false), 0, true, "last chunk installs")
	snap, _ := n.LatestSnapshot()
	if snap == nil || snap.LastIncludedIndex != 10 || !bytes.Equal(snap.Data, data) {
		t.Fatalf("installed snapshot = %+v", snap)
	}
	check(chunk(10, 0, 8, true), 0, true, "already installed")

	// Reassembled bytes that aren't JSON are refused.
	bad := []byte(`"unterminated`)
	r := n.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderId: "L", LastIncludedIndex: 20,
		LastIncludedTerm: 1, Data: bad[:5], More: true})
	check(r, 5, false, "bad chunk 0")
	r = n.HandleInstallSnapshot(InstallSnapshotArgs{Term: 1, LeaderId: "L", LastIncludedIndex: 20,
		LastIncludedTerm: 1, Offset: 5, Data: bad[5:]})
	check(r, 0, false, "invalid JSON is not installed")
	if snap, _ := n.LatestSnapshot(); snap.LastIncludedIndex != 10 {
		t.Fatalf("snapshot moved to %d on invalid data", snap.LastIncludedIndex)
	}
}

// Over TCP, a peer that predates chunks closes the connection on the new
// message type; the transport reports ErrSnapshotChunksUnsupported, while
// a whole snapshot still works.
func TestTCPTransportChunksAgainstOldPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				r := bufio.NewReader(conn)
				for {
					env, err := readEnvelope(r)
					if err != nil || env.Type&^rpcBinary != rpcInstallSnapshot {
						return // what an old version does with an unknown type
					}
					data, _ := encodeRPC(InstallSnapshotReply{Term: 7})
					var buf bytes.Buffer
					writeEnvelope(&buf, rpcEnvelope{Type: env.Type, Payload: data})
					conn.Write(buf.Bytes())
				}
			}(conn)
		}
	}()
	cli := NewTCPTransport(map[string]string{"old": ln.Addr().String()})
	defer cli.Close()
	_, err = cli.InstallSnapshot("old", InstallSnapshotArgs{Term: 1, Data: []byte("{}"), More: true})
	if !errors.Is(err, ErrSnapshotChunksUnsupported) {
		t.Fatalf("chunk to old peer: %v, want ErrSnapshotChunksUnsupported", err)
	}
	r, err := cli.InstallSnapshot("old", InstallSnapshotArgs{Term: 1, Data: []byte("{}")})
	if err != nil || r.Term != 7 {
		t.Fatalf("whole snapshot to old peer: %+v, %v", r, err)
	}
}

// Chunks round-trip over a real TCP transport between two new nodes.
func TestTCPTransportCarriesChunks(t *testing.T) {
	addrs := map[string]string{"f": "127.0.0.1:0"}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrs["f"] = ln.Addr().String()
	ln.Close()
	srv := NewTCPTransport(addrs)
	defer srv.Close()
	n, err := NewNodeWithMembers("f", Membership{"f": "", "L": ""}, srv, make(chan ApplyMsg, 8), "")
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	go srv.Serve("f")
	cli := NewTCPTransport(addrs)
	defer cli.Close()
	data := bigSnapshot(100)
	var r InstallSnapshotReply
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err = cli.InstallSnapshot("f", InstallSnapshotArgs{Term: 1, LeaderId: "L", LastIncludedIndex: 3,
			LastIncludedTerm: 1, Data: data[:40], More: true})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || r.Next != 40 || r.Done {
		t.Fatalf("chunk 0 over TCP: %+v, %v", r, err)
	}
	r, err = cli.InstallSnapshot("f", InstallSnapshotArgs{Term: 1, LeaderId: "L", LastIncludedIndex: 3,
		LastIncludedTerm: 1, Offset: 40, Data: data[40:]})
	if err != nil || !r.Done {
		t.Fatalf("last chunk over TCP: %+v, %v", r, err)
	}
	if snap, _ := n.LatestSnapshot(); snap == nil || !bytes.Equal(snap.Data, data) {
		t.Fatal("snapshot not installed from chunks")
	}
}
