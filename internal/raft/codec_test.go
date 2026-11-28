package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func codecSamples() []any {
	return []any{
		RequestVoteArgs{Term: 7, CandidateId: "n1", LastLogIndex: 1 << 40, LastLogTerm: 6},
		RequestVoteReply{Term: 9, VoteGranted: true},
		AppendEntriesArgs{Term: 3, LeaderId: "L", PrevLogIndex: 10, PrevLogTerm: 2, LeaderCommit: 9,
			Entries: []LogEntry{
				{Term: 3},                                // no-op: nil command
				{Term: 3, Command: []byte{}},             // empty command
				{Term: 3, Command: []byte{0, 0xff, 'x'}}, // binary command
				{Term: 3, Kind: EntryConfig, Command: []byte(`{"a":"1"}`)},
			}},
		AppendEntriesArgs{Term: 1, LeaderId: "L"}, // heartbeat: no entries
		AppendEntriesReply{Term: 4, Success: false, ConflictIndex: 77, ConflictTerm: 2},
		InstallSnapshotArgs{Term: 5, LeaderId: "L", LastIncludedIndex: 100, LastIncludedTerm: 4,
			Data: []byte(`{"k":"v"}`), Config: Membership{"a": "h:1", "b": ""}},
		InstallSnapshotArgs{Term: 5, LeaderId: "L", Data: []byte{}, Config: Membership{}},
		InstallSnapshotArgs{Term: 5, LeaderId: "L"}, // nil data, nil config
		InstallSnapshotReply{Term: 8},
		snapshotChunkArgs{Term: 5, LeaderId: "L", LastIncludedIndex: 100, LastIncludedTerm: 4,
			Data: []byte{1, 2, 3}, Config: Membership{"a": "h:1"}, Offset: 1 << 33, More: true},
		snapshotChunkArgs{Term: 5, LeaderId: "L", Offset: 9},
		snapshotChunkReply{Term: 8, Next: 1 << 20, Done: true},
		snapshotChunkReply{Term: 8},
		TimeoutNowArgs{Term: 12, LeaderId: "L"},
		TimeoutNowReply{Term: 13},
		ReadIndexArgs{From: "f"},
		ReadIndexReply{Index: 12, OK: true, LeaderId: "L"},
	}
}

func TestCodecRoundTrip(t *testing.T) {
	for _, v := range codecSamples() {
		b, err := encodeRPC(v)
		if err != nil {
			t.Fatal(err)
		}
		ptr := reflect.New(reflect.TypeOf(v))
		if err := decodeRPC(b, ptr.Interface()); err != nil {
			t.Fatalf("%T: %v", v, err)
		}
		if got := ptr.Elem().Interface(); !reflect.DeepEqual(got, v) {
			t.Fatalf("%T round trip:\n got %#v\nwant %#v", v, got, v)
		}
	}
}

func TestCodecRejectsTruncationAndTrailingBytes(t *testing.T) {
	for _, v := range codecSamples() {
		b, _ := encodeRPC(v)
		ptr := reflect.New(reflect.TypeOf(v))
		for cut := 0; cut < len(b); cut++ {
			if decodeRPC(b[:cut], ptr.Interface()) == nil {
				// A prefix can only decode if it is itself a complete
				// message; with every field present that never happens
				// for the shorter forms of these types.
				if cut != 0 || len(b) == 0 {
					t.Fatalf("%T: truncated to %d/%d bytes decoded without error", v, cut, len(b))
				}
			}
		}
		if decodeRPC(append(append([]byte{}, b...), 0), ptr.Interface()) == nil {
			t.Fatalf("%T: trailing byte accepted", v)
		}
	}
}

// FuzzDecodeRPC: arbitrary bytes must never panic or allocate wildly, and
// anything that decodes must re-encode to the same bytes.
func FuzzDecodeRPC(f *testing.F) {
	for _, v := range codecSamples() {
		b, _ := encodeRPC(v)
		f.Add(b)
	}
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		targets := []any{&RequestVoteArgs{}, &RequestVoteReply{}, &AppendEntriesArgs{}, &AppendEntriesReply{},
			&InstallSnapshotArgs{}, &InstallSnapshotReply{}, &ReadIndexArgs{}, &ReadIndexReply{},
			&snapshotChunkArgs{}, &snapshotChunkReply{}, &TimeoutNowArgs{}, &TimeoutNowReply{}}
		for _, p := range targets {
			if decodeRPC(b, p) != nil {
				continue
			}
			out, err := encodeRPC(reflect.ValueOf(p).Elem().Interface())
			if err != nil {
				t.Fatal(err)
			}
			// Canonical form: decode(encode(x)) must be stable.
			p2 := reflect.New(reflect.TypeOf(p).Elem()).Interface()
			if err := decodeRPC(out, p2); err != nil || !reflect.DeepEqual(p, p2) {
				t.Fatalf("%T not stable under re-encoding: %v", p, err)
			}
		}
	})
}

// Old nodes send JSON (type codes without the binary bit). A new server
// must still answer them, in JSON, so clusters can upgrade node by node.
func TestTCPTransportAnswersLegacyJSON(t *testing.T) {
	addrs := map[string]string{"srv": freeAddr(t), "cli": freeAddr(t)}
	srv, n := startServer(t, addrs)
	defer n.Stop()
	defer srv.Close()
	conn, err := net.DialTimeout("tcp", addrs["srv"], time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	payload, _ := json.Marshal(RequestVoteArgs{Term: 1, CandidateId: "cli"})
	var buf bytes.Buffer
	writeEnvelope(&buf, rpcEnvelope{Type: rpcRequestVote, Payload: payload})
	conn.Write(buf.Bytes())
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	env, err := readEnvelope(bufio.NewReader(conn))
	if err != nil {
		t.Fatal(err)
	}
	var reply RequestVoteReply
	if env.Type != rpcRequestVote || json.Unmarshal(env.Payload, &reply) != nil {
		t.Fatalf("legacy request not answered in JSON: type %d payload %q", env.Type, env.Payload)
	}
}

// legacyServer mimics a pre-binary kvengine: it answers JSON requests and
// hangs up on anything else.
func legacyServer(t *testing.T) (addr string, binaryHits *atomic.Int64) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	hits := &atomic.Int64{}
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
					if err != nil {
						return
					}
					if env.Type&rpcBinary != 0 {
						hits.Add(1)
						return // old version: unknown type, close without replying
					}
					data, _ := json.Marshal(RequestVoteReply{Term: 42, VoteGranted: true})
					var buf bytes.Buffer
					writeEnvelope(&buf, rpcEnvelope{Type: env.Type, Payload: data})
					conn.Write(buf.Bytes())
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), hits
}

// Rolling upgrade: a new node talking to an old one falls back to JSON
// after one failed binary attempt, and keeps using JSON afterwards.
func TestTCPTransportFallsBackToJSONForOldPeers(t *testing.T) {
	addr, binaryHits := legacyServer(t)
	cli := NewTCPTransport(map[string]string{"old": addr})
	defer cli.Close()
	for i := 0; i < 5; i++ {
		r, err := cli.RequestVote("old", RequestVoteArgs{Term: 1, CandidateId: "new"})
		if err != nil || r.Term != 42 || !r.VoteGranted {
			t.Fatalf("call %d to old peer: %+v, %v", i, r, err)
		}
	}
	if n := binaryHits.Load(); n != 1 {
		t.Fatalf("binary attempted %d times against a JSON-only peer, want 1", n)
	}
}
