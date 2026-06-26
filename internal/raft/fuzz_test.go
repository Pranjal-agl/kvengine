package raft

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// FuzzHandleRequestVote feeds arbitrary JSON to the RequestVote handler.
// The handler must never panic regardless of what fields are present,
// missing, or set to extreme values (MaxUint64 term, empty candidate ID, etc).
func FuzzHandleRequestVote(f *testing.F) {
	// Seed with valid RPC messages.
	valid, _ := json.Marshal(RequestVoteArgs{Term: 1, CandidateId: "n0", LastLogIndex: 1, LastLogTerm: 1})
	f.Add(valid)

	// Interesting edge cases.
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"term":18446744073709551615}`)) // MaxUint64
	f.Add([]byte(`{"term":-1}`))                   // negative (will decode as 0 in uint64)
	f.Add([]byte(`{"candidate_id":""}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(``))
	f.Add([]byte(`{"term":1,"candidate_id":"` + string(make([]byte, 10000)) + `"}`)) // huge string

	f.Fuzz(func(t *testing.T, data []byte) {
		node := fuzzNode(t)
		defer node.Stop()

		var args RequestVoteArgs
		if err := json.Unmarshal(data, &args); err != nil {
			return // fuzzer exploring; invalid JSON is fine to skip
		}
		// Must not panic.
		node.HandleRequestVote(args)
	})
}

// FuzzHandleAppendEntries feeds arbitrary JSON to the AppendEntries handler.
func FuzzHandleAppendEntries(f *testing.F) {
	valid, _ := json.Marshal(AppendEntriesArgs{
		Term: 1, LeaderId: "n0",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Term: 1, Command: []byte("cmd")}},
		LeaderCommit: 1,
	})
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"term":999999,"leader_id":"x","prev_log_index":999999,"prev_log_term":999999,"leader_commit":999999}`))
	f.Add([]byte(`{"entries":[{"term":1,"command":"aGVsbG8="},{"term":1,"command":"aGVsbG8="}]}`))
	// Huge entries slice.
	hugeEntries := `{"entries":[` + `{"term":1,"command":"dA=="},` + `{"term":1,"command":"dA=="}` + `]}`
	f.Add([]byte(hugeEntries))
	f.Add([]byte(`null`))

	f.Fuzz(func(t *testing.T, data []byte) {
		node := fuzzNode(t)
		defer node.Stop()

		var args AppendEntriesArgs
		if err := json.Unmarshal(data, &args); err != nil {
			return
		}
		// Cap entries to prevent OOM.
		if len(args.Entries) > 1000 {
			return // not t.Skip()
		}
		// Must not panic.
		node.HandleAppendEntries(args)
	})
}

// fuzzNode builds a minimal single-node cluster for fuzz testing.
// Returned node must be stopped by caller.
func fuzzNode(t *testing.T) *Node {
	t.Helper()
	tr := NewMemTransport()
	ch := make(chan ApplyMsg, 64)
	sp := filepath.Join(t.TempDir(), "fuzz-state.json")
	n, err := NewNode("fuzz", []string{}, tr, ch, sp)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}
