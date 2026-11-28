package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// diskNode starts a node whose only peer ("L") is never reachable, so its
// state changes only through the Handle* calls the test makes.
func diskNode(t *testing.T, dir string) *Node {
	t.Helper()
	n, err := NewNode("f", []string{"L"}, NewMemTransport(), make(chan ApplyMsg, 64),
		filepath.Join(dir, "state.json"), filepath.Join(dir, "snap.json"))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func restart(t *testing.T, n *Node, dir string) *Node {
	t.Helper()
	n.Stop()
	return diskNode(t, dir)
}

func entries(term uint64, cmds ...string) []LogEntry {
	out := make([]LogEntry, len(cmds))
	for i, c := range cmds {
		out[i] = LogEntry{Term: term, Command: []byte(c)}
	}
	return out
}

// logOf returns (snapshotIndex, terms+commands of log[1:]) for comparison.
func logOf(n *Node) (uint64, []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	for _, e := range n.log[1:] {
		out = append(out, string(rune('0'+e.Term))+":"+string(e.Command))
	}
	return n.snapshotIndex, out
}

func assertLog(t *testing.T, n *Node, wantStart uint64, want ...string) {
	t.Helper()
	start, got := logOf(n)
	if start != wantStart || len(got) != len(want) {
		t.Fatalf("log = start %d %v, want start %d %v", start, got, wantStart, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("log = start %d %v, want start %d %v", start, got, wantStart, want)
		}
	}
}

func TestLogSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	n := diskNode(t, dir)
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", Entries: entries(7, "a", "b", "c")})
	n = restart(t, n, dir)
	defer n.Stop()
	assertLog(t, n, 0, "7:a", "7:b", "7:c")
	if s := n.State(); s.Term != 7 {
		t.Fatalf("term = %d, want 7", s.Term)
	}
}

// A follower's truncation of a conflicting suffix must be durable: after a
// restart the old entries must not come back.
func TestTruncationSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	n := diskNode(t, dir)
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", Entries: entries(7, "a", "b", "c")})
	// New leader in term 8 overwrites from index 2.
	n.HandleAppendEntries(AppendEntriesArgs{Term: 8, LeaderId: "L", PrevLogIndex: 1, PrevLogTerm: 7,
		Entries: entries(8, "x")})
	assertLog(t, n, 0, "7:a", "8:x")
	n = restart(t, n, dir)
	defer n.Stop()
	assertLog(t, n, 0, "7:a", "8:x")
}

// No-ops (nil Command) and empty commands must stay distinct on disk:
// applyLoop skips the former and delivers the latter.
func TestNilVersusEmptyCommandOnDisk(t *testing.T) {
	dir := t.TempDir()
	n := diskNode(t, dir)
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L",
		Entries: []LogEntry{{Term: 7}, {Term: 7, Command: []byte{}}}})
	n = restart(t, n, dir)
	defer n.Stop()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.log[1].Command != nil || n.log[2].Command == nil {
		t.Fatalf("nil/empty commands not preserved: %#v", n.log[1:])
	}
}

// A crash after the snapshot file was written but before the log file was
// rewritten leaves a log that starts before the snapshot. Restart must trim
// it — and must rewrite the file, or a log that conflicted with the
// snapshot would take entries appended later down with it on the next
// restart.
func TestSnapshotNewerThanLogFile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		snapTerm  uint64
		wantAfter []string
	}{
		{"log agrees with snapshot", 7, []string{"7:d", "7:e", "9:f"}},
		{"log conflicts with snapshot", 9, []string{"9:f"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			n := diskNode(t, dir)
			n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", Entries: entries(7, "a", "b", "c", "d", "e")})
			n.mu.Lock()
			n.writeSnapshot(Snapshot{LastIncludedIndex: 3, LastIncludedTerm: tc.snapTerm, Data: json.RawMessage(`{}`)})
			n.mu.Unlock() // "crash" before the log file is rewritten
			n = restart(t, n, dir)

			// Append after the restart, from wherever the log now ends.
			last, _ := func() (uint64, uint64) { n.mu.Lock(); defer n.mu.Unlock(); return n.lastLogIndexAndTerm() }()
			n.mu.Lock()
			prevTerm := n.termAt(last)
			n.mu.Unlock()
			n.HandleAppendEntries(AppendEntriesArgs{Term: 9, LeaderId: "L", PrevLogIndex: last, PrevLogTerm: prevTerm,
				Entries: entries(9, "f")})
			n = restart(t, n, dir)
			defer n.Stop()
			assertLog(t, n, 3, tc.wantAfter...)
		})
	}
}

func TestTornLogTailRecovered(t *testing.T) {
	dir := t.TempDir()
	n := diskNode(t, dir)
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", Entries: entries(7, "a", "b")})
	n.Stop()
	f, _ := os.OpenFile(filepath.Join(dir, "state.json.log"), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{1, 2, 3, 4, 5, 6, 7})
	f.Close()
	n = diskNode(t, dir)
	assertLog(t, n, 0, "7:a", "7:b")
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", PrevLogIndex: 2, PrevLogTerm: 7, Entries: entries(7, "c")})
	n = restart(t, n, dir)
	defer n.Stop()
	assertLog(t, n, 0, "7:a", "7:b", "7:c")
}

// State files written before the append-only log existed (log inside the
// JSON) are migrated on first start.
func TestLegacyStateFileMigrates(t *testing.T) {
	dir := t.TempDir()
	start := uint64(0)
	legacy, _ := json.Marshal(persistentState{CurrentTerm: 4, VotedFor: "x", LogStart: &start,
		Log: append([]LogEntry{{Term: 0}}, entries(3, "a", "b")...)})
	os.WriteFile(filepath.Join(dir, "state.json"), legacy, 0o644)

	n := diskNode(t, dir)
	assertLog(t, n, 0, "3:a", "3:b")
	if s := n.State(); s.Term < 4 {
		t.Fatalf("term = %d, want >= 4", s.Term)
	}
	var ps persistentState
	data, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	json.Unmarshal(data, &ps)
	if len(ps.Log) != 0 {
		t.Fatal("log still stored in the JSON file after migration")
	}
	n = restart(t, n, dir)
	defer n.Stop()
	assertLog(t, n, 0, "3:a", "3:b")
}

// TakeSnapshot compacts the log file too, not just memory.
func TestSnapshotCompactsLogFile(t *testing.T) {
	dir := t.TempDir()
	n := diskNode(t, dir)
	var cmds []string
	for i := 0; i < 200; i++ {
		cmds = append(cmds, "command-payload-padding-padding-padding")
	}
	n.HandleAppendEntries(AppendEntriesArgs{Term: 7, LeaderId: "L", Entries: entries(7, cmds...), LeaderCommit: 200})
	logFile := filepath.Join(dir, "state.json.log")
	before, _ := os.Stat(logFile)
	for n.State().LastApplied < 200 {
		select {
		case <-n.applyCh:
		default:
		}
	}
	if err := n.TakeSnapshot(199, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(logFile)
	if after.Size()*10 > before.Size() {
		t.Fatalf("log file %d bytes after snapshot, was %d: not compacted", after.Size(), before.Size())
	}
	n = restart(t, n, dir)
	defer n.Stop()
	assertLog(t, n, 199, "7:command-payload-padding-padding-padding")
}

// A single disk-backed node has no followers to form a majority, so its
// proposals commit only once persistLoop has made them durable — and they
// must survive a restart.
func TestSingleNodeOnDiskCommitsAfterFsync(t *testing.T) {
	dir := t.TempDir()
	mk := func() (*Node, chan ApplyMsg) {
		ch := make(chan ApplyMsg, 64)
		n, err := NewNode("solo", nil, NewMemTransport(), ch,
			filepath.Join(dir, "state.json"), filepath.Join(dir, "snap.json"))
		if err != nil {
			t.Fatal(err)
		}
		return n, ch
	}
	n, ch := mk()
	deadline := time.Now().Add(2 * time.Second)
	for n.State().Role != Leader && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < 20; i++ {
		if _, _, ok := n.Propose([]byte{byte(i)}); !ok {
			t.Fatal("propose rejected")
		}
	}
	for got := 0; got < 20; got++ {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 20 proposals committed", got)
		}
	}
	n.Stop()
	n, _ = mk()
	defer n.Stop()
	n.mu.Lock()
	cmds := 0
	for _, e := range n.log[1:] {
		if e.Command != nil {
			cmds++
		}
	}
	n.mu.Unlock()
	if cmds != 20 {
		t.Fatalf("%d of 20 committed commands survived restart", cmds)
	}
}
