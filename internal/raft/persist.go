package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// persistentState is the subset of Raft state that MUST survive a crash.
// Without this, a restarted node can vote twice in the same term (violating
// Election Safety) or forget committed log entries.
//
// The three fields that must be persisted (per §5.4 of the Raft paper):
//   - currentTerm: so we never regress to an older term after restart
//   - votedFor:    so we never grant two votes in the same term
//   - log:         so we never lose committed entries
//
// LogStart is the absolute index of Log[0] (the snapshot sentinel). It is
// needed because the snapshot file and this file are written separately: if
// a crash lands between them, the snapshot can be newer than this log, and
// restoreState must know which entries the snapshot already covers.
type persistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	LogStart    *uint64    `json:"log_start,omitempty"` // nil in files written before this field existed
	Log         []LogEntry `json:"log"`
}

// persist atomically writes the node's current persistent state to disk.
// Must be called with n.mu held, BEFORE sending any RPC reply or returning
// from a Handle* method — if we crash between updating in-memory state and
// calling persist(), the node will behave incorrectly on restart.
//
// Atomic write: encode → write tmp → fsync → rename → fsync dir.
// If we crash mid-write, the old state file is still intact (rename is
// atomic on POSIX); the tmp file is just garbage.
//
// A node that cannot persist cannot safely vote or acknowledge entries, so
// a failure here panics (as etcd/raft does for storage errors) rather than
// letting the node carry on with state that would be lost on restart.
//
// If statePath is "" (in-memory tests), persist is a no-op.
func (n *Node) persist() {
	if n.statePath == "" {
		return
	}
	start := n.snapshotIndex
	data, err := json.Marshal(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		LogStart:    &start,
		Log:         n.log,
	})
	if err != nil {
		panic(fmt.Sprintf("raft: persist marshal: %v", err))
	}
	if err := writeFileAtomic(n.statePath, data); err != nil {
		panic(fmt.Sprintf("raft: persist: %v", err))
	}
}

// restoreState reads the persisted state from disk and applies it to the
// node. Called once during NewNode, after the snapshot (if any) has set
// snapshotIndex/snapshotTerm. Returns nil if the file doesn't exist — fresh
// node, start from scratch, which is correct.
func (n *Node) restoreState() error {
	if n.statePath == "" {
		return nil
	}
	data, err := os.ReadFile(n.statePath)
	if os.IsNotExist(err) {
		return nil // fresh node — nothing to restore
	}
	if err != nil {
		return fmt.Errorf("raft: restoreState read: %w", err)
	}
	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("raft: restoreState unmarshal: %w", err)
	}
	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	if len(ps.Log) == 0 {
		return nil
	}

	logStart := n.snapshotIndex // legacy files: log[0] was the snapshot sentinel
	if ps.LogStart != nil {
		logStart = *ps.LogStart
	}
	switch {
	case logStart == n.snapshotIndex:
		n.log = ps.Log
	case logStart < n.snapshotIndex:
		// Snapshot is newer than the persisted log (crash between writing
		// the two). Keep only the entries after the snapshot, and only if
		// the log agrees with the snapshot at its last index.
		pos := n.snapshotIndex - logStart
		if pos < uint64(len(ps.Log)) && ps.Log[pos].Term == n.snapshotTerm {
			n.log = append([]LogEntry{{Term: n.snapshotTerm}}, ps.Log[pos+1:]...)
		}
	default:
		// Log starts after the snapshot: the snapshot file is missing or
		// older than the log, so entries in between are unrecoverable.
		return fmt.Errorf("raft: persisted log starts at %d but snapshot ends at %d", logStart, n.snapshotIndex)
	}
	n.log[0].Command = nil
	return nil
}

// writeFileAtomic writes data to path via tmp → fsync → rename → fsync dir.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}
