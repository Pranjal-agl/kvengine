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
type persistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// persist atomically writes the node's current persistent state to disk.
// Must be called with n.mu held, BEFORE sending any RPC reply or returning
// from HandleRequestVote/HandleAppendEntries — if we crash between updating
// in-memory state and calling persist(), the node will behave incorrectly
// on restart.
//
// Atomic write: encode → write tmp → fsync → rename.
// If we crash mid-write, the old state file is still intact (rename is
// atomic on POSIX); the tmp file is just garbage.
//
// If statePath is "" (MemTransport-only tests), persist is a no-op.
func (n *Node) persist() error {
	if n.statePath == "" {
		return nil
	}
	ps := persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("raft: persist marshal: %w", err)
	}
	tmp := n.statePath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft: persist create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: persist write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: persist fsync: %w", err)
	}
	f.Close()
	if err := os.Rename(tmp, n.statePath); err != nil {
		return fmt.Errorf("raft: persist rename: %w", err)
	}
	return nil
}

// restoreState reads the persisted state from disk and applies it to the node.
// Called once during NewNode before the node starts processing any messages.
// Returns nil (no error) if the file doesn't exist — fresh node, start from
// scratch, which is correct.
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
	n.log = ps.Log
	return nil
}

// stateDir returns the directory for a node's state given its statePath.
// Used to ensure the directory exists before writing.
func stateDir(statePath string) string {
	return filepath.Dir(statePath)
}
