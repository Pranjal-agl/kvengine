package raft

import (
	"encoding/json"
	"fmt"
	"os"
)

// Snapshot captures the state machine at a point in time so the Raft log
// prefix before that point can be safely discarded. This solves the
// "log grows forever" problem.
//
// Raft paper §7: once a snapshot is taken at index N, log entries 1..N-1
// can be deleted. The snapshot stores the last included index and term so
// the node can still answer "what was log[N].Term?" for AppendEntries
// consistency checks — the answer comes from the snapshot header, not the
// (now truncated) log.
type Snapshot struct {
	LastIncludedIndex uint64          `json:"last_included_index"`
	LastIncludedTerm  uint64          `json:"last_included_term"`
	Data              json.RawMessage `json:"data"` // opaque state machine bytes
}

// TakeSnapshot compacts the log up to (and including) upToIndex.
// upToIndex must be <= commitIndex — we can only snapshot committed state.
// The state machine data is whatever the caller passes in (e.g. a JSON
// snapshot of the Store's in-memory map).
//
// After this call, log entries 1..upToIndex are deleted. The sentinel at
// log[0] is replaced with a dummy entry carrying LastIncludedTerm so that
// prevLogTerm lookups still work for the first real entry after the snapshot.
func (n *Node) TakeSnapshot(upToIndex uint64, stateMachineData []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upToIndex > n.commitIndex {
		return fmt.Errorf("raft: TakeSnapshot: upToIndex %d > commitIndex %d", upToIndex, n.commitIndex)
	}
	if upToIndex < n.snapshotIndex {
		return nil // already compacted past this point
	}
	if upToIndex >= uint64(len(n.log)) {
		return fmt.Errorf("raft: TakeSnapshot: upToIndex %d out of range (log len %d)", upToIndex, len(n.log))
	}

	lastTerm := n.log[upToIndex].Term

	snap := Snapshot{
		LastIncludedIndex: upToIndex,
		LastIncludedTerm:  lastTerm,
		Data:              json.RawMessage(stateMachineData),
	}
	if err := n.writeSnapshot(snap); err != nil {
		return err
	}

	// Truncate the log: keep entries after upToIndex, but replace log[0]
	// with a sentinel that carries lastTerm so prevLogTerm lookups work.
	newLog := make([]LogEntry, 1, uint64(len(n.log))-upToIndex)
	newLog[0] = LogEntry{Term: lastTerm} // sentinel with preserved term
	newLog = append(newLog, n.log[upToIndex+1:]...)
	n.log = newLog

	n.snapshotIndex = upToIndex
	n.snapshotTerm = lastTerm
	return n.persist()
}

// LatestSnapshot returns the most recently saved snapshot, or nil if none.
func (n *Node) LatestSnapshot() (*Snapshot, error) {
	n.mu.Lock()
	path := n.snapshotPath
	n.mu.Unlock()
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (n *Node) writeSnapshot(snap Snapshot) error {
	if n.snapshotPath == "" {
		return nil // no persistence configured
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("raft: snapshot marshal: %w", err)
	}
	tmp := n.snapshotPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("raft: snapshot create: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: snapshot write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: snapshot fsync: %w", err)
	}
	f.Close()
	return os.Rename(tmp, n.snapshotPath)
}
