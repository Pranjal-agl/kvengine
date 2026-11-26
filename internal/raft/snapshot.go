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
// Raft paper §7: once a snapshot is taken at index N, log entries 1..N can
// be deleted. The snapshot stores the last included index and term so the
// node can still answer "what was the term at N?" for AppendEntries
// consistency checks — the answer comes from the snapshot, not the (now
// truncated) log.
type Snapshot struct {
	LastIncludedIndex uint64          `json:"last_included_index"`
	LastIncludedTerm  uint64          `json:"last_included_term"`
	Data              json.RawMessage `json:"data"` // opaque state machine bytes (must be valid JSON)
}

// TakeSnapshot compacts the log up to (and including) upToIndex.
// stateMachineData must be the state machine's state after applying
// exactly the entries up to upToIndex, and upToIndex must already have been
// delivered on applyCh. It must be valid JSON (it is embedded in the
// snapshot file as-is).
//
// After this call, entries up to upToIndex are gone from memory and disk;
// log[0] becomes a sentinel carrying upToIndex's term so prevLogTerm lookups
// at the compaction point still work. Calling with an index at or below the
// current snapshot is a no-op.
func (n *Node) TakeSnapshot(upToIndex uint64, stateMachineData []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upToIndex <= n.snapshotIndex || n.stopped() {
		return nil // already compacted past this point (or shut down)
	}
	if upToIndex > n.lastApplied {
		return fmt.Errorf("raft: TakeSnapshot: upToIndex %d > lastApplied %d", upToIndex, n.lastApplied)
	}
	if !json.Valid(stateMachineData) {
		return fmt.Errorf("raft: TakeSnapshot: state machine data is not valid JSON")
	}

	lastTerm := n.termAt(upToIndex)
	snap := Snapshot{
		LastIncludedIndex: upToIndex,
		LastIncludedTerm:  lastTerm,
		Data:              json.RawMessage(stateMachineData),
	}
	// Snapshot first, then the truncated log: if we crash in between, the
	// old log still covers everything (see restoreState).
	if err := n.writeSnapshot(snap); err != nil {
		return err
	}

	newLog := make([]LogEntry, 1, n.lastIndex()-upToIndex+1)
	newLog[0] = LogEntry{Term: lastTerm} // sentinel with preserved term
	newLog = append(newLog, n.log[upToIndex-n.snapshotIndex+1:]...)
	n.log = newLog
	n.snapshotIndex = upToIndex
	n.snapshotTerm = lastTerm
	n.snapshotData = stateMachineData
	n.persist()
	return nil
}

// LatestSnapshot returns the most recently saved snapshot, or nil if none.
// With no snapshot path configured, returns the in-memory snapshot.
func (n *Node) LatestSnapshot() (*Snapshot, error) {
	n.mu.Lock()
	path := n.snapshotPath
	if path == "" {
		defer n.mu.Unlock()
		if n.snapshotIndex == 0 {
			return nil, nil
		}
		return &Snapshot{n.snapshotIndex, n.snapshotTerm, json.RawMessage(n.snapshotData)}, nil
	}
	n.mu.Unlock()

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
	if err := writeFileAtomic(n.snapshotPath, data); err != nil {
		return fmt.Errorf("raft: snapshot: %w", err)
	}
	return nil
}
