package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Cluster membership changes, one server at a time (Raft thesis §4.1).
//
// A configuration is the set of voting members, each with the address its
// transport listens on. Changes are ordinary log entries (Kind ==
// EntryConfig) carrying the complete new configuration. A server uses the
// latest configuration in its log as soon as it is appended — committed or
// not — and falls back to an earlier one if that entry is truncated. Before
// the first config entry, the "base" configuration applies: the one stored
// in the snapshot, or the one the node was bootstrapped with.
//
// Adding or removing a single server at a time is safe because any
// majority of the old configuration and any majority of the new one
// overlap, so two leaders can never be elected for the same term across
// the change. The rules that keep it safe:
//
//   - At most one change in flight: a new change is refused until the
//     previous config entry is committed.
//   - A leader must have committed an entry in its current term before
//     proposing a change (fixes the bug in the original single-server
//     algorithm, described on the raft-dev list in 2015; the election no-op
//     makes this cheap).
//   - A leader that removes itself keeps leading until the change commits
//     (not counting itself toward majorities), then steps down (§4.2.2).
//   - Servers ignore RequestVote while they have recently heard from a
//     current leader, so a removed server that doesn't know it was removed
//     can't disrupt the cluster by campaigning (§4.2.3).
//   - Servers that aren't members never start elections — this is also how
//     a brand-new server waits, empty, until the leader adds it.

// Membership maps member ID to its transport address. An empty address is
// allowed for in-process transports.
type Membership map[string]string

func (m Membership) clone() Membership {
	out := make(Membership, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (m Membership) equal(o Membership) bool {
	if len(m) != len(o) {
		return false
	}
	for k, v := range m {
		if ov, ok := o[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// IDs returns the member IDs in sorted order.
func (m Membership) IDs() []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// EntryKind distinguishes log entries.
type EntryKind uint8

const (
	EntryCommand EntryKind = 0 // state machine command (nil Command = leader no-op)
	EntryConfig  EntryKind = 1 // Command holds a JSON-encoded Membership
)

var (
	// ErrConfigChangeInProgress: the previous membership change hasn't
	// committed yet. Retry after it does.
	ErrConfigChangeInProgress = errors.New("raft: a membership change is already in progress")
	// ErrLeaderNotReady: a new leader hasn't committed an entry in its
	// term yet. Retry shortly.
	ErrLeaderNotReady = errors.New("raft: leader has not committed in its term yet")
	// ErrLost: the entry was overwritten by a new leader and not committed.
	ErrLost = errors.New("raft: entry was overwritten by a new leader")
	// ErrCommitTimeout: the entry did not commit in time; outcome unknown.
	ErrCommitTimeout = errors.New("raft: timed out waiting for commit")
)

// peerAddrSetter is implemented by transports that need addresses for
// members they weren't constructed with (TCPTransport).
type peerAddrSetter interface {
	SetPeerAddrs(Membership)
}

// isMember reports whether id is in the current configuration.
func (n *Node) isMember(id string) bool {
	_, ok := n.config[id]
	return ok
}

// configAt returns the configuration in effect at absolute index i (the
// latest config entry at or before i, else the base configuration).
// Called with mu held; i must be >= snapshotIndex.
func (n *Node) configAt(i uint64) (Membership, uint64) {
	for j := i; j > n.snapshotIndex; j-- {
		if e := n.entryAt(j); e.Kind == EntryConfig {
			if m, err := decodeMembership(e.Command); err == nil {
				return m, j
			}
		}
	}
	return n.baseConfig, n.snapshotIndex
}

func decodeMembership(b []byte) (Membership, error) {
	var m Membership
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = Membership{}
	}
	return m, nil
}

// recomputeConfig derives the current configuration from the log and, if
// it changed, updates peers, the transport's address book and the
// leader's per-peer replication state. Call after any change to the log or
// base configuration. Called with mu held.
func (n *Node) recomputeConfig() { n.recomputeConfigFrom(0) }

// recomputeConfigFrom is recomputeConfig when only entries at index >= from
// changed. If the entry that set the current configuration wasn't touched,
// only the new entries need scanning, which keeps follower appends O(new
// entries) rather than O(log length).
func (n *Node) recomputeConfigFrom(from uint64) {
	var cfg Membership
	var idx uint64
	if n.config != nil && from > n.configIndex && n.configIndex >= n.snapshotIndex {
		cfg, idx = n.config, n.configIndex
		for j := n.lastIndex(); j >= from && j > n.snapshotIndex; j-- {
			if e := n.entryAt(j); e.Kind == EntryConfig {
				if m, err := decodeMembership(e.Command); err == nil {
					cfg, idx = m, j
					break
				}
			}
		}
	} else {
		cfg, idx = n.configAt(n.lastIndex())
	}
	n.configIndex = idx
	if n.config != nil && cfg.equal(n.config) {
		return
	}
	n.config = cfg.clone()
	n.peers = n.peers[:0]
	for _, id := range cfg.IDs() {
		if id != n.id {
			n.peers = append(n.peers, id)
		}
	}
	if s, ok := n.transport.(peerAddrSetter); ok {
		s.SetPeerAddrs(cfg)
	}
	if n.role == Leader {
		last := n.lastIndex()
		for _, p := range n.peers {
			if _, ok := n.nextIndex[p]; !ok {
				n.nextIndex[p] = last + 1
				n.matchIndex[p] = 0
			}
		}
		for p := range n.nextIndex {
			if !n.isMember(p) {
				delete(n.nextIndex, p)
				delete(n.matchIndex, p)
				delete(n.ackRound, p)
				delete(n.snapshotInFlight, p)
			}
		}
	}
	n.signal()
}

// hasQuorum reports whether the members for which has(id) is true form a
// majority of the current configuration. An empty configuration never has
// a quorum.
func (n *Node) hasQuorum(has func(id string) bool) bool {
	count := 0
	for id := range n.config {
		if has(id) {
			count++
		}
	}
	return count*2 > len(n.config)
}

// Members returns a copy of the current configuration.
func (n *Node) Members() Membership {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.config.clone()
}

// AddServer proposes adding id (reachable at addr) as a voting member.
// Returns the log index and term of the config entry; use WaitCommitted to
// wait for it. Adding an existing member with the same address is a no-op
// that returns the current commit point.
func (n *Node) AddServer(id, addr string) (uint64, uint64, error) {
	if id == "" {
		return 0, 0, errors.New("raft: empty server id")
	}
	return n.changeConfig(func(m Membership) bool {
		if cur, ok := m[id]; ok && cur == addr {
			return false
		}
		m[id] = addr
		return true
	})
}

// RemoveServer proposes removing id from the cluster. Removing the leader
// itself is allowed: it steps down once the change commits.
func (n *Node) RemoveServer(id string) (uint64, uint64, error) {
	return n.changeConfig(func(m Membership) bool {
		if _, ok := m[id]; !ok {
			return false
		}
		delete(m, id)
		return true
	})
}

func (n *Node) changeConfig(mutate func(Membership) bool) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() || n.role != Leader {
		return 0, 0, &NotLeaderError{LeaderId: n.leaderId}
	}
	if n.termAt(n.commitIndex) != n.currentTerm {
		return 0, 0, ErrLeaderNotReady
	}
	if n.configIndex > n.commitIndex {
		return 0, 0, ErrConfigChangeInProgress
	}
	next := n.config.clone()
	if !mutate(next) {
		return n.commitIndex, n.termAt(n.commitIndex), nil // already so
	}
	if len(next) == 0 {
		return 0, 0, errors.New("raft: cannot remove the last member")
	}
	data, err := json.Marshal(next)
	if err != nil {
		return 0, 0, err
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Kind: EntryConfig, Command: data})
	idx := n.lastIndex()
	n.recomputeConfigFrom(idx) // takes effect on append, not commit
	select {
	case n.persistNotify <- struct{}{}:
	default:
	}
	n.broadcastAppendEntries()
	n.maybeAdvanceCommitIndex()
	return idx, n.currentTerm, nil
}

// WaitCommitted blocks until the entry at index commits (nil), is found to
// have been overwritten by a different entry (ErrLost), or the timeout
// passes (ErrCommitTimeout). term is the term returned when proposing it.
func (n *Node) WaitCommitted(index, term uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if n.hasIndex(index) && n.termAt(index) != term {
			return ErrLost
		}
		if n.commitIndex >= index {
			if index < n.snapshotIndex || n.termAt(index) == term {
				return nil
			}
			return ErrLost
		}
		if n.stopped() {
			return ErrStopped
		}
		ch := n.changed
		n.mu.Unlock()
		select {
		case <-ch:
		case <-timer.C:
		case <-n.stopCh:
		}
		n.mu.Lock()
		if time.Now().After(deadline) && n.commitIndex < index {
			return ErrCommitTimeout
		}
	}
}

// stepDownIfRemoved: a leader whose removal has committed stops leading
// (thesis §4.2.2). Called with mu held after commitIndex advances.
func (n *Node) stepDownIfRemoved() {
	if n.role == Leader && !n.isMember(n.id) && n.commitIndex >= n.configIndex {
		n.role = Follower
		n.leaderId = ""
		n.signal()
	}
}

func (m Membership) String() string {
	s := "{"
	for i, id := range m.IDs() {
		if i > 0 {
			s += ", "
		}
		s += id
		if m[id] != "" {
			s += "@" + m[id]
		}
	}
	return s + "}"
}

var _ fmt.Stringer = Membership(nil)
