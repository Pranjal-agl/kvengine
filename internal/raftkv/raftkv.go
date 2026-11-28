// Package raftkv runs a key-value store as a Raft state machine: writes are
// proposed to Raft and acknowledged only once committed by a quorum and
// applied locally; reads are served from the local state machine.
//
// It satisfies server.KVStore, so the TCP server can sit directly on top.
//
// Consistency: a successful Put/Delete is durable on a majority. Reads
// through GetErr are linearizable by default, on any node: they obtain a
// Raft read index (raft.Node.ReadIndex — a quorum heartbeat round, plus a
// round trip to the leader when called on a follower), wait for the local
// state machine to apply it, then read locally. A read therefore reflects
// every write that completed before it began, and a deposed leader cannot
// serve stale data. Options.StaleReads skips the read index for lower
// latency at the cost of possibly stale results.
package raftkv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"kvengine/internal/raft"
	"kvengine/internal/store"
)

var (
	// ErrLost means the proposal was overwritten by a new leader and was
	// definitely not applied. Safe to retry.
	ErrLost = errors.New("raftkv: write lost to a leadership change; retry")
	// ErrTimeout means the write was not seen to commit in time. It may
	// still commit later; the outcome is unknown.
	ErrTimeout = errors.New("raftkv: timed out waiting for commit; outcome unknown")
	// ErrUnknown means a snapshot from a new leader replaced the state
	// machine before this node could see whether the write committed.
	ErrUnknown = errors.New("raftkv: outcome unknown (state replaced by leader snapshot); retry")
)

// NotLeaderError is returned for writes sent to a node that is not the
// leader. LeaderID is the Raft ID of the leader this node last heard from,
// or "" if unknown.
type NotLeaderError struct{ LeaderID string }

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not leader (leader unknown, retry shortly)"
	}
	return fmt.Sprintf("not leader (leader is %s)", e.LeaderID)
}

// Options configure a KV.
type Options struct {
	// SnapshotEvery takes a Raft snapshot after this many applied
	// commands, bounding log size. 0 disables snapshots.
	SnapshotEvery int
	// WriteTimeout bounds how long Put/Delete wait for commit. Default 5s.
	WriteTimeout time.Duration
	// ReadTimeout bounds how long a linearizable read waits to confirm
	// leadership and catch up. Default 2s.
	ReadTimeout time.Duration
	// StaleReads serves GetErr straight from the local state machine,
	// skipping the read index: faster, but may return stale data.
	StaleReads bool
	// Logger receives operational messages. Default: log.Default().
	Logger *log.Logger
}

// command is the Raft log entry payload. []byte fields encode as base64,
// so binary keys and values survive the JSON round-trip intact.
type command struct {
	Op  string `json:"op"` // "put" or "del"
	Key []byte `json:"k"`
	Val []byte `json:"v,omitempty"`
}

// legacyCommand is the payload v1.0 wrote: a map[string]string, so keys
// and values are plain JSON strings rather than base64.
type legacyCommand struct {
	Op  string `json:"op"`
	Key string `json:"k"`
	Val string `json:"v"`
}

// decodeCommand decodes a log entry written by this version or by v1.0.
//
// The two formats share field names but not field types: v1.0 stored k and
// v as strings, this version as base64. Decoding a v1.0 entry as base64
// fails for most keys and, worse, silently yields different bytes for keys
// that happen to be valid base64 ("user", "abcd"). They are told apart by
// field order, which encoding/json fixes: v1.0 marshalled a map, whose keys
// come out sorted ({"k":...,"op":...}); this version marshals the command
// struct, whose fields come out in declaration order ({"op":...}).
func decodeCommand(data []byte) (command, error) {
	var cmd command
	if bytes.HasPrefix(data, []byte(`{"k":`)) {
		var old legacyCommand
		if err := json.Unmarshal(data, &old); err != nil {
			return cmd, err
		}
		cmd = command{Op: old.Op, Key: []byte(old.Key)}
		if old.Op == "put" {
			cmd.Val = []byte(old.Val)
		}
		return cmd, nil
	}
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

type waiter struct {
	term uint64
	done chan error
}

// KV is a Raft-replicated key-value store.
type KV struct {
	node    *raft.Node
	applyCh <-chan raft.ApplyMsg
	sm      *store.Store
	opts    Options

	mu      sync.Mutex
	waiters map[uint64]*waiter // by log index
	applied uint64             // last applied index (commands or snapshot)
	advance chan struct{}      // closed and replaced whenever applied moves

	sinceSnapshot int // commands applied since the last snapshot; apply loop only

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// New restores the state machine from the node's latest snapshot (if any)
// and starts consuming applyCh, which must be the channel the node was
// created with. Call Close to stop.
func New(node *raft.Node, applyCh <-chan raft.ApplyMsg, opts Options) (*KV, error) {
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 5 * time.Second
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = 2 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	kv := &KV{
		node:    node,
		applyCh: applyCh,
		sm:      store.NewMemory(),
		opts:    opts,
		waiters: make(map[uint64]*waiter),
		advance: make(chan struct{}),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	snap, err := node.LatestSnapshot()
	if err != nil {
		return nil, fmt.Errorf("raftkv: read snapshot: %w", err)
	}
	if snap != nil {
		if err := kv.sm.RestoreSnapshot(snap.Data); err != nil {
			return nil, fmt.Errorf("raftkv: restore snapshot: %w", err)
		}
		kv.applied = snap.LastIncludedIndex
	}
	go kv.applyLoop()
	return kv, nil
}

// Close stops applying entries. It does not stop the Raft node.
func (kv *KV) Close() error {
	kv.stopOnce.Do(func() { close(kv.stop) })
	<-kv.done
	return kv.sm.Close()
}

// Put replicates key=value through Raft and returns once it is committed
// and applied on this node.
func (kv *KV) Put(key, value []byte) error {
	return kv.propose(command{Op: "put", Key: key, Val: value})
}

// Delete replicates the deletion of key through Raft.
func (kv *KV) Delete(key []byte) error {
	return kv.propose(command{Op: "del", Key: key})
}

// Get reads straight from the local state machine, without a read index.
// It may be stale; use GetErr for linearizable reads. (Get exists to
// satisfy server.KVStore; the server prefers GetErr when available.)
func (kv *KV) Get(key []byte) ([]byte, bool) { return kv.sm.Get(key) }

// GetErr is a linearizable read (unless Options.StaleReads): it returns the
// value as of some point between the call and its return, reflecting every
// write acknowledged before the call. Returns *NotLeaderError if no leader
// can confirm the read, or ErrTimeout.
func (kv *KV) GetErr(key []byte) ([]byte, bool, error) {
	if !kv.opts.StaleReads {
		deadline := time.Now().Add(kv.opts.ReadTimeout)
		idx, err := kv.node.ReadIndex(kv.opts.ReadTimeout)
		if err != nil {
			var nle *raft.NotLeaderError
			if errors.As(err, &nle) {
				return nil, false, &NotLeaderError{LeaderID: nle.LeaderId}
			}
			return nil, false, fmt.Errorf("raftkv: read: %w", err)
		}
		if err := kv.waitApplied(idx, deadline); err != nil {
			return nil, false, err
		}
	}
	v, ok := kv.sm.Get(key)
	return v, ok, nil
}

// waitApplied blocks until the state machine has applied index idx.
func (kv *KV) waitApplied(idx uint64, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		kv.mu.Lock()
		applied, ch := kv.applied, kv.advance
		kv.mu.Unlock()
		if applied >= idx {
			return nil
		}
		select {
		case <-ch:
		case <-timer.C:
			return fmt.Errorf("raftkv: read: timed out catching up to index %d (at %d)", idx, applied)
		case <-kv.stop:
			return ErrTimeout
		}
	}
}

// setApplied records progress and wakes readers. Called with kv.mu held.
func (kv *KV) setApplied(idx uint64) {
	kv.applied = idx
	close(kv.advance)
	kv.advance = make(chan struct{})
}

// Len returns the number of keys in the local state machine.
func (kv *KV) Len() int { return kv.sm.Len() }

func (kv *KV) propose(cmd command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// Hold mu across Propose so the apply loop cannot apply this index
	// before the waiter is registered.
	kv.mu.Lock()
	idx, term, ok := kv.node.Propose(data)
	if !ok {
		kv.mu.Unlock()
		return &NotLeaderError{LeaderID: kv.node.State().LeaderId}
	}
	w := &waiter{term: term, done: make(chan error, 1)}
	kv.waiters[idx] = w
	kv.mu.Unlock()

	timer := time.NewTimer(kv.opts.WriteTimeout)
	defer timer.Stop()
	select {
	case err := <-w.done:
		return err
	case <-timer.C:
	case <-kv.stop:
	}
	kv.mu.Lock()
	delete(kv.waiters, idx)
	kv.mu.Unlock()
	return ErrTimeout
}

func (kv *KV) applyLoop() {
	defer close(kv.done)
	for {
		var msg raft.ApplyMsg
		select {
		case <-kv.stop:
			kv.failAll(ErrTimeout)
			return
		case msg = <-kv.applyCh:
		}
		if msg.SnapshotValid {
			kv.applySnapshot(msg)
		} else {
			kv.applyCommand(msg)
		}
	}
}

func (kv *KV) applySnapshot(msg raft.ApplyMsg) {
	if err := kv.sm.RestoreSnapshot(msg.Snapshot); err != nil {
		// The leader sent a snapshot we can't decode; continuing would
		// diverge from the cluster.
		kv.opts.Logger.Fatalf("raftkv: cannot restore snapshot at index %d: %v", msg.SnapshotIndex, err)
	}
	kv.mu.Lock()
	kv.setApplied(msg.SnapshotIndex)
	for idx, w := range kv.waiters {
		if idx <= msg.SnapshotIndex {
			w.done <- ErrUnknown
			delete(kv.waiters, idx)
		}
	}
	kv.mu.Unlock()
}

func (kv *KV) applyCommand(msg raft.ApplyMsg) {
	cmd, err := decodeCommand(msg.Command)
	if err != nil {
		kv.opts.Logger.Printf("raftkv: skipping undecodable command at index %d: %v", msg.CommandIndex, err)
	} else {
		switch cmd.Op {
		case "put":
			err = kv.sm.Put(cmd.Key, cmd.Val)
		case "del":
			err = kv.sm.Delete(cmd.Key)
		default:
			err = fmt.Errorf("unknown op %q", cmd.Op)
		}
	}

	kv.mu.Lock()
	kv.setApplied(msg.CommandIndex)
	// Any waiter at or below this index is now decided: the entry at its
	// index either is the one it proposed (same term) or replaced it.
	// Indices skipped by apply (leader no-ops) count as replaced too.
	for idx, w := range kv.waiters {
		if idx > msg.CommandIndex {
			continue
		}
		if idx == msg.CommandIndex && w.term == msg.CommandTerm {
			w.done <- err
		} else {
			w.done <- ErrLost
		}
		delete(kv.waiters, idx)
	}
	kv.mu.Unlock()

	// Count commands rather than testing CommandIndex%N: indices skip
	// leader no-ops, so a modulo test could miss its target for a long time.
	kv.sinceSnapshot++
	if kv.opts.SnapshotEvery > 0 && kv.sinceSnapshot >= kv.opts.SnapshotEvery {
		kv.sinceSnapshot = 0
		kv.snapshot(msg.CommandIndex)
	}
}

func (kv *KV) snapshot(index uint64) {
	data, err := kv.sm.Snapshot()
	if err == nil {
		err = kv.node.TakeSnapshot(index, data)
	}
	if err != nil {
		kv.opts.Logger.Printf("raftkv: snapshot at index %d failed: %v", index, err)
	}
}

func (kv *KV) failAll(err error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for idx, w := range kv.waiters {
		w.done <- err
		delete(kv.waiters, idx)
	}
}

// AddNode adds a voting member (id reachable at its Raft address addr)
// and waits for the change to commit. The new server should already be
// running with an empty membership (kvengine raft --join) so it can be
// caught up. Only one change can be in flight at a time.
func (kv *KV) AddNode(id, addr string) error {
	return kv.changeMembership(func() (uint64, uint64, error) { return kv.node.AddServer(id, addr) })
}

// RemoveNode removes a member and waits for the change to commit. Removing
// the current leader is allowed: it hands off by stepping down once the
// change commits.
func (kv *KV) RemoveNode(id string) error {
	return kv.changeMembership(func() (uint64, uint64, error) { return kv.node.RemoveServer(id) })
}

func (kv *KV) changeMembership(propose func() (uint64, uint64, error)) error {
	idx, term, err := propose()
	if err != nil {
		var nle *raft.NotLeaderError
		if errors.As(err, &nle) {
			return &NotLeaderError{LeaderID: nle.LeaderId}
		}
		return err
	}
	switch err := kv.node.WaitCommitted(idx, term, kv.opts.WriteTimeout); {
	case errors.Is(err, raft.ErrLost):
		return ErrLost
	case errors.Is(err, raft.ErrCommitTimeout):
		return ErrTimeout
	default:
		return err
	}
}

// MembersInfo describes the current membership, one "id addr" line per
// member with the leader marked, for the MEMBERS command.
func (kv *KV) MembersInfo() string {
	m := kv.node.Members()
	leader := kv.node.State().LeaderId
	var out string
	for _, id := range m.IDs() {
		line := id + " " + m[id]
		if id == leader {
			line += " leader"
		}
		out += line + "\n"
	}
	return out
}

// Applied returns the index of the last entry applied to the state machine.
func (kv *KV) Applied() uint64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.applied
}
