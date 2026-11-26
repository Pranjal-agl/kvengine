// Package raftkv runs a key-value store as a Raft state machine: writes are
// proposed to Raft and acknowledged only once committed by a quorum and
// applied locally; reads are served from the local state machine.
//
// It satisfies server.KVStore, so the TCP server can sit directly on top.
//
// Consistency: a successful Put/Delete is durable on a majority and visible
// to subsequent Gets on the same node. Gets do not go through Raft, so a
// follower (or a leader that has just been deposed without knowing it yet)
// can return stale data. Linearizable reads would need ReadIndex or leader
// leases, which are not implemented.
package raftkv

import (
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
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	kv := &KV{
		node:    node,
		applyCh: applyCh,
		sm:      store.NewMemory(),
		opts:    opts,
		waiters: make(map[uint64]*waiter),
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

// Get reads from the local state machine (see package doc on staleness).
func (kv *KV) Get(key []byte) ([]byte, bool) { return kv.sm.Get(key) }

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
	kv.applied = msg.SnapshotIndex
	for idx, w := range kv.waiters {
		if idx <= msg.SnapshotIndex {
			w.done <- ErrUnknown
			delete(kv.waiters, idx)
		}
	}
	kv.mu.Unlock()
}

func (kv *KV) applyCommand(msg raft.ApplyMsg) {
	var cmd command
	var err error
	if err = json.Unmarshal(msg.Command, &cmd); err != nil {
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
	kv.applied = msg.CommandIndex
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

// Applied returns the index of the last entry applied to the state machine.
func (kv *KV) Applied() uint64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.applied
}
