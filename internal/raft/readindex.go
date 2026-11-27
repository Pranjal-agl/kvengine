package raft

import (
	"errors"
	"fmt"
	"time"
)

// Linearizable reads via ReadIndex (Raft thesis §6.4).
//
// A read served from a node's local state machine can be stale: a follower
// may lag, and a leader may have been deposed by a partition without
// knowing it yet. ReadIndex closes both gaps without writing to the log:
//
//  1. The leader waits until it has committed an entry from its own term
//     (the no-op from becomeLeader). Until then its commitIndex may be
//     behind what an earlier leader already committed.
//  2. It records readIndex = commitIndex.
//  3. It starts a fresh heartbeat round and waits for a majority to
//     acknowledge it. That proves no newer leader existed at the moment the
//     read arrived, so nothing beyond readIndex had been committed then.
//  4. The caller waits until its state machine has applied readIndex, then
//     reads locally.
//
// A follower gets a read index by asking the leader (the ReadIndex RPC),
// then waits for its own state machine to reach it. Either way the read
// reflects every write that completed before it started.

var (
	// ErrNotLeader matches (via errors.Is) any *NotLeaderError.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrReadTimeout means leadership could not be confirmed in time.
	ErrReadTimeout = errors.New("raft: timed out confirming read index")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("raft: node stopped")
)

// NotLeaderError reports that a read index could not be obtained because
// leadership is unknown or moved. LeaderId is a redirect hint ("" if unknown).
type NotLeaderError struct{ LeaderId string }

func (e *NotLeaderError) Error() string {
	if e.LeaderId == "" {
		return "raft: not the leader (leader unknown)"
	}
	return fmt.Sprintf("raft: not the leader (leader is %s)", e.LeaderId)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// readIndexRPCTimeout bounds how long a leader spends confirming a read
// index on behalf of a follower. A healthy leader needs one heartbeat round
// trip; one that can't get a quorum within an election timeout is very
// likely no longer the leader.
const readIndexRPCTimeout = 2 * electionTimeoutMax

// ReadIndex returns an index such that, once the caller's state machine has
// applied it, a local read is linearizable. The index is always one that
// is delivered on applyCh (or a snapshot index), never a leader no-op, so
// it is safe to wait for it by watching applyCh. On the leader this confirms
// leadership with a quorum; on a follower it asks the leader.
func (n *Node) ReadIndex(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	n.mu.Lock()
	if n.stopped() {
		n.mu.Unlock()
		return 0, ErrStopped
	}
	if n.role == Leader {
		defer n.mu.Unlock()
		return n.leaderReadIndex(deadline)
	}
	leader := n.leaderId
	n.mu.Unlock()

	if leader == "" {
		return 0, &NotLeaderError{}
	}
	reply, err := n.transport.ReadIndex(leader, ReadIndexArgs{From: n.id})
	if err != nil {
		return 0, fmt.Errorf("raft: read index from leader %s: %w", leader, err)
	}
	if !reply.OK {
		return 0, &NotLeaderError{LeaderId: reply.LeaderId}
	}
	return reply.Index, nil
}

// HandleReadIndex serves a follower's ReadIndex request. Only a leader
// answers; anyone else replies !OK with its leader hint (never forwarding,
// so requests can't loop).
func (n *Node) HandleReadIndex(args ReadIndexArgs) ReadIndexReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() || n.role != Leader {
		return ReadIndexReply{LeaderId: n.leaderId}
	}
	idx, err := n.leaderReadIndex(time.Now().Add(readIndexRPCTimeout))
	if err != nil {
		return ReadIndexReply{LeaderId: n.leaderId}
	}
	return ReadIndexReply{Index: idx, OK: true}
}

// leaderReadIndex runs steps 1-3 above. Called with mu held (released
// while waiting); returns with mu held.
func (n *Node) leaderReadIndex(deadline time.Time) (uint64, error) {
	term := n.currentTerm
	for n.termAt(n.commitIndex) != term {
		if err := n.waitChange(deadline, term); err != nil {
			return 0, err
		}
	}
	// No-ops (nil Command) don't change state and are never delivered on
	// applyCh, so a caller tracking progress from applyCh would wait
	// forever for one. Step back to the last real command: the state there
	// is identical.
	readIdx := n.commitIndex
	for readIdx > n.snapshotIndex && n.entryAt(readIdx).Command == nil {
		readIdx--
	}

	round := n.joinReadRound()
	for !n.quorumAcked(round) {
		if err := n.waitChange(deadline, term); err != nil {
			return 0, err
		}
	}
	return readIdx, nil
}

// joinReadRound returns the heartbeat round this read must see a quorum
// for. The round must start after the read arrived: if none is in flight,
// start one now; otherwise wait for the next one, which advanceReadRound
// starts as soon as the in-flight round completes. Called with mu held.
func (n *Node) joinReadRound() uint64 {
	if !n.readRoundOn {
		n.startReadRound()
		return n.readRound
	}
	n.readsQueued = true
	return n.readRound + 1
}

func (n *Node) startReadRound() {
	n.hbRound++
	n.readRound = n.hbRound
	n.readRoundOn = true
	n.readsQueued = false
	n.broadcastAppendEntries()
	if n.quorumAcked(n.readRound) { // single-node cluster
		n.readRoundOn = false
	}
}

// advanceReadRound runs after every ack: once the in-flight read round has
// a quorum, start the next one if reads are queued for it. Called with mu
// held.
func (n *Node) advanceReadRound() {
	if n.readRoundOn && n.quorumAcked(n.readRound) {
		n.readRoundOn = false
		if n.readsQueued {
			n.startReadRound()
		}
	}
}

func (n *Node) quorumAcked(round uint64) bool {
	count := 1 // self
	for _, p := range n.peers {
		if n.ackRound[p] >= round {
			count++
		}
	}
	return count*2 > len(n.peers)+1
}

// waitChange releases mu until n.changed fires, the deadline passes, or
// the node stops, then reacquires mu and checks we're still leader of term.
func (n *Node) waitChange(deadline time.Time, term uint64) error {
	ch := n.changed
	n.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	var err error
	select {
	case <-ch:
	case <-timer.C:
		err = ErrReadTimeout
	case <-n.stopCh:
		err = ErrStopped
	}
	timer.Stop()
	n.mu.Lock()
	if err != ErrStopped && (n.role != Leader || n.currentTerm != term) {
		return &NotLeaderError{LeaderId: n.leaderId}
	}
	return err
}
