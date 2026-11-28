package raft

import (
	"errors"
	"fmt"
	"time"
)

// Leadership transfer (Raft thesis §3.10).
//
// The leader hands leadership to a chosen member instead of letting the
// cluster find out it's gone by timing out:
//
//  1. The leader stops accepting proposals and membership changes, so the
//     target can catch up with a log that has stopped growing.
//  2. It replicates to the target until the target's log matches its own.
//  3. It sends TimeoutNow. The target starts an election straight away,
//     with RequestVoteArgs.Transfer set, which tells followers that still
//     hear from the old leader not to ignore it (§4.2.3 ignores candidates
//     while a leader is alive; that rule exists to stop *disruptive*
//     elections, and this one is wanted). Every other voting rule — one
//     vote per term, the candidate's log at least as up to date — applies
//     unchanged, so safety doesn't depend on the transfer at all.
//  4. The target's higher term reaches the old leader, which steps down.
//
// If the target hasn't become leader within the timeout, the transfer is
// abandoned and the leader resumes accepting proposals.
//
// Used by: TransferLeadership (the TRANSFER admin command, and kvengine's
// SIGTERM handler, so a rolling restart has no leaderless gap), and a
// leader whose own removal commits, which hands off to the most up-to-date
// remaining member instead of leaving the cluster to time out.

// TimeoutNowArgs asks a member to start an election immediately.
type TimeoutNowArgs struct {
	Term     uint64
	LeaderId string
}

type TimeoutNowReply struct {
	Term uint64
}

var (
	// ErrTransferInProgress: a leadership transfer is under way; the leader
	// isn't accepting proposals or membership changes until it ends.
	ErrTransferInProgress = errors.New("raft: leadership transfer in progress")
	// ErrTransferTimeout: the target didn't become leader in time. The old
	// leader is still leader (or the cluster elected someone else).
	ErrTransferTimeout = errors.New("raft: leadership transfer timed out")
)

// TransferLeadership hands leadership to target, a voting member, or to the
// most up-to-date other member if target is "". It returns once target is
// leader, or ErrTransferTimeout. Transferring to the current leader is a
// no-op.
func (n *Node) TransferLeadership(target string, timeout time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() || n.role != Leader {
		return &NotLeaderError{LeaderId: n.leaderId}
	}
	if n.transferTo != "" {
		return ErrTransferInProgress
	}
	if target == "" {
		target = n.bestTransferTarget()
		if target == "" {
			return errors.New("raft: no other member to transfer leadership to")
		}
	}
	if target == n.id {
		return nil
	}
	if !n.isMember(target) {
		return fmt.Errorf("raft: %s is not a voting member", target)
	}

	term := n.currentTerm
	n.transferTo = target
	sent := false
	n.sendAppendEntries(target)
	deadline := time.Now().Add(timeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if n.stopped() {
			return ErrStopped
		}
		if n.currentTerm > term {
			// Someone won an election after ours started; we stepped down.
			if n.leaderId == target {
				return nil
			}
			if n.leaderId != "" {
				return fmt.Errorf("raft: leadership went to %s, not %s", n.leaderId, target)
			}
		} else if n.role != Leader {
			return &NotLeaderError{LeaderId: n.leaderId}
		} else if !sent && n.matchIndex[target] >= n.lastIndex() {
			// The target has everything; tell it to campaign now.
			n.sendTimeoutNow(target)
			sent = true
		}
		if !time.Now().Before(deadline) {
			if n.currentTerm == term && n.transferTo == target {
				n.transferTo = "" // resume accepting proposals
				n.signal()
			}
			return fmt.Errorf("%w (target %s)", ErrTransferTimeout, target)
		}
		ch := n.changed
		n.mu.Unlock()
		select {
		case <-ch:
		case <-timer.C:
		case <-n.stopCh:
		}
		n.mu.Lock()
	}
}

// bestTransferTarget returns the member, other than this node, whose log
// is furthest along (ties broken by ID for determinism), or "". Called
// with mu held, as leader.
func (n *Node) bestTransferTarget() string {
	best, bestMatch := "", uint64(0)
	for _, id := range n.config.IDs() {
		if id == n.id {
			continue
		}
		if m := n.matchIndex[id]; best == "" || m > bestMatch {
			best, bestMatch = id, m
		}
	}
	return best
}

// sendTimeoutNow asks target to start an election. Fire and forget: the
// outcome shows up as a new term. Called with mu held.
func (n *Node) sendTimeoutNow(target string) {
	args := TimeoutNowArgs{Term: n.currentTerm, LeaderId: n.id}
	go func() {
		reply, err := n.transport.TimeoutNow(target, args)
		if err != nil {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if !n.stopped() && reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
		}
	}()
}

// HandleTimeoutNow starts an election at once if the request comes from
// this node's current leader and this node is a voting member.
func (n *Node) HandleTimeoutNow(args TimeoutNowArgs) TimeoutNowReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() || args.Term < n.currentTerm {
		return TimeoutNowReply{Term: n.currentTerm}
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	if n.role == Leader || !n.isMember(n.id) {
		return TimeoutNowReply{Term: n.currentTerm}
	}
	n.startElection(true)
	return TimeoutNowReply{Term: n.currentTerm}
}

// handOffAfterRemoval: a leader whose removal has just committed asks the
// most up-to-date remaining member to campaign at once, so the cluster
// isn't leaderless for an election timeout. Called with mu held, while
// still leader.
func (n *Node) handOffAfterRemoval() {
	if target := n.bestTransferTarget(); target != "" && n.matchIndex[target] >= n.commitIndex {
		n.sendTimeoutNow(target)
	}
}
