// Package raft implements the Raft consensus algorithm: leader election
// and log replication. See docs/ARCHITECTURE.md "Phase 5: Raft" for design
// decisions. Read the Ongaro & Ousterhout paper before modifying this file —
// the algorithm's correctness depends on a small set of invariants that are
// easy to accidentally break.
//
// Known limitations (documented, not bugs):
//   - Persistent state (currentTerm, votedFor, log) is in-memory only.
//     A real deployment needs to persist these to disk before responding to
//     any RPC — without this, a restarted node can vote twice in the same term,
//     violating Election Safety. Marked as a TODO in docs/ROADMAP.md.
//   - No log compaction / snapshots: the log grows forever.
//   - No cluster membership changes.
package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 200 * time.Millisecond
	electionTimeoutMax = 400 * time.Millisecond
)

// Role is the node's current role in the Raft protocol.
type Role uint8

const (
	Follower  Role = iota
	Candidate      // voted for self, waiting for votes from peers
	Leader         // won an election, replicating log to followers
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Leader"
	}
}

// LogEntry is one entry in the Raft log: the term it was received in and
// the client command (arbitrary bytes). log[0] is always a dummy sentinel
// with Term=0 and Command=nil so that real entries start at index 1 and
// "PrevLogIndex=0" always has a valid corresponding term.
type LogEntry struct {
	Term    uint64
	Command []byte
}

// ApplyMsg is sent to applyCh when a log entry is committed by a quorum
// and ready to be applied to the state machine (the Store).
type ApplyMsg struct {
	CommandIndex uint64 // 1-indexed Raft log position
	Command      []byte
}

// --- RPC wire types ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast-backup hints so the leader doesn't decrement nextIndex one at a
	// time after a log inconsistency — it can skip straight to the right
	// position. See onAEReply for how the leader uses these.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Node is one member of a Raft cluster. All mutable state is guarded by mu.
// External goroutines (transport, tests) call HandleRequestVote and
// HandleAppendEntries under their own goroutines, each acquiring mu.
// Timer goroutines (electionLoop, heartbeatLoop) also acquire mu when
// they fire. The invariant: mu is NEVER held across a transport RPC call —
// those are spawned as separate goroutines.
type Node struct {
	mu    sync.Mutex
	id    string
	peers []string

	// statePath: file for durable Raft state. "" = no persistence (tests).
	statePath    string
	snapshotPath string // path to snapshot file; "" = no snapshots

	// Snapshot state: the index/term of the last compacted log entry.
	snapshotIndex uint64
	snapshotTerm  uint64

	// Persistent state — must be written to disk before any RPC reply.
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state
	commitIndex   uint64
	lastApplied   uint64
	role          Role
	leaderId      string
	lastHeartbeat time.Time

	// Leader-only volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Infrastructure
	transport    Transport
	applyCh      chan ApplyMsg
	commitNotify chan struct{}
	stopOnce     sync.Once // ensures Stop() is idempotent
	stopCh       chan struct{}
}

// NewNode creates a Raft node. statePath and snapshotPath may be "" to
// disable persistence (in-memory tests only). For real deployments, pass
// paths like "/var/lib/kvengine/raft-state.json" and
// "/var/lib/kvengine/raft-snapshot.json".
func NewNode(id string, peers []string, transport Transport, applyCh chan ApplyMsg, statePath string, snapshotPath ...string) (*Node, error) {
	sp := ""
	if len(snapshotPath) > 0 {
		sp = snapshotPath[0]
	}
	n := &Node{
		id:           id,
		peers:        peers,
		statePath:    statePath,
		snapshotPath: sp,
		transport:    transport,
		applyCh:      applyCh,
		log:          []LogEntry{{Term: 0}},
		role:         Follower,
		commitNotify: make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	// Restore snapshot first (sets snapshotIndex/snapshotTerm), then
	// persistent state (may override log with entries after the snapshot).
	if snap, err := n.LatestSnapshot(); err == nil && snap != nil {
		n.snapshotIndex = snap.LastIncludedIndex
		n.snapshotTerm = snap.LastIncludedTerm
		n.commitIndex = snap.LastIncludedIndex
		n.lastApplied = snap.LastIncludedIndex
		n.log = []LogEntry{{Term: snap.LastIncludedTerm}}
	}
	if err := n.restoreState(); err != nil {
		return nil, fmt.Errorf("raft: restore: %w", err)
	}
	if len(n.log) == 0 {
		n.log = []LogEntry{{Term: 0}}
	}
	transport.Register(id, n)
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
	return n, nil
}

// Stop shuts down the node. Safe to call multiple times.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// --- State accessor (safe to call from any goroutine) ---

type NodeState struct {
	Role     Role
	Term     uint64
	LeaderId string
}

func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeState{n.role, n.currentTerm, n.leaderId}
}

// --- Election ---

// electionLoop fires when we haven't heard from a leader in time and we're
// not already the leader. Uses the "lastHeartbeat" pattern: sleep for a
// random timeout, then check if enough time has actually elapsed (guards
// against spurious wakeups and handles the case where the sleep itself
// took longer than expected).
func (n *Node) electionLoop() {
	for {
		timeout := randDuration(electionTimeoutMin, electionTimeoutMax)
		select {
		case <-n.stopCh:
			return
		case <-time.After(timeout):
		}

		n.mu.Lock()
		if n.role != Leader && time.Since(n.lastHeartbeat) >= timeout {
			n.startElection() // called with mu held
		}
		n.mu.Unlock()
	}
}

// startElection transitions us to Candidate, increments the term, votes
// for ourself, and sends RequestVote to all peers in separate goroutines.
// Called with n.mu held; stays locked on return.
func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.lastHeartbeat = time.Now() // prevent back-to-back elections
	n.persist()                  // must persist before sending RequestVote RPCs

	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogIndexAndTerm()

	var votesMu sync.Mutex
	votes := 1                       // voted for self
	needed := (len(n.peers)+1)/2 + 1 // majority of total cluster size

	for _, peer := range n.peers {
		go func(peer string) {
			reply, err := n.transport.RequestVote(peer, RequestVoteArgs{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			// Discard stale replies: we may have moved to a new term or role
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				won := votes >= needed
				votesMu.Unlock()
				if won {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeFollower(term uint64) {
	// Called with mu held.
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persist() //nolint: must persist before any reply
}

func (n *Node) becomeLeader() {
	// Called with mu held. Only reachable from Candidate state.
	n.role = Leader
	n.leaderId = n.id
	lastIdx, _ := n.lastLogIndexAndTerm()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}
	// Immediate heartbeat asserts leadership and prevents a new election.
	n.broadcastAppendEntries()
}

// --- Log replication ---

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role == Leader {
				n.broadcastAppendEntries()
			}
			n.mu.Unlock()
		}
	}
}

// broadcastAppendEntries sends AppendEntries to all peers. Called with mu held.
// Each peer gets its own goroutine so one slow/partitioned peer doesn't
// block the others.
func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		n.sendAppendEntries(peer)
	}
}

// sendAppendEntries builds and dispatches one AppendEntries RPC to peer.
// Called with mu held; spawns a goroutine for the actual call.
func (n *Node) sendAppendEntries(peer string) {
	nextIdx := n.nextIndex[peer]
	if nextIdx < 1 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.log[prevLogIndex].Term

	var entries []LogEntry
	if uint64(len(n.log)) > nextIdx {
		entries = make([]LogEntry, uint64(len(n.log))-nextIdx)
		copy(entries, n.log[nextIdx:])
	}

	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm

	go func() {
		reply, err := n.transport.AppendEntries(peer, args)
		if err != nil {
			return
		}

		n.mu.Lock()
		defer n.mu.Unlock()

		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			return
		}
		if n.role != Leader || n.currentTerm != term {
			return // stale reply
		}

		if reply.Success {
			newMatch := args.PrevLogIndex + uint64(len(args.Entries))
			if newMatch > n.matchIndex[peer] {
				n.matchIndex[peer] = newMatch
			}
			n.nextIndex[peer] = n.matchIndex[peer] + 1
			n.maybeAdvanceCommitIndex()
		} else {
			// Fast backup: use conflict hints to jump nextIndex to the right
			// spot rather than decrementing one entry at a time.
			if reply.ConflictTerm != 0 {
				// Find the last entry in our log with ConflictTerm.
				found := uint64(0)
				for i := uint64(len(n.log) - 1); i > 0; i-- {
					if n.log[i].Term == reply.ConflictTerm {
						found = i
						break
					}
				}
				if found > 0 {
					n.nextIndex[peer] = found + 1
				} else {
					n.nextIndex[peer] = reply.ConflictIndex
				}
			} else if reply.ConflictIndex > 0 {
				n.nextIndex[peer] = reply.ConflictIndex
			} else if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
			// Retry immediately so the follower catches up without waiting
			// for the next heartbeat tick.
			n.sendAppendEntries(peer)
		}
	}()
}

// maybeAdvanceCommitIndex checks whether commitIndex can be advanced: finds
// the highest log index N such that log[N].Term == currentTerm and a
// majority of nodes (including self) have matchIndex >= N.
//
// The "log[N].Term == currentTerm" rule is critical: Raft only commits
// entries from the *current* term by counting replicas. Entries from
// previous terms get committed indirectly once a current-term entry is
// committed after them (the Log Matching property ensures everything before
// the commit point is also committed). Without this rule, a leader could
// incorrectly commit an old entry that gets overwritten by a future leader.
func (n *Node) maybeAdvanceCommitIndex() {
	for idx := uint64(len(n.log) - 1); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1 // self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count*2 > len(n.peers)+1 { // majority
			n.commitIndex = idx
			select {
			case n.commitNotify <- struct{}{}:
			default:
			}
			break
		}
	}
}

// --- Incoming RPC handlers (called by transport from peer goroutines) ---

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Grant vote if: haven't voted yet (or already voted for this candidate)
	// AND candidate's log is at least as up-to-date as ours.
	// "Up-to-date" = higher last term, or same last term and at least as long.
	lastIdx, lastTerm := n.lastLogIndexAndTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	canVote := n.votedFor == "" || n.votedFor == args.CandidateId

	if logOK && canVote {
		n.votedFor = args.CandidateId
		n.persist()                  // must persist votedFor before granting vote
		n.lastHeartbeat = time.Now() // reset election timer on granting a vote
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.Term >= n.currentTerm {
		n.becomeFollower(args.Term)
		n.leaderId = args.LeaderId
	}
	n.lastHeartbeat = time.Now() // valid message from current leader

	// Log consistency check: our log must contain an entry at PrevLogIndex
	// with term PrevLogTerm, otherwise the follower's log diverges from the
	// leader's at some earlier point and we must reject.
	if args.PrevLogIndex >= uint64(len(n.log)) {
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: uint64(len(n.log)), // tell leader our log length
			ConflictTerm:  0,
		}
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		// Find the first index where this conflicting term appears, so the
		// leader can skip the whole term in one round-trip.
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 1 && n.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}
	}

	// Append new entries, removing any conflicting tail first.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx] // truncate divergent suffix
			} else {
				continue // already have this (idempotent on retry)
			}
		}
		n.log = append(n.log, entry)
	}
	if len(args.Entries) > 0 {
		n.persist() // must persist log changes before replying success
	}

	// Advance commitIndex to match the leader, bounded by our log length.
	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		select {
		case n.commitNotify <- struct{}{}:
		default:
		}
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// --- Client interface ---

// Propose submits a command to be replicated. Returns (logIndex, term, isLeader).
// If isLeader is false, this node is not the current leader and the command
// was not submitted — the client should retry on another node (or wait for
// redirection, which is not implemented here).
func (n *Node) Propose(command []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, n.currentTerm, false
	}
	entry := LogEntry{Term: n.currentTerm, Command: command}
	n.log = append(n.log, entry)
	idx := uint64(len(n.log) - 1)
	n.matchIndex[n.id] = idx // leader counts itself
	n.persist()              // persist before broadcasting
	n.broadcastAppendEntries()
	return idx, n.currentTerm, true
}

// --- Apply loop ---

// applyLoop watches for commitIndex advancing past lastApplied and sends
// committed entries to applyCh for the state machine (Store) to consume.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.commitNotify:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log[n.lastApplied]
				idx := n.lastApplied
				n.mu.Unlock()

				if entry.Command != nil {
					select {
					case n.applyCh <- ApplyMsg{CommandIndex: idx, Command: entry.Command}:
					case <-n.stopCh:
						return
					}
				}
				n.mu.Lock()
			}
			n.mu.Unlock()
		}
	}
}

// LogSnapshot returns a copy of the current log (for testing/inspection).
func (n *Node) LogSnapshot() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]LogEntry, len(n.log))
	copy(out, n.log)
	return out
}

func (n *Node) lastLogIndexAndTerm() (uint64, uint64) {
	last := uint64(len(n.log) - 1)
	return last, n.log[last].Term
}

func randDuration(min, max time.Duration) time.Duration {
	diff := int64(max - min)
	if diff <= 0 {
		return min
	}
	return min + time.Duration(rand.Int63n(diff))
}
