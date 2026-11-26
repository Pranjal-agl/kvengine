// Package raft implements the Raft consensus algorithm: leader election
// and log replication. See docs/ARCHITECTURE.md "Phase 5: Raft" for design
// decisions. Read the Ongaro & Ousterhout paper before modifying this file —
// the algorithm's correctness depends on a small set of invariants that are
// easy to accidentally break.
//
// Implemented: leader election, log replication with fast backup,
// persistence of currentTerm/votedFor/log (persist.go), log compaction via
// snapshots (snapshot.go) and InstallSnapshot for followers that fall behind
// the compaction point.
//
// Indexing: Raft indices are absolute and never reset. After compaction,
// n.log holds only the suffix starting at snapshotIndex — n.log[0] is a
// sentinel standing in for the last compacted entry (its Term is
// snapshotTerm). All access goes through the helpers in this file
// (lastIndex, termAt, entryAt, sliceFrom) which translate absolute indices to
// slice positions. Never index n.log directly with a Raft index.
//
// Known limitations: no cluster membership changes, no linearizable reads
// (ReadIndex / leases), no pre-vote.
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
//
// An ApplyMsg carries either a committed command or, when SnapshotValid is
// set, a snapshot received from the leader via InstallSnapshot. On a
// snapshot message the state machine must replace its entire state with
// Snapshot; the next command will have index SnapshotIndex+1.
type ApplyMsg struct {
	CommandIndex uint64 // 1-indexed Raft log position
	CommandTerm  uint64 // term of the entry; lets callers detect overwritten proposals
	Command      []byte

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
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
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderId          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}
type InstallSnapshotReply struct {
	Term uint64
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
// External goroutines (transport, tests) call the Handle* methods under
// their own goroutines, each acquiring mu. Timer goroutines (electionLoop,
// heartbeatLoop) also acquire mu when they fire. The invariant: mu is NEVER
// held across a transport RPC call — those are spawned as separate
// goroutines.
type Node struct {
	mu    sync.Mutex
	id    string
	peers []string

	// statePath: file for durable Raft state. "" = no persistence (tests).
	statePath    string
	snapshotPath string // path to snapshot file; "" = keep snapshots in memory only

	// Snapshot state: the index/term of the last compacted log entry, and
	// the state machine bytes at that point (kept in memory so the leader
	// can send InstallSnapshot without re-reading the file).
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte

	// Persistent state — must be written to disk before any RPC reply.
	currentTerm uint64
	votedFor    string
	log         []LogEntry // log[0] is a sentinel for snapshotIndex

	// Volatile state
	commitIndex   uint64
	lastApplied   uint64
	role          Role
	leaderId      string
	lastHeartbeat time.Time
	pendingSnap   *ApplyMsg // installed snapshot not yet delivered to applyCh

	// Leader-only volatile state
	nextIndex        map[string]uint64
	matchIndex       map[string]uint64
	snapshotInFlight map[string]bool

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
//
// If a snapshot exists, the caller must restore its state machine from
// LatestSnapshot() before consuming applyCh: the first command delivered
// after a restart is at index snapshotIndex+1.
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
	snap, err := n.LatestSnapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: read snapshot: %w", err)
	}
	if snap != nil {
		n.snapshotIndex = snap.LastIncludedIndex
		n.snapshotTerm = snap.LastIncludedTerm
		n.snapshotData = []byte(snap.Data)
		n.commitIndex = snap.LastIncludedIndex
		n.lastApplied = snap.LastIncludedIndex
		n.log = []LogEntry{{Term: snap.LastIncludedTerm}}
	}
	if err := n.restoreState(); err != nil {
		return nil, fmt.Errorf("raft: restore: %w", err)
	}
	transport.Register(id, n)
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
	return n, nil
}

// Stop shuts down the node. Safe to call multiple times. After Stop
// returns, the node ignores incoming RPCs and late RPC replies and never
// writes to disk again, so its files can be reused by a new Node.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		close(n.stopCh)
		n.mu.Unlock()
	})
}

// stopped reports whether Stop has been called. Checked with mu held by
// every path that mutates state, so a stopped node is inert.
func (n *Node) stopped() bool {
	select {
	case <-n.stopCh:
		return true
	default:
		return false
	}
}

// --- Log index helpers (all called with mu held) ---

// lastIndex is the absolute index of the last entry in the log.
func (n *Node) lastIndex() uint64 { return n.snapshotIndex + uint64(len(n.log)) - 1 }

// termAt returns the term of the entry at absolute index i. Valid for
// snapshotIndex <= i <= lastIndex().
func (n *Node) termAt(i uint64) uint64 { return n.log[i-n.snapshotIndex].Term }

// entryAt returns the entry at absolute index i (snapshotIndex < i <= lastIndex()).
func (n *Node) entryAt(i uint64) LogEntry { return n.log[i-n.snapshotIndex] }

// hasIndex reports whether the log (including the sentinel) covers i.
func (n *Node) hasIndex(i uint64) bool { return i >= n.snapshotIndex && i <= n.lastIndex() }

func (n *Node) lastLogIndexAndTerm() (uint64, uint64) {
	last := n.lastIndex()
	return last, n.termAt(last)
}

// truncateAfter keeps entries up to and including absolute index i.
func (n *Node) truncateAfter(i uint64) { n.log = n.log[:i-n.snapshotIndex+1] }

// --- State accessor (safe to call from any goroutine) ---

type NodeState struct {
	Role        Role
	Term        uint64
	LeaderId    string
	CommitIndex uint64
	LastApplied uint64
}

func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeState{n.role, n.currentTerm, n.leaderId, n.commitIndex, n.lastApplied}
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
		if !n.stopped() && n.role != Leader && time.Since(n.lastHeartbeat) >= timeout {
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
	n.leaderId = ""
	n.lastHeartbeat = time.Now() // prevent back-to-back elections
	n.persist()                  // must persist before sending RequestVote RPCs

	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogIndexAndTerm()

	votes := 1                       // voted for self; guarded by n.mu
	needed := (len(n.peers)+1)/2 + 1 // majority of total cluster size
	if votes >= needed {
		n.becomeLeader() // single-node cluster
		return
	}

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
			if n.stopped() {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}
			// Discard stale replies: we may have moved to a new term or role
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= needed {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeFollower moves to Follower. If term is newer than ours, it adopts
// the term and clears votedFor. Called with mu held.
//
// votedFor is cleared ONLY when the term advances. Clearing it on a
// same-term message (e.g. the first heartbeat from the leader we just
// voted for) would let this node grant a second vote in the same term to a
// late RequestVote from another candidate — two leaders in one term.
func (n *Node) becomeFollower(term uint64) {
	n.role = Follower
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.leaderId = ""
		n.persist() // must persist before any reply
	}
}

func (n *Node) becomeLeader() {
	// Called with mu held. Only reachable from Candidate state.
	n.role = Leader
	n.leaderId = n.id
	lastIdx := n.lastIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	n.snapshotInFlight = make(map[string]bool, len(n.peers))
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
			if !n.stopped() && n.role == Leader {
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

// sendAppendEntries builds and dispatches one AppendEntries RPC to peer, or
// an InstallSnapshot if the entries the peer needs have been compacted
// away. Called with mu held; spawns a goroutine for the actual call.
func (n *Node) sendAppendEntries(peer string) {
	nextIdx := n.nextIndex[peer]
	if nextIdx <= n.snapshotIndex {
		n.sendInstallSnapshot(peer)
		return
	}
	if last := n.lastIndex(); nextIdx > last+1 {
		nextIdx = last + 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAt(prevLogIndex)

	var entries []LogEntry
	if n.lastIndex() >= nextIdx {
		src := n.log[nextIdx-n.snapshotIndex:]
		entries = make([]LogEntry, len(src))
		copy(entries, src)
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
		if n.stopped() {
			return
		}

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
			return
		}
		// Fast backup: use conflict hints to jump nextIndex to the right
		// spot rather than decrementing one entry at a time. If the jump
		// lands at or below our snapshot, the next send becomes an
		// InstallSnapshot.
		next := n.nextIndex[peer]
		if reply.ConflictTerm != 0 {
			found := uint64(0)
			for i := n.lastIndex(); i > n.snapshotIndex; i-- {
				if n.termAt(i) == reply.ConflictTerm {
					found = i
					break
				}
			}
			if found > 0 {
				next = found + 1
			} else {
				next = reply.ConflictIndex
			}
		} else if reply.ConflictIndex > 0 {
			next = reply.ConflictIndex
		} else if next > 1 {
			next--
		}
		if next <= n.matchIndex[peer] {
			next = n.matchIndex[peer] + 1 // never regress below known-good
		}
		n.nextIndex[peer] = next
		// Retry immediately so the follower catches up without waiting
		// for the next heartbeat tick.
		n.sendAppendEntries(peer)
	}()
}

// sendInstallSnapshot ships the current snapshot to a peer whose nextIndex
// is at or below our compaction point. At most one is in flight per peer,
// since snapshots can be large and heartbeats fire every 50ms.
// Called with mu held.
func (n *Node) sendInstallSnapshot(peer string) {
	if n.snapshotInFlight[peer] {
		return
	}
	n.snapshotInFlight[peer] = true
	args := InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderId:          n.id,
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              n.snapshotData,
	}
	term := n.currentTerm

	go func() {
		reply, err := n.transport.InstallSnapshot(peer, args)

		n.mu.Lock()
		defer n.mu.Unlock()
		if n.stopped() {
			return
		}
		if n.currentTerm == term && n.snapshotInFlight != nil {
			n.snapshotInFlight[peer] = false
		}
		if err != nil {
			return
		}
		if reply.Term > n.currentTerm {
			n.becomeFollower(reply.Term)
			return
		}
		if n.role != Leader || n.currentTerm != term {
			return
		}
		if args.LastIncludedIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = args.LastIncludedIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommitIndex()
		n.sendAppendEntries(peer) // follow up with any entries after the snapshot
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
	floor := n.commitIndex
	if n.snapshotIndex > floor {
		floor = n.snapshotIndex
	}
	for idx := n.lastIndex(); idx > floor; idx-- {
		if n.termAt(idx) != n.currentTerm {
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
			n.notifyApply()
			break
		}
	}
}

func (n *Node) notifyApply() {
	select {
	case n.commitNotify <- struct{}{}:
	default:
	}
}

// --- Incoming RPC handlers (called by transport from peer goroutines) ---

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() {
		return RequestVoteReply{Term: n.currentTerm}
	}

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
		if n.votedFor != args.CandidateId {
			n.votedFor = args.CandidateId
			n.persist() // must persist votedFor before granting vote
		}
		n.lastHeartbeat = time.Now() // reset election timer on granting a vote
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() {
		return AppendEntriesReply{Term: n.currentTerm}
	}

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	n.becomeFollower(args.Term) // also steps down a same-term candidate
	n.leaderId = args.LeaderId
	n.lastHeartbeat = time.Now() // valid message from current leader

	// Entries at or below our snapshot are committed and already reflected
	// in it. Skip that prefix and check consistency at the snapshot point
	// instead, where the term is known from the snapshot.
	entries := args.Entries
	prevIdx, prevTerm := args.PrevLogIndex, args.PrevLogTerm
	if prevIdx < n.snapshotIndex {
		skip := n.snapshotIndex - prevIdx
		if skip >= uint64(len(entries)) {
			// Everything in this RPC is already covered by our snapshot.
			return AppendEntriesReply{Term: n.currentTerm, Success: true}
		}
		entries = entries[skip:]
		prevIdx, prevTerm = n.snapshotIndex, n.snapshotTerm
	}

	// Log consistency check: our log must contain an entry at prevIdx with
	// term prevTerm, otherwise the follower's log diverges from the
	// leader's at some earlier point and we must reject.
	if prevIdx > n.lastIndex() {
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: n.lastIndex() + 1, // tell leader where our log ends
		}
	}
	if t := n.termAt(prevIdx); t != prevTerm {
		// Find the first index of the conflicting term, so the leader can
		// skip the whole term in one round-trip.
		conflictIndex := prevIdx
		for conflictIndex > n.snapshotIndex+1 && n.termAt(conflictIndex-1) == t {
			conflictIndex--
		}
		return AppendEntriesReply{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  t,
		}
	}

	// Append new entries, removing any conflicting tail first. Entries we
	// already have with matching terms are skipped, so a retried or
	// reordered RPC never truncates entries that a newer RPC appended.
	changed := false
	for i, entry := range entries {
		idx := prevIdx + uint64(i) + 1
		if idx <= n.lastIndex() {
			if n.termAt(idx) == entry.Term {
				continue // already have this (idempotent on retry)
			}
			n.truncateAfter(idx - 1) // truncate divergent suffix
		}
		n.log = append(n.log, entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persist() // must persist log changes before replying success
	}

	// Advance commitIndex to match the leader, bounded by the last entry
	// this RPC proved we share with the leader.
	if args.LeaderCommit > n.commitIndex {
		lastNew := prevIdx + uint64(len(entries))
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.notifyApply()
		}
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// HandleInstallSnapshot replaces this follower's state with the leader's
// snapshot when the follower is too far behind to be caught up from the
// leader's (compacted) log.
func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped() {
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	if args.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	n.becomeFollower(args.Term)
	n.leaderId = args.LeaderId
	n.lastHeartbeat = time.Now()

	// Already have everything this snapshot contains (and it's committed):
	// installing it would move the state machine backwards.
	if args.LastIncludedIndex <= n.commitIndex {
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	// If our log has the snapshot's last entry with a matching term, the
	// entries after it are still valid (Log Matching); keep them.
	// Otherwise our whole log is superseded.
	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if n.hasIndex(args.LastIncludedIndex) && n.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog = append(newLog, n.log[args.LastIncludedIndex-n.snapshotIndex+1:]...)
	}

	snap := Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}
	if err := n.writeSnapshot(snap); err != nil {
		// Can't make the snapshot durable: don't adopt it. The leader
		// will retry.
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	n.log = newLog
	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm
	n.snapshotData = args.Data
	n.persist()

	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex
	n.pendingSnap = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	n.notifyApply()
	return InstallSnapshotReply{Term: n.currentTerm}
}

// --- Client interface ---

// Propose submits a command to be replicated. Returns (logIndex, term, isLeader).
// If isLeader is false, this node is not the current leader and the command
// was not submitted — the client should retry on another node.
//
// A true return does NOT mean the command is committed. It is committed
// once an ApplyMsg arrives with CommandIndex == logIndex and
// CommandTerm == term; if one arrives at logIndex with a different term,
// the proposal was overwritten by a new leader and was lost.
func (n *Node) Propose(command []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader || n.stopped() {
		return 0, n.currentTerm, false
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: command})
	idx := n.lastIndex()
	n.persist() // persist before broadcasting
	n.broadcastAppendEntries()
	n.maybeAdvanceCommitIndex() // commits immediately in a single-node cluster
	return idx, n.currentTerm, true
}

// --- Apply loop ---

// applyLoop is the only goroutine that sends on applyCh. It delivers, in
// order: any snapshot installed from the leader, then every committed entry
// past lastApplied.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.commitNotify:
		}
		n.mu.Lock()
		for {
			var msg ApplyMsg
			if n.pendingSnap != nil {
				msg = *n.pendingSnap
				n.pendingSnap = nil
			} else if n.lastApplied < n.commitIndex {
				n.lastApplied++
				e := n.entryAt(n.lastApplied)
				if e.Command == nil {
					continue
				}
				msg = ApplyMsg{CommandIndex: n.lastApplied, CommandTerm: e.Term, Command: e.Command}
			} else {
				break
			}
			n.mu.Unlock()
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				return
			}
			n.mu.Lock()
		}
		n.mu.Unlock()
	}
}

// LogSnapshot returns a copy of the in-memory log (for testing/inspection).
// Element 0 is the sentinel for the last compacted index (see SnapshotIndex).
func (n *Node) LogSnapshot() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]LogEntry, len(n.log))
	copy(out, n.log)
	return out
}

// SnapshotIndex returns the absolute index of the last compacted entry.
func (n *Node) SnapshotIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.snapshotIndex
}

func randDuration(min, max time.Duration) time.Duration {
	diff := int64(max - min)
	if diff <= 0 {
		return min
	}
	return min + time.Duration(rand.Int63n(diff))
}
