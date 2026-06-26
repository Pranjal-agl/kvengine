package raft

import (
	"fmt"
	"sync"
)

// Transport is the networking abstraction for Raft RPCs. In tests we use
// MemTransport (in-process, with partition injection). A real TCP transport
// would implement the same interface — the algorithm doesn't care.
type Transport interface {
	RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	Register(id string, node *Node)
}

// MemTransport is a fully in-process transport: RPC calls are direct method
// invocations on the target Node (under the target's lock). Supports
// partition injection — a partitioned link returns an error, simulating a
// dropped packet. Safe for concurrent use from many goroutines.
type MemTransport struct {
	mu         sync.RWMutex
	nodes      map[string]*Node
	partitions map[string]map[string]bool // partitions[a][b] = true → a cannot reach b
}

func NewMemTransport() *MemTransport {
	return &MemTransport{
		nodes:      make(map[string]*Node),
		partitions: make(map[string]map[string]bool),
	}
}

func (t *MemTransport) Unregister(id string) {
	t.mu.Lock()
	delete(t.nodes, id)
	t.mu.Unlock()
}

func (t *MemTransport) Register(id string, node *Node) {
	t.mu.Lock()
	t.nodes[id] = node
	t.mu.Unlock()
}

// Partition makes it so that from cannot reach to (unidirectional). Call
// both Partition(a,b) and Partition(b,a) for a full bidirectional split.
func (t *MemTransport) Partition(from, to string) {
	t.mu.Lock()
	if t.partitions[from] == nil {
		t.partitions[from] = make(map[string]bool)
	}
	t.partitions[from][to] = true
	t.mu.Unlock()
}

// Heal removes a partition link so from can reach to again.
func (t *MemTransport) Heal(from, to string) {
	t.mu.Lock()
	if t.partitions[from] != nil {
		delete(t.partitions[from], to)
	}
	t.mu.Unlock()
}

// HealAll removes all partition links.
func (t *MemTransport) HealAll() {
	t.mu.Lock()
	t.partitions = make(map[string]map[string]bool)
	t.mu.Unlock()
}

func (t *MemTransport) isPartitioned(from, to string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.partitions[from] != nil && t.partitions[from][to]
}

func (t *MemTransport) getNode(id string) (*Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	return n, ok
}

func (t *MemTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	if t.isPartitioned(args.CandidateId, peer) {
		return RequestVoteReply{}, fmt.Errorf("partitioned: %s cannot reach %s", args.CandidateId, peer)
	}
	node, ok := t.getNode(peer)
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("unknown peer %s", peer)
	}
	return node.HandleRequestVote(args), nil
}

func (t *MemTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if t.isPartitioned(args.LeaderId, peer) {
		return AppendEntriesReply{}, fmt.Errorf("partitioned: %s cannot reach %s", args.LeaderId, peer)
	}
	node, ok := t.getNode(peer)
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("unknown peer %s", peer)
	}
	return node.HandleAppendEntries(args), nil
}
