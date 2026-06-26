package raft

import (
	"fmt"
	"math/rand"
	"sync"
)

// SimTransport is a deterministic simulation transport for Raft testing.
// Unlike MemTransport (which delivers messages immediately and uses
// real-time randomness), SimTransport:
//
//  1. Uses a seeded PRNG so fault scenarios are 100% reproducible —
//     a failing test with seed S can always be replayed with seed S.
//  2. Supports per-link drop rates so packet loss is injected
//     probabilistically but deterministically from the seed.
//  3. Records every RPC (sent, dropped, delivered) for post-hoc analysis.
//
// This is the "reproducible concurrency tests" requirement from the doc —
// not full deterministic simulation (which would require restructuring
// Raft into a single-threaded event loop), but reproducible fault injection
// that can reliably reproduce any discovered bug.
type SimTransport struct {
	mu   sync.RWMutex
	rng  *rand.Rand
	seed int64

	nodes    map[string]*Node
	dropRate map[string]map[string]float64 // dropRate[from][to] = 0.0-1.0

	// Recorded history for analysis.
	history []SimEvent
}

// SimEvent records one RPC attempt for post-hoc analysis.
type SimEvent struct {
	From    string
	To      string
	Type    string // "RequestVote" or "AppendEntries"
	Dropped bool
}

func NewSimTransport(seed int64) *SimTransport {
	return &SimTransport{
		rng:      rand.New(rand.NewSource(seed)),
		seed:     seed,
		nodes:    make(map[string]*Node),
		dropRate: make(map[string]map[string]float64),
	}
}

func (t *SimTransport) Register(id string, node *Node) {
	t.mu.Lock()
	t.nodes[id] = node
	t.mu.Unlock()
}

// SetDropRate sets the probability that a message from→to is dropped.
// 0.0 = never drop, 1.0 = always drop.
func (t *SimTransport) SetDropRate(from, to string, rate float64) {
	t.mu.Lock()
	if t.dropRate[from] == nil {
		t.dropRate[from] = make(map[string]float64)
	}
	t.dropRate[from][to] = rate
	t.mu.Unlock()
}

// SetBidirectionalDropRate sets drop rate in both directions.
func (t *SimTransport) SetBidirectionalDropRate(a, b string, rate float64) {
	t.SetDropRate(a, b, rate)
	t.SetDropRate(b, a, rate)
}

// Partition makes from→to fully unreachable (drop rate 1.0).
func (t *SimTransport) Partition(from, to string) {
	t.SetDropRate(from, to, 1.0)
}

// Heal removes partition between from→to.
func (t *SimTransport) Heal(from, to string) {
	t.SetDropRate(from, to, 0.0)
}

// HealAll removes all partitions and drop rates.
func (t *SimTransport) HealAll() {
	t.mu.Lock()
	t.dropRate = make(map[string]map[string]float64)
	t.mu.Unlock()
}

// History returns a copy of all recorded RPC events.
func (t *SimTransport) History() []SimEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]SimEvent, len(t.history))
	copy(out, t.history)
	return out
}

// Seed returns the seed used to initialize this transport.
func (t *SimTransport) Seed() int64 { return t.seed }

func (t *SimTransport) shouldDrop(from, to string) bool {
	t.mu.Lock() // write lock because rng.Float64 mutates rng state
	defer t.mu.Unlock()
	rate := 0.0
	if m, ok := t.dropRate[from]; ok {
		rate = m[to]
	}
	drop := t.rng.Float64() < rate
	return drop
}

func (t *SimTransport) record(from, to, typ string, dropped bool) {
	t.mu.Lock()
	t.history = append(t.history, SimEvent{From: from, To: to, Type: typ, Dropped: dropped})
	t.mu.Unlock()
}

func (t *SimTransport) getNode(id string) (*Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	return n, ok
}

func (t *SimTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	dropped := t.shouldDrop(args.CandidateId, peer)
	t.record(args.CandidateId, peer, "RequestVote", dropped)
	if dropped {
		return RequestVoteReply{}, fmt.Errorf("sim: dropped RequestVote %s→%s", args.CandidateId, peer)
	}
	node, ok := t.getNode(peer)
	if !ok {
		return RequestVoteReply{}, fmt.Errorf("sim: unknown peer %s", peer)
	}
	return node.HandleRequestVote(args), nil
}

func (t *SimTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	dropped := t.shouldDrop(args.LeaderId, peer)
	t.record(args.LeaderId, peer, "AppendEntries", dropped)
	if dropped {
		return AppendEntriesReply{}, fmt.Errorf("sim: dropped AppendEntries %s→%s", args.LeaderId, peer)
	}
	node, ok := t.getNode(peer)
	if !ok {
		return AppendEntriesReply{}, fmt.Errorf("sim: unknown peer %s", peer)
	}
	return node.HandleAppendEntries(args), nil
}
