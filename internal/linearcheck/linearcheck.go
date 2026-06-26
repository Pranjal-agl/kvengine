// Package linearcheck implements a linearizability checker for the kvengine
// KV store. It records concurrent operations (with start/end timestamps) and
// verifies they can be arranged into a legal sequential order consistent
// with real time — the formal definition of linearizability.
//
// Algorithm: Wing & Gong (1993), adapted for a KV store state machine.
// Exponential worst-case but fast in practice for small histories (≤ ~20 ops)
// because the partial order from timestamps prunes the search space heavily.
package linearcheck

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// OpType is the kind of KV operation.
type OpType uint8

const (
	OpPut OpType = iota
	OpGet
	OpDelete
)

func (o OpType) String() string {
	switch o {
	case OpPut:
		return "Put"
	case OpGet:
		return "Get"
	default:
		return "Del"
	}
}

// Op is one completed client operation recorded for linearizability checking.
type Op struct {
	ID    int64 // unique per operation, for debugging
	Type  OpType
	Key   string
	Value string // for Put: value written; for Get: value returned ("" = not found)
	Found bool   // for Get: whether the key existed
	Start time.Time
	End   time.Time
}

// Recorder is safe for concurrent use from many goroutines — clients call
// Begin before the operation and End after it returns.
type Recorder struct {
	mu  sync.Mutex
	ops []Op
	seq int64 // atomic op ID counter
}

// Begin marks the start of an operation; returns an ID to pass to End.
func (r *Recorder) Begin() (id int64, start time.Time) {
	id = atomic.AddInt64(&r.seq, 1)
	return id, time.Now()
}

// RecordPut records a completed Put. Call after Put returns.
func (r *Recorder) RecordPut(id int64, start time.Time, key, value string) {
	r.mu.Lock()
	r.ops = append(r.ops, Op{ID: id, Type: OpPut, Key: key, Value: value, Start: start, End: time.Now()})
	r.mu.Unlock()
}

// RecordGet records a completed Get. Call after Get returns.
func (r *Recorder) RecordGet(id int64, start time.Time, key, value string, found bool) {
	r.mu.Lock()
	r.ops = append(r.ops, Op{ID: id, Type: OpGet, Key: key, Value: value, Found: found, Start: start, End: time.Now()})
	r.mu.Unlock()
}

// RecordDelete records a completed Delete.
func (r *Recorder) RecordDelete(id int64, start time.Time, key string) {
	r.mu.Lock()
	r.ops = append(r.ops, Op{ID: id, Type: OpDelete, Key: key, Start: start, End: time.Now()})
	r.mu.Unlock()
}

// Ops returns a snapshot of recorded operations.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// kvState is the reference sequential state machine: a simple map.
type kvState struct {
	data map[string]string
}

func newKVState() *kvState {
	return &kvState{data: make(map[string]string)}
}

func (s *kvState) clone() *kvState {
	c := &kvState{data: make(map[string]string, len(s.data))}
	for k, v := range s.data {
		c.data[k] = v
	}
	return c
}

// apply attempts to apply op to state and checks whether the result matches
// what was actually observed. Returns (new state, ok).
func (s *kvState) apply(op Op) (*kvState, bool) {
	next := s.clone()
	switch op.Type {
	case OpPut:
		next.data[op.Key] = op.Value
		return next, true // Put always succeeds
	case OpDelete:
		delete(next.data, op.Key)
		return next, true // Delete is idempotent, always succeeds
	case OpGet:
		v, found := next.data[op.Key]
		if found != op.Found {
			return nil, false
		}
		if found && v != op.Value {
			return nil, false
		}
		return next, true
	}
	return nil, false
}

// Check verifies that ops is a linearizable history for a KV store.
// Returns nil if linearizable, or an error describing the violation.
//
// Uses recursive backtracking: at each step, try every op whose Start is
// before any other op's End (i.e. it could have "happened first" in a
// valid sequential order). If applying it matches the observed result,
// recurse with it removed. If we can empty the op list, the history is
// linearizable.
//
// Limit: this is O(n!) worst case. For histories up to ~16 ops it's fast;
// for larger histories use the -short flag to skip linearizability tests.
func Check(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	if err := check(newKVState(), ops); err != nil {
		return fmt.Errorf("not linearizable: %w", err)
	}
	return nil
}

func check(state *kvState, remaining []Op) error {
	if len(remaining) == 0 {
		return nil
	}

	// Find the earliest End time — no op can be sequenced after this
	// endpoint without violating real-time order.
	earliest := remaining[0].End
	for _, op := range remaining[1:] {
		if op.End.Before(earliest) {
			earliest = op.End
		}
	}

	// Try sequencing each op whose Start is before earliest End.
	// These are the ops that "could" have completed first.
	for i, op := range remaining {
		if op.Start.After(earliest) {
			continue // this op started after the earliest completion — can't be first
		}
		nextState, ok := state.apply(op)
		if !ok {
			continue // applying this op here doesn't match the observed result
		}
		// Remove op[i] from remaining and recurse.
		rest := make([]Op, 0, len(remaining)-1)
		rest = append(rest, remaining[:i]...)
		rest = append(rest, remaining[i+1:]...)
		if err := check(nextState, rest); err == nil {
			return nil // found a valid linearization
		}
	}
	return fmt.Errorf("no valid linearization found for %d remaining ops", len(remaining))
}
