// Package linearcheck implements a linearizability checker for the kvengine
// KV store. It records concurrent operations (with start/end timestamps) and
// verifies they can be arranged into a legal sequential order consistent
// with real time — the formal definition of linearizability.
//
// Algorithm: Wing & Gong (1993) backtracking search, with the two
// optimisations that make it practical on real histories (both used by
// checkers such as Lowe's and Porcupine):
//
//   - Per-key decomposition. Linearizability is local (Herlihy & Wing):
//     a history over independent objects is linearizable iff each object's
//     sub-history is. Every key of a KV store is an independent register,
//     so each key is checked on its own.
//   - Memoisation. The search state is (set of ops already linearized,
//     register value). Two paths that reach the same state have the same
//     future, so a state that failed once is never explored again.
//
// Without these, a 60-op history could take many minutes; with them,
// thousands of heavily overlapping ops check in well under a second.
package linearcheck

import (
	"encoding/binary"
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

// Check verifies that ops is a linearizable history for a KV store.
// Returns nil if linearizable, or an error naming the first key whose
// sub-history has no valid linearization.
//
// An op may be linearized next only if it started no later than the
// earliest end among the ops not yet linearized; otherwise that op would
// have to take effect after some op that finished before it began.
func Check(ops []Op) error {
	byKey := make(map[string][]Op)
	var keys []string
	for _, op := range ops {
		if _, seen := byKey[op.Key]; !seen {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			return fmt.Errorf("not linearizable: no valid linearization for the %d ops on key %q",
				len(byKey[k]), k)
		}
	}
	return nil
}

// register is the sequential model for one key.
type register struct {
	present bool
	value   string
}

// step applies op to r. ok is false if op is a Get whose observed result
// doesn't match r.
func (r register) step(op Op) (next register, ok bool) {
	switch op.Type {
	case OpPut:
		return register{true, op.Value}, true
	case OpDelete:
		return register{}, true
	case OpGet:
		if op.Found != r.present || (op.Found && op.Value != r.value) {
			return r, false
		}
		return r, true
	}
	return r, false
}

func checkKey(ops []Op) bool {
	n := len(ops)
	done := make([]uint64, (n+63)/64)
	failed := make(map[string]struct{})

	var search func(r register, remaining int) bool
	search = func(r register, remaining int) bool {
		if remaining == 0 {
			return true
		}
		memo := stateKey(done, r)
		if _, bad := failed[memo]; bad {
			return false
		}
		var earliest time.Time
		first := true
		for i := 0; i < n; i++ {
			if !isSet(done, i) && (first || ops[i].End.Before(earliest)) {
				earliest, first = ops[i].End, false
			}
		}
		for i := 0; i < n; i++ {
			if isSet(done, i) || ops[i].Start.After(earliest) {
				continue
			}
			next, ok := r.step(ops[i])
			if !ok {
				continue
			}
			set(done, i)
			if search(next, remaining-1) {
				return true
			}
			unset(done, i)
		}
		failed[memo] = struct{}{}
		return false
	}
	return search(register{}, n)
}

func stateKey(done []uint64, r register) string {
	b := make([]byte, 0, len(done)*8+2+len(r.value))
	for _, w := range done {
		b = binary.LittleEndian.AppendUint64(b, w)
	}
	if r.present {
		b = append(b, 1)
		b = append(b, r.value...)
	} else {
		b = append(b, 0)
	}
	return string(b)
}

func isSet(bits []uint64, i int) bool { return bits[i/64]&(1<<(i%64)) != 0 }
func set(bits []uint64, i int)        { bits[i/64] |= 1 << (i % 64) }
func unset(bits []uint64, i int)      { bits[i/64] &^= 1 << (i % 64) }
