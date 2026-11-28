package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Binary wire encoding for Raft RPCs (TCPTransport).
//
// JSON was a quarter of the CPU on the cluster write path: decoding
// AppendEntries with base64'd commands via reflection. This codec is
// hand-written: integers are uvarints, strings and byte slices are
// length-prefixed, and the decoder bounds-checks every length against the
// remaining input, so a corrupt or hostile message produces an error,
// never a panic or a huge allocation (FuzzDecodeRPC).
//
// Distinctions the state machine depends on are kept explicit: a nil
// Command (leader no-op) vs an empty one, and a nil Config vs an empty one.

var errShort = errors.New("raft codec: message truncated")

type encoder struct{ b []byte }

func (e *encoder) u64(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) bool(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}
func (e *encoder) bytes(v []byte) { e.u64(uint64(len(v))); e.b = append(e.b, v...) }
func (e *encoder) str(v string)   { e.u64(uint64(len(v))); e.b = append(e.b, v...) }

// optBytes encodes nil distinctly from empty.
func (e *encoder) optBytes(v []byte) {
	e.bool(v != nil)
	if v != nil {
		e.bytes(v)
	}
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

func (d *decoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail(errShort)
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bool() bool {
	if d.err != nil {
		return false
	}
	if len(d.b) < 1 {
		d.fail(errShort)
		return false
	}
	v := d.b[0]
	d.b = d.b[1:]
	if v > 1 {
		d.fail(fmt.Errorf("raft codec: bad bool %d", v))
	}
	return v == 1
}

// count reads a length and checks at least min*length bytes remain, so a
// forged count can't trigger an oversized allocation.
func (d *decoder) count(min uint64) int {
	n := d.u64()
	if d.err == nil && (min > 0 && n > uint64(len(d.b))/min || min == 0 && n > uint64(len(d.b))) {
		d.fail(errShort)
		return 0
	}
	return int(n)
}

func (d *decoder) bytes() []byte {
	n := d.count(1)
	if d.err != nil {
		return nil
	}
	v := make([]byte, n)
	copy(v, d.b[:n])
	d.b = d.b[n:]
	return v
}

func (d *decoder) str() string { return string(d.bytes()) }

func (d *decoder) optBytes() []byte {
	if !d.bool() {
		return nil
	}
	return d.bytes()
}

func (d *decoder) done() error {
	if d.err == nil && len(d.b) != 0 {
		d.fail(fmt.Errorf("raft codec: %d trailing bytes", len(d.b)))
	}
	return d.err
}

func encodeMembership(e *encoder, m Membership) {
	e.bool(m != nil)
	if m == nil {
		return
	}
	ids := m.IDs()
	e.u64(uint64(len(ids)))
	for _, id := range ids {
		e.str(id)
		e.str(m[id])
	}
}

func decodeMembershipBin(d *decoder) Membership {
	if !d.bool() {
		return nil
	}
	n := d.count(2)
	m := make(Membership, n)
	for i := 0; i < n && d.err == nil; i++ {
		id := d.str()
		m[id] = d.str()
	}
	return m
}

// encodeRPC encodes any RPC argument or reply type.
func encodeRPC(v any) ([]byte, error) {
	e := &encoder{}
	switch m := v.(type) {
	case RequestVoteArgs:
		e.u64(m.Term)
		e.str(m.CandidateId)
		e.u64(m.LastLogIndex)
		e.u64(m.LastLogTerm)
	case RequestVoteReply:
		e.u64(m.Term)
		e.bool(m.VoteGranted)
	case AppendEntriesArgs:
		e.u64(m.Term)
		e.str(m.LeaderId)
		e.u64(m.PrevLogIndex)
		e.u64(m.PrevLogTerm)
		e.u64(m.LeaderCommit)
		e.u64(uint64(len(m.Entries)))
		for _, en := range m.Entries {
			e.u64(en.Term)
			e.u64(uint64(en.Kind))
			e.optBytes(en.Command)
		}
	case AppendEntriesReply:
		e.u64(m.Term)
		e.bool(m.Success)
		e.u64(m.ConflictIndex)
		e.u64(m.ConflictTerm)
	case InstallSnapshotArgs:
		e.u64(m.Term)
		e.str(m.LeaderId)
		e.u64(m.LastIncludedIndex)
		e.u64(m.LastIncludedTerm)
		e.optBytes(m.Data)
		encodeMembership(e, m.Config)
	case InstallSnapshotReply:
		e.u64(m.Term)
	case snapshotChunkArgs:
		e.u64(m.Term)
		e.str(m.LeaderId)
		e.u64(m.LastIncludedIndex)
		e.u64(m.LastIncludedTerm)
		e.optBytes(m.Data)
		encodeMembership(e, m.Config)
		e.u64(m.Offset)
		e.bool(m.More)
	case snapshotChunkReply:
		e.u64(m.Term)
		e.u64(m.Next)
		e.bool(m.Done)
	case TimeoutNowArgs:
		e.u64(m.Term)
		e.str(m.LeaderId)
	case TimeoutNowReply:
		e.u64(m.Term)
	case ReadIndexArgs:
		e.str(m.From)
	case ReadIndexReply:
		e.u64(m.Index)
		e.bool(m.OK)
		e.str(m.LeaderId)
	default:
		return nil, fmt.Errorf("raft codec: cannot encode %T", v)
	}
	return e.b, nil
}

// decodeRPC decodes b into the value pointed to by v.
func decodeRPC(b []byte, v any) error {
	d := &decoder{b: b}
	switch m := v.(type) {
	case *RequestVoteArgs:
		m.Term = d.u64()
		m.CandidateId = d.str()
		m.LastLogIndex = d.u64()
		m.LastLogTerm = d.u64()
	case *RequestVoteReply:
		m.Term = d.u64()
		m.VoteGranted = d.bool()
	case *AppendEntriesArgs:
		m.Term = d.u64()
		m.LeaderId = d.str()
		m.PrevLogIndex = d.u64()
		m.PrevLogTerm = d.u64()
		m.LeaderCommit = d.u64()
		n := d.count(3) // each entry is at least 3 bytes
		if n > 0 {
			m.Entries = make([]LogEntry, 0, n)
		}
		for i := 0; i < n && d.err == nil; i++ {
			var en LogEntry
			en.Term = d.u64()
			kind := d.u64()
			if kind > uint64(EntryConfig) {
				d.fail(fmt.Errorf("raft codec: bad entry kind %d", kind))
			}
			en.Kind = EntryKind(kind)
			en.Command = d.optBytes()
			m.Entries = append(m.Entries, en)
		}
	case *AppendEntriesReply:
		m.Term = d.u64()
		m.Success = d.bool()
		m.ConflictIndex = d.u64()
		m.ConflictTerm = d.u64()
	case *InstallSnapshotArgs:
		m.Term = d.u64()
		m.LeaderId = d.str()
		m.LastIncludedIndex = d.u64()
		m.LastIncludedTerm = d.u64()
		m.Data = d.optBytes()
		m.Config = decodeMembershipBin(d)
	case *InstallSnapshotReply:
		m.Term = d.u64()
	case *snapshotChunkArgs:
		m.Term = d.u64()
		m.LeaderId = d.str()
		m.LastIncludedIndex = d.u64()
		m.LastIncludedTerm = d.u64()
		m.Data = d.optBytes()
		m.Config = decodeMembershipBin(d)
		m.Offset = d.u64()
		m.More = d.bool()
	case *snapshotChunkReply:
		m.Term = d.u64()
		m.Next = d.u64()
		m.Done = d.bool()
	case *TimeoutNowArgs:
		m.Term = d.u64()
		m.LeaderId = d.str()
	case *TimeoutNowReply:
		m.Term = d.u64()
	case *ReadIndexArgs:
		m.From = d.str()
	case *ReadIndexReply:
		m.Index = d.u64()
		m.OK = d.bool()
		m.LeaderId = d.str()
	default:
		return fmt.Errorf("raft codec: cannot decode into %T", v)
	}
	return d.done()
}
