// Package replication implements leader-follower WAL streaming.
// See docs/ARCHITECTURE.md "Phase 4: Replication" for the design
// decisions: wire protocol, consistency model (async), and idempotency.
package replication

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"kvengine/internal/wal"
)

// Leader accepts incoming follower connections and streams the WAL to each
// one starting from whatever byte offset the follower reports. One goroutine
// per follower — same model as the server package, same reasoning applies.
//
// Consistency model: async. The leader fsyncs locally, then streams.
// Put/Delete return to the client before the follower has applied the record.
// This means a follower may lag; if the leader crashes, the follower may be
// behind. This is documented in ARCHITECTURE.md and is a deliberate choice
// for Phase 4 — synchronous replication (waiting for follower ack before
// returning to client) is the Phase 5/Raft territory.
type Leader struct {
	w *wal.WAL

	mu   sync.Mutex
	ln   net.Listener
	done chan struct{} // closed by Close() to stop all StreamFrom waiters
}

func NewLeader(w *wal.WAL) *Leader {
	return &Leader{w: w, done: make(chan struct{})}
}

func (l *Leader) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("replication leader: listen: %w", err)
	}
	return l.Serve(ln)
}

func (l *Leader) Serve(ln net.Listener) error {
	l.mu.Lock()
	l.ln = ln
	l.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go l.handleFollower(conn)
	}
}

func (l *Leader) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ln == nil {
		return nil
	}
	return l.ln.Addr()
}

func (l *Leader) Close() error {
	l.mu.Lock()
	ln := l.ln
	l.mu.Unlock()

	close(l.done) // signal all StreamFrom waiters to exit
	if ln != nil {
		return ln.Close()
	}
	return nil
}

// handleFollower runs the per-connection replication protocol:
//
//	follower → leader: 8-byte little-endian int64 (follower's current offset)
//	leader  → follower: raw WAL encoded bytes from that offset, indefinitely
//
// The raw bytes are exactly the same format as the on-disk WAL, so the
// follower can decode them with wal.ReadRecord — the same function used for
// crash recovery. One format, two uses: no separate replication encoding.
//
// If the follower sends an offset beyond the leader's current durable
// offset (shouldn't happen in normal operation, but defensive), StreamFrom
// blocks until new data catches up to that point, which is correct.
func (l *Leader) handleFollower(conn net.Conn) {
	defer conn.Close()

	// Read the follower's resume offset.
	var fromOffset int64
	if err := binary.Read(conn, binary.LittleEndian, &fromOffset); err != nil {
		return // follower closed before even sending its offset
	}
	if fromOffset < 0 {
		fromOffset = 0
	}

	// Stream raw WAL bytes from that offset. StreamFrom blocks at the
	// durable frontier until new data arrives or done is closed (shutdown).
	bw := bufio.NewWriter(conn)
	_ = l.w.StreamFrom(fromOffset, bw, l.done)
	// Error from StreamFrom means either the follower disconnected (write
	// failed) or leader is shutting down (done closed). Either way: close.
}
