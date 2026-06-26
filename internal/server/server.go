// Package server implements the network protocol layer for kvengine: a
// simple, binary-safe, line-based protocol over TCP. See docs/ARCHITECTURE.md
// "Phase 3: Networking" for the full protocol spec and the reasoning behind
// each design choice (deadlines, framing, backpressure policy).
package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// defaultIdleTimeout bounds how long a connection may sit without
	// sending a complete command before the server gives up on it. Prevents
	// a slow/dead/malicious client from holding a goroutine (and its
	// resources) forever.
	defaultIdleTimeout = 30 * time.Second

	// maxLineSize bounds the size of a single command line. Without a
	// bound, a client that never sends '\n' would make readLine grow its
	// buffer without limit — an easy memory-exhaustion vector.
	maxLineSize = 64 * 1024

	// maxValueSize bounds a single PUT's value size for the same reason.
	maxValueSize = 16 * 1024 * 1024 // 16 MiB
)

// KVStore is the interface the server requires from its backing store.
// *store.Store satisfies it; so does the Raft-wrapped store in cmd/kvengine.
type KVStore interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, bool)
	Len() int
}

// Server serves the kvengine protocol over TCP, backed by a KVStore.
type Server struct {
	store KVStore

	mu       sync.Mutex // guards listener: written by Serve, read by Close/Addr from other goroutines
	listener net.Listener

	// IdleTimeout overrides defaultIdleTimeout; exported so tests can use a
	// short timeout instead of waiting 30s. Zero means "use the default."
	IdleTimeout time.Duration
}

func New(s KVStore) *Server {
	return &Server{store: s}
}

func (srv *Server) idleTimeout() time.Duration {
	if srv.IdleTimeout > 0 {
		return srv.IdleTimeout
	}
	return defaultIdleTimeout
}

// ListenAndServe binds addr and serves until the listener is closed (via
// Close) or Accept returns a fatal error. One goroutine per connection —
// see docs/ARCHITECTURE.md for why this is the right starting model and
// what would change it.
func (srv *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	return srv.Serve(ln)
}

// Serve runs the accept loop on an already-created listener (used directly
// by tests so they can bind to an OS-assigned port via "127.0.0.1:0" and
// read back the actual address before connecting).
func (srv *Server) Serve(ln net.Listener) error {
	srv.mu.Lock()
	srv.listener = ln
	srv.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go srv.handleConn(conn)
	}
}

func (srv *Server) Addr() net.Addr {
	srv.mu.Lock()
	ln := srv.listener
	srv.mu.Unlock()
	if ln == nil {
		return nil
	}
	return ln.Addr()
}

func (srv *Server) Close() error {
	srv.mu.Lock()
	ln := srv.listener
	srv.mu.Unlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

// handleConn owns one connection end to end. It never assumes a command (or
// even a single line) arrives in one TCP read — see readLine and the PUT
// value-reading path below, both of which use loop/ReadFull patterns that
// are correct regardless of how the OS/network fragments the bytes.
func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		// Re-armed every iteration: each new command gets a fresh idle
		// window, so a client that's actively (if slowly) sending data
		// doesn't get cut off, but one that goes silent does.
		if err := conn.SetReadDeadline(time.Now().Add(srv.idleTimeout())); err != nil {
			return
		}

		line, err := readLine(r, maxLineSize)
		if err != nil {
			return // EOF, timeout, or line-too-long: nothing safe to do but close
		}
		fields := strings.Fields(string(line))
		if len(fields) == 0 {
			continue // blank line: tolerate as a no-op (simple keep-alive ping)
		}
		cmd := strings.ToUpper(fields[0])

		var fatal bool
		switch cmd {
		case "PUT":
			fatal = srv.handlePut(conn, r, w, fields)
		case "GET":
			srv.handleGet(w, fields)
		case "DEL":
			srv.handleDel(w, fields)
		default:
			writeErr(w, fmt.Errorf("unknown command %q", cmd))
		}

		if err := w.Flush(); err != nil {
			return // write side broke (client gone) — nothing more to do
		}
		if fatal {
			return
		}
	}
}

// readLine reads up to maxSize bytes up to and including a '\n', stripping
// a trailing '\r' if present, and returns the line WITHOUT the terminator.
// Implemented byte-by-byte over a *bufio.Reader rather than via
// ReadString('\n') so the size bound is enforced incrementally — ReadString
// would happily grow its buffer forever against a client that never sends
// '\n'.
func readLine(r *bufio.Reader, maxSize int) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			break
		}
		line = append(line, b)
		if len(line) > maxSize {
			return nil, fmt.Errorf("server: line exceeds max size %d", maxSize)
		}
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

// handlePut implements: PUT <key> <valueLen>\r\n<valueLen bytes>\r\n
// Returns fatal=true if a framing error occurred that desyncs the byte
// stream (e.g. a bad length means we don't know how many bytes to skip to
// resync) — in that case the connection must be closed rather than
// continuing, since further reads would be misinterpreted.
func (srv *Server) handlePut(conn net.Conn, r *bufio.Reader, w *bufio.Writer, fields []string) (fatal bool) {
	if len(fields) != 3 {
		writeErr(w, fmt.Errorf("PUT requires <key> <valueLen>, got %d args", len(fields)-1))
		return false // safe to continue: no value bytes were promised on the wire
	}
	key := fields[1]
	n, err := strconv.Atoi(fields[2])
	if err != nil || n < 0 {
		writeErr(w, fmt.Errorf("invalid value length %q", fields[2]))
		return false
	}
	if n > maxValueSize {
		writeErr(w, fmt.Errorf("value length %d exceeds max %d", n, maxValueSize))
		return true // client is about to send n bytes we won't read: stream is unrecoverable, close
	}

	// Extend the deadline for the value transfer itself — a large value
	// from a slow client should get more time than the idle window alone
	// would allow, but still bounded (not unlimited).
	if err := conn.SetReadDeadline(time.Now().Add(srv.idleTimeout())); err != nil {
		return true
	}

	value := make([]byte, n)
	// io.ReadFull is the explicit "don't assume one Read() returns
	// everything" handling: it loops internally until exactly n bytes are
	// read, a short read/EOF occurs, or the deadline fires.
	if _, err := io.ReadFull(r, value); err != nil {
		return true // torn/short read: stream position is now unknown, close
	}

	// Consume the trailing \r\n terminator after the value.
	term := make([]byte, 2)
	if _, err := io.ReadFull(r, term); err != nil || term[0] != '\r' || term[1] != '\n' {
		writeErr(w, fmt.Errorf("malformed PUT terminator"))
		return true
	}

	if err := srv.store.Put([]byte(key), value); err != nil {
		writeErr(w, fmt.Errorf("put failed: %w", err))
		return false
	}
	fmt.Fprint(w, "+OK\r\n")
	return false
}

// handleGet implements: GET <key>\r\n -> "$<len>\r\n<value>\r\n" or "$-1\r\n"
func (srv *Server) handleGet(w *bufio.Writer, fields []string) {
	if len(fields) != 2 {
		writeErr(w, fmt.Errorf("GET requires exactly 1 arg, got %d", len(fields)-1))
		return
	}
	v, ok := srv.store.Get([]byte(fields[1]))
	if !ok {
		fmt.Fprint(w, "$-1\r\n")
		return
	}
	fmt.Fprintf(w, "$%d\r\n", len(v))
	w.Write(v)
	fmt.Fprint(w, "\r\n")
}

// handleDel implements: DEL <key>\r\n -> "+OK\r\n" (idempotent, no error if absent)
func (srv *Server) handleDel(w *bufio.Writer, fields []string) {
	if len(fields) != 2 {
		writeErr(w, fmt.Errorf("DEL requires exactly 1 arg, got %d", len(fields)-1))
		return
	}
	if err := srv.store.Delete([]byte(fields[1])); err != nil {
		writeErr(w, fmt.Errorf("delete failed: %w", err))
		return
	}
	fmt.Fprint(w, "+OK\r\n")
}

func writeErr(w *bufio.Writer, err error) {
	msg := strings.ReplaceAll(err.Error(), "\r\n", " ")
	fmt.Fprintf(w, "-ERR %s\r\n", msg)
}
