package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// rpcType distinguishes the two Raft RPC types on the wire.
type rpcType uint8

const (
	rpcRequestVote     rpcType = 1
	rpcAppendEntries   rpcType = 2
	rpcInstallSnapshot rpcType = 3
	rpcReadIndex       rpcType = 4

	// rpcBinary marks a payload in the binary codec (codec.go). Requests
	// are always sent binary; the server also still answers JSON requests
	// (types without this bit) from older versions, replying in kind, so
	// a cluster can be upgraded one node at a time.
	rpcBinary rpcType = 0x80

	dialTimeout = 3 * time.Second
	rpcTimeout  = 5 * time.Second
)

// rpcMsg is the envelope written on the wire:
//
//	[1]byte  rpcType
//	[4]byte  payload length (little-endian uint32)
//	[N]byte  JSON payload
type rpcEnvelope struct {
	Type    rpcType
	Payload []byte
}

func writeEnvelope(w io.Writer, env rpcEnvelope) error {
	hdr := make([]byte, 5)
	hdr[0] = byte(env.Type)
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(env.Payload)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(env.Payload)
	return err
}

func readEnvelope(r io.Reader) (rpcEnvelope, error) {
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return rpcEnvelope{}, err
	}
	length := binary.LittleEndian.Uint32(hdr[1:])
	if length > 32<<20 { // 32 MiB sanity cap
		return rpcEnvelope{}, fmt.Errorf("tcp transport: rpc payload too large: %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return rpcEnvelope{}, err
	}
	return rpcEnvelope{Type: rpcType(hdr[0]), Payload: payload}, nil
}

// TCPTransport implements Transport over real TCP connections.
//
// Connections are persistent: each carries one request/response at a time,
// and an idle pool of up to maxIdlePerPeer connections per peer is kept for
// reuse. Concurrent RPCs to one peer use separate connections, so a slow
// InstallSnapshot never delays a heartbeat. Dialing per RPC used to cost a
// TCP handshake (plus a TIME_WAIT socket) on every heartbeat.
//
// All Raft RPCs are idempotent, so if a pooled connection turns out to be
// dead (the peer restarted, or closed it as idle), the call is retried once
// on a freshly dialed connection.
//
// The server side (Serve) accepts connections and answers requests on each
// until the client closes it, it sits idle for serverIdleTimeout, or the
// transport is closed.
type TCPTransport struct {
	mu       sync.RWMutex
	node     *Node             // set by Register
	addrs    map[string]string // peer id → "host:port"
	listener net.Listener
	closed   bool

	poolMu sync.Mutex
	idle   map[string][]*clientConn // peer → idle outbound conns
	legacy map[string]bool          // peers that only speak JSON (older versions)
	active map[net.Conn]struct{}    // inbound conns, closed by Close

	accepted atomic.Int64 // inbound connections accepted (for tests/metrics)
}

const (
	maxIdlePerPeer    = 4
	serverIdleTimeout = 2 * time.Minute
)

type clientConn struct {
	net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

// NewTCPTransport creates a transport. addrs maps each peer's id to its
// listen address (e.g. {"node1": "127.0.0.1:7001", "node2": "127.0.0.1:7002"}).
// The local node's own address is included so it can bind the listener.
func NewTCPTransport(addrs map[string]string) *TCPTransport {
	return &TCPTransport{
		addrs:  addrs,
		idle:   make(map[string][]*clientConn),
		legacy: make(map[string]bool),
		active: make(map[net.Conn]struct{}),
	}
}

// SetPeerAddrs records addresses for cluster members, so the transport can
// reach servers added after it was created (membership changes). Empty
// addresses are ignored, and existing entries are never removed: a member
// that was removed may still need to be reached briefly (e.g. to learn of
// a newer term).
func (t *TCPTransport) SetPeerAddrs(m Membership) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, addr := range m {
		if addr != "" {
			t.addrs[id] = addr
		}
	}
}

func (t *TCPTransport) Register(id string, node *Node) {
	t.mu.Lock()
	t.node = node
	t.mu.Unlock()
}

// Serve binds the listener for id and starts accepting RPC connections.
// Blocks until the listener is closed; call Close() to stop.
func (t *TCPTransport) Serve(id string) error {
	t.mu.RLock()
	addr, ok := t.addrs[id]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("tcp transport: no address for %s", id)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp transport: listen %s: %w", addr, err)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		ln.Close()
		return net.ErrClosed
	}
	t.listener = ln
	t.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		t.poolMu.Lock()
		if t.isClosed() {
			t.poolMu.Unlock()
			conn.Close()
			return net.ErrClosed
		}
		t.active[conn] = struct{}{}
		t.poolMu.Unlock()
		t.accepted.Add(1)
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) isClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

// Close stops serving, closes every inbound and pooled outbound
// connection, and makes further RPCs fail.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	ln := t.listener
	t.mu.Unlock()

	t.poolMu.Lock()
	for c := range t.active {
		c.Close()
	}
	t.active = make(map[net.Conn]struct{})
	for peer, conns := range t.idle {
		for _, c := range conns {
			c.Close()
		}
		delete(t.idle, peer)
	}
	t.poolMu.Unlock()

	if ln != nil {
		return ln.Close()
	}
	return nil
}

// handleConn answers requests on one inbound connection, one at a time.
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		t.poolMu.Lock()
		delete(t.active, conn)
		t.poolMu.Unlock()
	}()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(serverIdleTimeout)); err != nil {
			return
		}
		env, err := readEnvelope(r)
		if err != nil {
			return
		}
		t.mu.RLock()
		node := t.node
		t.mu.RUnlock()
		if node == nil {
			return
		}
		reply, ok := dispatch(node, env)
		if !ok {
			return
		}
		var data []byte
		if env.Type&rpcBinary != 0 {
			data, err = encodeRPC(reply)
		} else {
			data, err = json.Marshal(reply)
		}
		if err != nil {
			return
		}
		if err := conn.SetWriteDeadline(time.Now().Add(rpcTimeout)); err != nil {
			return
		}
		if err := writeEnvelope(w, rpcEnvelope{Type: env.Type, Payload: data}); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// dispatch decodes one request and runs the matching handler. ok is false
// for an unknown type or undecodable payload; the connection is then
// closed, since the stream can no longer be trusted.
func dispatch(node *Node, env rpcEnvelope) (reply any, ok bool) {
	decode := func(v any) bool { return json.Unmarshal(env.Payload, v) == nil }
	if env.Type&rpcBinary != 0 {
		decode = func(v any) bool { return decodeRPC(env.Payload, v) == nil }
	}
	switch env.Type &^ rpcBinary {
	case rpcRequestVote:
		var args RequestVoteArgs
		if !decode(&args) {
			return nil, false
		}
		return node.HandleRequestVote(args), true
	case rpcAppendEntries:
		var args AppendEntriesArgs
		if !decode(&args) {
			return nil, false
		}
		return node.HandleAppendEntries(args), true
	case rpcInstallSnapshot:
		var args InstallSnapshotArgs
		if !decode(&args) {
			return nil, false
		}
		return node.HandleInstallSnapshot(args), true
	case rpcReadIndex:
		var args ReadIndexArgs
		if !decode(&args) {
			return nil, false
		}
		return node.HandleReadIndex(args), true
	}
	return nil, false
}

func (t *TCPTransport) dial(peer string) (*clientConn, error) {
	t.mu.RLock()
	addr, ok := t.addrs[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tcp transport: unknown peer %s", peer)
	}
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return &clientConn{Conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}, nil
}

// getConn returns a pooled connection to peer if one is idle (reused=true),
// or dials a new one.
func (t *TCPTransport) getConn(peer string) (c *clientConn, reused bool, err error) {
	if t.isClosed() {
		return nil, false, net.ErrClosed
	}
	t.poolMu.Lock()
	if conns := t.idle[peer]; len(conns) > 0 {
		c = conns[len(conns)-1]
		t.idle[peer] = conns[:len(conns)-1]
		t.poolMu.Unlock()
		return c, true, nil
	}
	t.poolMu.Unlock()
	c, err = t.dial(peer)
	return c, false, err
}

func (t *TCPTransport) putConn(peer string, c *clientConn) {
	t.poolMu.Lock()
	defer t.poolMu.Unlock()
	if t.isClosed() || len(t.idle[peer]) >= maxIdlePerPeer {
		c.Close()
		return
	}
	t.idle[peer] = append(t.idle[peer], c)
}

// call performs one request/response RPC, reusing a pooled connection
// when possible.
//
// Requests go out in the binary codec. A peer running an older version
// closes the connection on a type it doesn't know, without replying; if a
// fresh connection gets EOF straight back, the call is retried once in
// JSON and the peer is remembered as JSON-only. That memory is dropped
// whenever the peer can't be dialed (it may be restarting with a new
// version), so it gets re-probed.
func (t *TCPTransport) call(peer string, typ rpcType, args, reply any) error {
	if t.isLegacy(peer) {
		return t.callFormat(peer, typ, args, reply, false)
	}
	err := t.callFormat(peer, typ, args, reply, true)
	if errors.Is(err, errPeerHungUp) {
		if jerr := t.callFormat(peer, typ, args, reply, false); jerr == nil {
			t.setLegacy(peer, true)
			return nil
		}
	}
	return err
}

// errPeerHungUp: a freshly dialed connection was closed without a reply.
var errPeerHungUp = errors.New("tcp transport: peer closed connection without replying")

func (t *TCPTransport) isLegacy(peer string) bool {
	t.poolMu.Lock()
	defer t.poolMu.Unlock()
	return t.legacy[peer]
}

func (t *TCPTransport) setLegacy(peer string, v bool) {
	t.poolMu.Lock()
	defer t.poolMu.Unlock()
	if v {
		t.legacy[peer] = true
	} else {
		delete(t.legacy, peer)
	}
}

func (t *TCPTransport) callFormat(peer string, typ rpcType, args, reply any, binaryFmt bool) error {
	var data []byte
	var err error
	if binaryFmt {
		typ |= rpcBinary
		data, err = encodeRPC(args)
	} else {
		data, err = json.Marshal(args)
	}
	if err != nil {
		return err
	}
	c, reused, err := t.getConn(peer)
	if err != nil {
		t.setLegacy(peer, false) // unreachable: re-probe its format later
		return err
	}
	env, err := roundTrip(c, typ, data)
	if err != nil && reused {
		// The pooled connection may have gone stale. Raft RPCs are
		// idempotent, so retrying on a fresh connection is safe.
		c.Close()
		if c, err = t.dial(peer); err != nil {
			t.setLegacy(peer, false)
			return err
		}
		env, err = roundTrip(c, typ, data)
	}
	if err != nil {
		c.Close()
		if errors.Is(err, io.EOF) {
			return errPeerHungUp
		}
		return err
	}
	t.putConn(peer, c)
	if env.Type != typ {
		return fmt.Errorf("tcp transport: reply type %d for request type %d", env.Type, typ)
	}
	if binaryFmt {
		return decodeRPC(env.Payload, reply)
	}
	return json.Unmarshal(env.Payload, reply)
}

func roundTrip(c *clientConn, typ rpcType, payload []byte) (rpcEnvelope, error) {
	if err := c.SetDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return rpcEnvelope{}, err
	}
	if err := writeEnvelope(c.w, rpcEnvelope{Type: typ, Payload: payload}); err != nil {
		return rpcEnvelope{}, err
	}
	if err := c.w.Flush(); err != nil {
		return rpcEnvelope{}, err
	}
	return readEnvelope(c.r)
}

func (t *TCPTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.call(peer, rpcRequestVote, args, &reply)
	return reply, err
}

func (t *TCPTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.call(peer, rpcAppendEntries, args, &reply)
	return reply, err
}

func (t *TCPTransport) InstallSnapshot(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	err := t.call(peer, rpcInstallSnapshot, args, &reply)
	return reply, err
}

func (t *TCPTransport) ReadIndex(peer string, args ReadIndexArgs) (ReadIndexReply, error) {
	var reply ReadIndexReply
	err := t.call(peer, rpcReadIndex, args, &reply)
	return reply, err
}
