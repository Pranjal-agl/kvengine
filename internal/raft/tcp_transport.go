package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// rpcType distinguishes the two Raft RPC types on the wire.
type rpcType uint8

const (
	rpcRequestVote     rpcType = 1
	rpcAppendEntries   rpcType = 2
	rpcInstallSnapshot rpcType = 3
	rpcReadIndex       rpcType = 4

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
// Each RPC opens a connection to the peer, sends the request, reads the
// reply, and closes — simple and correct. A persistent-connection pool
// would improve throughput but is not needed here; the bottleneck is
// fsync latency, not connection setup.
//
// The server side (Serve) accepts incoming connections and dispatches
// them to the local node's handlers.
type TCPTransport struct {
	mu       sync.RWMutex
	node     *Node             // set by Register
	addrs    map[string]string // peer id → "host:port"
	listener net.Listener
}

// NewTCPTransport creates a transport. addrs maps each peer's id to its
// listen address (e.g. {"node1": "127.0.0.1:7001", "node2": "127.0.0.1:7002"}).
// The local node's own address is included so it can bind the listener.
func NewTCPTransport(addrs map[string]string) *TCPTransport {
	return &TCPTransport{addrs: addrs}
}

func (t *TCPTransport) Register(id string, node *Node) {
	t.mu.Lock()
	t.node = node
	t.mu.Unlock()
}

// Serve binds the listener for id and starts accepting RPC connections.
// Blocks until the listener is closed; call Close() to stop.
func (t *TCPTransport) Serve(id string) error {
	addr, ok := t.addrs[id]
	if !ok {
		return fmt.Errorf("tcp transport: no address for %s", id)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp transport: listen %s: %w", addr, err)
	}
	t.mu.Lock()
	t.listener = ln
	t.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) Close() error {
	t.mu.RLock()
	ln := t.listener
	t.mu.RUnlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

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

	var reply any
	switch env.Type {
	case rpcRequestVote:
		var args RequestVoteArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply = node.HandleRequestVote(args)
	case rpcAppendEntries:
		var args AppendEntriesArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply = node.HandleAppendEntries(args)
	case rpcInstallSnapshot:
		var args InstallSnapshotArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply = node.HandleInstallSnapshot(args)
	case rpcReadIndex:
		var args ReadIndexArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply = node.HandleReadIndex(args)
	default:
		return
	}
	data, err := json.Marshal(reply)
	if err != nil {
		return
	}
	if err := writeEnvelope(w, rpcEnvelope{Type: env.Type, Payload: data}); err != nil {
		return
	}
	w.Flush()
}

func (t *TCPTransport) dial(peer string) (net.Conn, error) {
	t.mu.RLock()
	addr, ok := t.addrs[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tcp transport: unknown peer %s", peer)
	}
	return net.DialTimeout("tcp", addr, dialTimeout)
}

// call performs one request/response RPC over a fresh connection.
func (t *TCPTransport) call(peer string, typ rpcType, args, reply any) error {
	conn, err := t.dial(peer)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(rpcTimeout)); err != nil {
		return err
	}
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(conn)
	if err := writeEnvelope(bw, rpcEnvelope{Type: typ, Payload: data}); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	env, err := readEnvelope(bufio.NewReader(conn))
	if err != nil {
		return err
	}
	if env.Type != typ {
		return fmt.Errorf("tcp transport: reply type %d for request type %d", env.Type, typ)
	}
	return json.Unmarshal(env.Payload, reply)
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
