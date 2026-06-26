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
	rpcRequestVote   rpcType = 1
	rpcAppendEntries rpcType = 2
	dialTimeout              = 3 * time.Second
	rpcTimeout               = 5 * time.Second
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

	switch env.Type {
	case rpcRequestVote:
		var args RequestVoteArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply := node.HandleRequestVote(args)
		data, _ := json.Marshal(reply)
		writeEnvelope(w, rpcEnvelope{Type: rpcRequestVote, Payload: data})
		w.Flush()

	case rpcAppendEntries:
		var args AppendEntriesArgs
		if err := json.Unmarshal(env.Payload, &args); err != nil {
			return
		}
		reply := node.HandleAppendEntries(args)
		data, _ := json.Marshal(reply)
		writeEnvelope(w, rpcEnvelope{Type: rpcAppendEntries, Payload: data})
		w.Flush()
	}
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

func (t *TCPTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	conn, err := t.dial(peer)
	if err != nil {
		return RequestVoteReply{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(rpcTimeout))

	data, _ := json.Marshal(args)
	bw := bufio.NewWriter(conn)
	if err := writeEnvelope(bw, rpcEnvelope{Type: rpcRequestVote, Payload: data}); err != nil {
		return RequestVoteReply{}, err
	}
	if err := bw.Flush(); err != nil {
		return RequestVoteReply{}, err
	}
	env, err := readEnvelope(bufio.NewReader(conn))
	if err != nil {
		return RequestVoteReply{}, err
	}
	var reply RequestVoteReply
	if err := json.Unmarshal(env.Payload, &reply); err != nil {
		return RequestVoteReply{}, err
	}
	return reply, nil
}

func (t *TCPTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	conn, err := t.dial(peer)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(rpcTimeout))

	data, _ := json.Marshal(args)
	bw := bufio.NewWriter(conn)
	if err := writeEnvelope(bw, rpcEnvelope{Type: rpcAppendEntries, Payload: data}); err != nil {
		return AppendEntriesReply{}, err
	}
	if err := bw.Flush(); err != nil {
		return AppendEntriesReply{}, err
	}
	env, err := readEnvelope(bufio.NewReader(conn))
	if err != nil {
		return AppendEntriesReply{}, err
	}
	var reply AppendEntriesReply
	if err := json.Unmarshal(env.Payload, &reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}
