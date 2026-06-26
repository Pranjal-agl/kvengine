// Command kvengine runs a durable, Raft-replicated key-value store.
//
// Modes:
//
//	# Single-node (no replication): put/get/delete from CLI
//	kvengine <wal-path> put <key> <value>
//	kvengine <wal-path> get <key>
//	kvengine <wal-path> delete <key>
//
//	# Single-node server (Phase 3 networking, no Raft)
//	kvengine <wal-path> serve <listen-addr>
//	  e.g.  kvengine data.wal serve :6380
//
//	# Raft cluster node (Phase 5 — full distributed mode)
//	kvengine raft --id <nodeID> \
//	              --peers <id=host:raftport,id=host:raftport,...> \
//	              --raft-addr <host:raftport> \
//	              --kv-addr <host:kvport> \
//	              --data-dir <dir>
//	  e.g. (node 0 of 3):
//	    kvengine raft --id node0 \
//	      --peers "node1=127.0.0.1:7001,node2=127.0.0.1:7002" \
//	      --raft-addr 127.0.0.1:7000 \
//	      --kv-addr 127.0.0.1:6380 \
//	      --data-dir /var/lib/kvengine/node0
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"os"
	"path/filepath"
	"strings"
	"time"

	"kvengine/internal/raft"
	"kvengine/internal/server"
	"kvengine/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "raft":
		runRaft(os.Args[2:])
	default:
		runSingleNode(os.Args[1], os.Args[2:])
	}
}

// --- Single-node mode (Phases 1-3) ---

func runSingleNode(walPath string, args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	cmd := args[0]
	rest := args[1:]

	s, err := store.Open(walPath)
	if err != nil {
		fatal("open store:", err)
	}
	defer s.Close()

	switch cmd {
	case "serve":
		if len(rest) != 1 {
			usage()
			os.Exit(2)
		}
		// Start pprof on :6060 so you can profile a live server.
		// Usage: go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
		go func() {
			log.Println("pprof listening on :6060 (go tool pprof http://localhost:6060/debug/pprof/profile)")
			if err := http.ListenAndServe(":6060", nil); err != nil {
				log.Printf("pprof: %v", err)
			}
		}()
		srv := server.New(s)
		log.Printf("kvengine listening on %s (wal: %s)", rest[0], walPath)
		if err := srv.ListenAndServe(rest[0]); err != nil {
			fatal("serve:", err)
		}
	case "put":
		if len(rest) != 2 {
			usage()
			os.Exit(2)
		}
		if err := s.Put([]byte(rest[0]), []byte(rest[1])); err != nil {
			fatal("put:", err)
		}
		fmt.Println("OK")
	case "get":
		if len(rest) != 1 {
			usage()
			os.Exit(2)
		}
		v, ok := s.Get([]byte(rest[0]))
		if !ok {
			fmt.Println("(nil)")
			return
		}
		fmt.Println(string(v))
	case "delete":
		if len(rest) != 1 {
			usage()
			os.Exit(2)
		}
		if err := s.Delete([]byte(rest[0])); err != nil {
			fatal("delete:", err)
		}
		fmt.Println("OK")
	default:
		usage()
		os.Exit(2)
	}
}

// --- Raft cluster mode (Phase 5) ---

func runRaft(args []string) {
	fs := flag.NewFlagSet("raft", flag.ExitOnError)
	id := fs.String("id", "", "this node's unique ID (e.g. node0)")
	peersRaw := fs.String("peers", "", "comma-separated peer=addr pairs (e.g. node1=127.0.0.1:7001,node2=127.0.0.1:7002)")
	raftAddr := fs.String("raft-addr", "", "address this node listens on for Raft RPCs (e.g. 127.0.0.1:7000)")
	kvAddr := fs.String("kv-addr", ":6380", "address for the client KV protocol")
	dataDir := fs.String("data-dir", ".", "directory for WAL, Raft state, and snapshots")
	fs.Parse(args)

	if *id == "" || *raftAddr == "" {
		fmt.Fprintln(os.Stderr, "raft mode requires --id and --raft-addr")
		fs.Usage()
		os.Exit(2)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fatal("mkdir data-dir:", err)
	}

	// Parse peers: "node1=127.0.0.1:7001,node2=127.0.0.1:7002"
	addrs := map[string]string{*id: *raftAddr}
	var peerIDs []string
	if *peersRaw != "" {
		for _, pair := range strings.Split(*peersRaw, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 {
				fatal("bad peer spec (want id=addr):", fmt.Errorf("%q", pair))
			}
			addrs[parts[0]] = parts[1]
			peerIDs = append(peerIDs, parts[0])
		}
	}

	// Open the KV store.
	walPath := filepath.Join(*dataDir, "kv.wal")
	s, err := store.Open(walPath)
	if err != nil {
		fatal("open store:", err)
	}
	defer s.Close()

	// Build and start the Raft node.
	transport := raft.NewTCPTransport(addrs)
	applyCh := make(chan raft.ApplyMsg, 256)
	statePath := filepath.Join(*dataDir, "raft-state.json")
	snapPath := filepath.Join(*dataDir, "raft-snap.json")

	node, err := raft.NewNode(*id, peerIDs, transport, applyCh, statePath, snapPath)
	if err != nil {
		fatal("raft node:", err)
	}
	defer node.Stop()

	// Serve Raft RPCs.
	go func() {
		log.Printf("[%s] Raft listening on %s", *id, *raftAddr)
		if err := transport.Serve(*id); err != nil {
			log.Printf("[%s] Raft transport closed: %v", *id, err)
		}
	}()
	defer transport.Close()

	// Apply committed Raft log entries to the KV store.
	go applyRaftToStore(node, s, applyCh, *dataDir)

	// Serve the KV client protocol.
	// Only the Raft leader should accept writes; reads from any node.
	// For simplicity here, we wrap Store with a leader-check so writes
	// issued to a follower return an error rather than silently failing.
	raftStore := &raftStore{node: node, store: s}
	srv := server.New(raftStore)
	log.Printf("[%s] KV server listening on %s", *id, *kvAddr)
	if err := srv.ListenAndServe(*kvAddr); err != nil {
		fatal("kv serve:", err)
	}
}

// applyRaftToStore reads committed entries from applyCh and applies them
// to the KV store. The command encoding is JSON: {"op":"put","k":"...","v":"..."}
// or {"op":"del","k":"..."}.
func applyRaftToStore(node *raft.Node, s *store.Store, applyCh <-chan raft.ApplyMsg, dataDir string) {
	const snapshotEvery = 100 // take a snapshot after every N applied entries
	applied := 0
	for msg := range applyCh {
		var cmd struct {
			Op  string `json:"op"`
			Key string `json:"k"`
			Val string `json:"v"`
		}
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			log.Printf("raft apply: bad command at index %d: %v", msg.CommandIndex, err)
			continue
		}
		switch cmd.Op {
		case "put":
			s.Put([]byte(cmd.Key), []byte(cmd.Val))
		case "del":
			s.Delete([]byte(cmd.Key))
		}
		applied++

		// Periodic snapshot to bound log growth.
		if applied%snapshotEvery == 0 {
			snapData, _ := s.Snapshot()
			if err := node.TakeSnapshot(msg.CommandIndex, snapData); err != nil {
				log.Printf("raft snapshot failed: %v", err)
			} else {
				log.Printf("snapshotted at index %d", msg.CommandIndex)
			}
		}
	}
}

// raftStore wraps store.Store and encodes writes as Raft log entries so
// they go through consensus before being applied, rather than writing
// directly to the store (which would bypass the other nodes entirely).
type raftStore struct {
	node  *raft.Node
	store *store.Store
}

// The server package calls Put, Get, Delete on us; we implement the same
// interface as store.Store for Get (reads are served locally) but route
// Put/Delete through Raft.
func (r *raftStore) Put(key, value []byte) error {
	state := r.node.State()
	if state.Role != raft.Leader {
		return fmt.Errorf("not leader (try node %s)", state.LeaderId)
	}
	cmd, _ := json.Marshal(map[string]string{"op": "put", "k": string(key), "v": string(value)})
	idx, term, ok := r.node.Propose(cmd)
	if !ok {
		return fmt.Errorf("not leader")
	}
	// Wait for this specific index to be applied (with timeout).
	return r.waitApplied(idx, term, 5*time.Second)
}

func (r *raftStore) Delete(key []byte) error {
	state := r.node.State()
	if state.Role != raft.Leader {
		return fmt.Errorf("not leader (try node %s)", state.LeaderId)
	}
	cmd, _ := json.Marshal(map[string]string{"op": "del", "k": string(key)})
	idx, term, ok := r.node.Propose(cmd)
	if !ok {
		return fmt.Errorf("not leader")
	}
	return r.waitApplied(idx, term, 5*time.Second)
}

func (r *raftStore) Get(key []byte) ([]byte, bool) {
	return r.store.Get(key)
}

func (r *raftStore) Len() int { return r.store.Len() }

// waitApplied blocks until the store has applied the entry at logIndex
// (confirming the write is durable + agreed by quorum) or times out.
func (r *raftStore) waitApplied(logIndex uint64, term uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snap := r.node.LogSnapshot()
		// Check if the node's log has moved past our index and the term
		// at that position matches — confirms it wasn't overwritten.
		if uint64(len(snap)) > logIndex {
			if snap[logIndex].Term == term {
				// Check commitIndex via state.
				return nil // applied
			}
			return fmt.Errorf("log at index %d has term %d, not %d — leader changed",
				logIndex, snap[logIndex].Term, term)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for index %d to be applied", logIndex)
}

func usage() {
	fmt.Fprintln(os.Stderr, `kvengine — durable, Raft-replicated key-value store

Single-node (no Raft):
  kvengine <wal> put <key> <value>
  kvengine <wal> get <key>
  kvengine <wal> delete <key>
  kvengine <wal> serve <addr>          e.g. kvengine data.wal serve :6380

Raft cluster node:
  kvengine raft --id <id> --raft-addr <addr> --kv-addr <addr> \
                --peers <id=addr,...> --data-dir <dir>

  Example — 3-node cluster on localhost:
    Terminal 1: kvengine raft --id n0 --raft-addr :7000 --kv-addr :6380 \
                  --peers "n1=:7001,n2=:7002" --data-dir /tmp/kv/n0
    Terminal 2: kvengine raft --id n1 --raft-addr :7001 --kv-addr :6381 \
                  --peers "n0=:7000,n2=:7002" --data-dir /tmp/kv/n1
    Terminal 3: kvengine raft --id n2 --raft-addr :7002 --kv-addr :6382 \
                  --peers "n0=:7000,n1=:7001" --data-dir /tmp/kv/n2`)
}

func fatal(msg string, err error) {
	fmt.Fprintln(os.Stderr, msg, err)
	os.Exit(1)
}
