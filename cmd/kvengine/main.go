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
//	              --data-dir <dir> [--snapshot-every N] [--stale-reads] [--join]
//	              [--catch-up-timeout 2m]
//
//	# Grow or shrink a running cluster (send to any node; it must reach the leader):
//	#   start the new node with --join, then on the leader:
//	ADDNODE <id> <raftAddr>      REMOVENODE <id>      MEMBERS
//	  e.g. (node 0 of 3):
//	    kvengine raft --id node0 \
//	      --peers "node1=127.0.0.1:7001,node2=127.0.0.1:7002" \
//	      --raft-addr 127.0.0.1:7000 \
//	      --kv-addr 127.0.0.1:6380 \
//	      --data-dir /var/lib/kvengine/node0
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"kvengine/internal/raft"
	"kvengine/internal/raftkv"
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
		// pprof for profiling a live server (scripts/flamegraph.sh uses it):
		//   go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
		// Bound to loopback by default — pprof exposes internals and can be
		// used to burn CPU, so it shouldn't be reachable from the network.
		// Override with KVENGINE_PPROF=<addr>, or disable with KVENGINE_PPROF=off.
		if addr := pprofAddr(); addr != "" {
			go func() {
				log.Printf("pprof listening on %s", addr)
				if err := http.ListenAndServe(addr, nil); err != nil {
					log.Printf("pprof: %v", err)
				}
			}()
		}
		srv := server.New(s)
		shutdownOnSignal(func() {
			srv.Close()
			s.Close() // waits for in-flight writes to commit
		})
		log.Printf("kvengine listening on %s (wal: %s)", rest[0], walPath)
		if err := srv.ListenAndServe(rest[0]); err != nil && !errors.Is(err, net.ErrClosed) {
			fatal("serve:", err)
		}
		waitForShutdown()
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
	dataDir := fs.String("data-dir", ".", "directory for Raft state and snapshots")
	snapEvery := fs.Int("snapshot-every", 1000, "take a Raft snapshot after this many applied writes (0 = never)")
	staleReads := fs.Bool("stale-reads", false, "serve GETs from local state without a read index (faster, may be stale)")
	join := fs.Bool("join", false, "start empty and wait to be added to an existing cluster (ADDNODE on its leader); --peers is not needed")
	catchUp := fs.Duration("catch-up-timeout", raft.DefaultCatchUpTimeout, "how long ADDNODE replicates to a new node, as a non-voter, before giving up on it")
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
	if *peersRaw != "" {
		for _, pair := range strings.Split(*peersRaw, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				fatal("bad peer spec (want id=addr):", fmt.Errorf("%q", pair))
			}
			if parts[0] == *id {
				fatal("bad peer spec:", fmt.Errorf("--peers must not include this node (%s)", *id))
			}
			addrs[parts[0]] = parts[1]
		}
	}

	// Build the Raft node. The Raft log and snapshots are the durable
	// source of truth; the key-value state is rebuilt from them on start.
	transport := raft.NewTCPTransport(addrs)
	applyCh := make(chan raft.ApplyMsg, 256)
	// Bootstrap membership: this node plus --peers. Only used until the node
	// has a snapshot or a membership entry in its log; a joining node
	// starts with none and learns it from the leader.
	members := raft.Membership{}
	if !*join {
		for nid, addr := range addrs {
			members[nid] = addr
		}
	}
	node, err := raft.NewNodeWithMembers(*id, members, transport, applyCh,
		filepath.Join(*dataDir, "raft-state.json"),
		filepath.Join(*dataDir, "raft-snap.json"))
	if err != nil {
		fatal("raft node:", err)
	}
	kv, err := raftkv.New(node, applyCh, raftkv.Options{SnapshotEvery: *snapEvery, StaleReads: *staleReads, CatchUpTimeout: *catchUp})
	if err != nil {
		fatal("raft kv:", err)
	}

	go func() {
		log.Printf("[%s] Raft listening on %s", *id, *raftAddr)
		if err := transport.Serve(*id); err != nil {
			log.Printf("[%s] Raft transport closed: %v", *id, err)
		}
	}()

	srv := server.New(kv)
	shutdownOnSignal(func() {
		srv.Close()
		transport.Close()
		node.Stop()
		kv.Close()
	})
	log.Printf("[%s] KV server listening on %s", *id, *kvAddr)
	if err := srv.ListenAndServe(*kvAddr); err != nil && !errors.Is(err, net.ErrClosed) {
		fatal("kv serve:", err)
	}
	waitForShutdown()
}

// shutdownOnSignal runs cleanup on SIGINT/SIGTERM, then exits.
//
// Cleanup closes the server, which makes ListenAndServe return in main.
// main must then call waitForShutdown rather than returning: returning from
// main exits the process immediately, cutting cleanup off half-way (e.g.
// in the middle of writing a snapshot).
func shutdownOnSignal(cleanup func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("received %v, shutting down", sig)
		cleanup()
		log.Printf("shutdown complete")
		os.Exit(0)
	}()
}

// waitForShutdown blocks until the signal handler's cleanup exits the
// process.
func waitForShutdown() { select {} }

func usage() {
	fmt.Fprintln(os.Stderr, `kvengine — durable, Raft-replicated key-value store

Single-node (no Raft):
  kvengine <wal> put <key> <value>
  kvengine <wal> get <key>
  kvengine <wal> delete <key>
  kvengine <wal> serve <addr>          e.g. kvengine data.wal serve :6380

Raft cluster node:
  kvengine raft --id <id> --raft-addr <addr> --kv-addr <addr> \
                --peers <id=addr,...> --data-dir <dir> [--snapshot-every N] [--stale-reads]

  GETs are linearizable on every node by default (ReadIndex);
  --stale-reads serves them from local state instead.

  Membership changes (one at a time, sent to the leader's KV port):
    start the new node with --join (no --peers needed), then
    ADDNODE <id> <raftAddr> | REMOVENODE <id> | MEMBERS
    ADDNODE first catches the new node up as a non-voter, and fails
    (leaving membership unchanged) if that takes over --catch-up-timeout.

  Example — 3-node cluster on localhost:
    Terminal 1: kvengine raft --id n0 --raft-addr :7000 --kv-addr :6380 \
                  --peers "n1=:7001,n2=:7002" --data-dir /tmp/kv/n0
    Terminal 2: kvengine raft --id n1 --raft-addr :7001 --kv-addr :6381 \
                  --peers "n0=:7000,n2=:7002" --data-dir /tmp/kv/n1
    Terminal 3: kvengine raft --id n2 --raft-addr :7002 --kv-addr :6382 \
                  --peers "n0=:7000,n1=:7001" --data-dir /tmp/kv/n2`)
}

func pprofAddr() string {
	switch v := os.Getenv("KVENGINE_PPROF"); v {
	case "":
		return "127.0.0.1:6060"
	case "off":
		return ""
	default:
		return v
	}
}

func fatal(msg string, err error) {
	fmt.Fprintln(os.Stderr, msg, err)
	os.Exit(1)
}
