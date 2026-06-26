# kvengine

A concurrent, crash-safe, Raft-replicated key-value store built from scratch in Go.

I built this to go deep on systems correctness rather than breadth. Every layer from raw disk I/O up to distributed consensus, and every correctness claim backed by a test that would actually catch it if it broke.

```
go test -race ./...   # all green
```

---

## What it does

- **Durable writes** - every acknowledged write is fsynced before returning. Kill the process mid-write and the data survives.
- **Concurrent reads/writes** - safe under arbitrary goroutine concurrency, proven by the race detector and a linearizability checker.
- **Network protocol** - a binary-safe TCP protocol with explicit partial-read handling, per-connection deadlines, and backpressure isolation.
- **Leader-follower replication** - WAL streamed to followers with crash-safe resume via persisted byte offset.
- **Raft consensus** - leader election, log replication, log compaction. Survives leader failures and network partitions.
- **LSM storage engine** - a MemTable + SSTable alternative to the WAL store, with L0 to L1 compaction that bounds file count.

---

## Layout

```
cmd/kvengine/              CLI: single-node and Raft cluster modes
internal/
  wal/                     Write-ahead log: append, fsync, crash-tolerant replay
  store/                   KV store backed by WAL
  server/                  TCP server: binary-safe protocol, partial reads, deadlines
  replication/             Leader-follower WAL streaming with byte-offset resume
  raft/                    Raft consensus: election, replication, snapshots, TCP transport
  lsm/                     LSM storage engine: MemTable + SSTables + compaction
  linearcheck/             Linearizability checker (Wing-Gong model checker)
test/fault_injection/      SIGKILL-based crash durability harness
scripts/                   flamegraph.sh, fuzz.sh
docs/                      ARCHITECTURE.md, BENCHMARKS.md, TESTING.md
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design decisions and the reasoning behind each one.

---

## Quick start

```bash
go build -o kvengine ./cmd/kvengine

# Single-node server
./kvengine data.wal serve :6380

# Talk to it
printf 'PUT hello 5\r\nworld\r\n' | nc -q1 127.0.0.1 6380   # +OK
printf 'GET hello\r\n'            | nc -q1 127.0.0.1 6380   # $5\r\nworld
printf 'DEL hello\r\n'            | nc -q1 127.0.0.1 6380   # +OK

# Kill the server, restart it, GET again. Value survives (WAL).
```

### 3-node Raft cluster

```bash
mkdir -p /tmp/kv/{n0,n1,n2}

# Terminal 1
./kvengine raft --id n0 --raft-addr :7000 --kv-addr :6380 \
  --peers "n1=:7001,n2=:7002" --data-dir /tmp/kv/n0

# Terminal 2
./kvengine raft --id n1 --raft-addr :7001 --kv-addr :6381 \
  --peers "n0=:7000,n2=:7002" --data-dir /tmp/kv/n1

# Terminal 3
./kvengine raft --id n2 --raft-addr :7002 --kv-addr :6382 \
  --peers "n0=:7000,n1=:7001" --data-dir /tmp/kv/n2

# Write to whichever node responds +OK (that's the leader)
# Read from any node
printf 'PUT key 5\r\nvalue\r\n' | nc -q1 127.0.0.1 6380
printf 'GET key\r\n'            | nc -q1 127.0.0.1 6381

# Kill the leader. The other two elect a new one in about 400ms.
# Restart the killed node and it catches up automatically.
```

---

## Tests

```bash
make test-race           # full suite with race detector
make fuzz-seeds          # run fuzz seed corpus (fast)
make fuzz FUZZ_SECS=60   # run all fuzz targets for 60s each
make bench               # store, server, and LSM benchmarks
make flamegraph          # CPU flamegraph under load -> profiles/cpu.svg
```

### What each test actually proves

| Package | Key tests | What they prove |
|---|---|---|
| `internal/wal` | `TestReplayTornTail`, `FuzzReadRecord` | Crash recovery handles torn writes correctly; decoder never panics on garbage input |
| `internal/store` | `TestRestartRecoversState`, `TestLinearizability` | WAL state survives restart; 30 concurrent goroutines produce a linearizable history |
| `test/fault_injection` | `TestFaultInjectionTornWrites` | SIGKILL at random points never loses an acknowledged write |
| `internal/server` | `TestPartialWrites`, `TestSlowClientDoesNotBlockOthers` | Byte-at-a-time parser works correctly; one frozen client cannot stall the server |
| `internal/replication` | `TestFollowerResumeAfterDisconnect` | Follower disconnects, misses writes, restarts, and catches up with no duplicates |
| `internal/raft` | `TestConvergenceAfterHeal`, `TestNoCommitWithoutQuorum` | Old leader log is overwritten after partition heals; no commit without quorum |
| `internal/raft` | `TestPersistenceAcrossRestart`, `TestTCPTransportCluster` | Term and log survive node restart; real TCP cluster elects and replicates correctly |
| `internal/raft` | `TestSimPacketLoss`, `TestSimHighDropRatePartition` | Raft retry logic works correctly under 20% and 90% packet drop rates |
| `internal/lsm` | `TestCompaction`, `TestTombstoneAcrossLevels` | Compaction merges levels correctly; deletes shadow older values across levels |
| `internal/linearcheck` | `TestConcurrentStoreLinearizable`, `TestCheckNotLinearizable` | 48 concurrent ops on a real store are linearizable; violations are detected |

---

## Benchmarks

Full write-up in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Operation | Latency | Bottleneck |
|---|---|---|
| WAL store Put | ~550 us | fsync, one disk round-trip |
| WAL store Get | ~51 ns | map lookup |
| TCP PUT+GET round-trip | ~630 us | +80 us over the fsync floor |
| LSM Put (in-memory) | ~250 ns | memtable write, no fsync |
| LSM Get (SSTable) | ~8 us | linear scan, no bloom filter yet |

The short version: fsync dominates writes by about 10,000x. I benchmarked map sharding before deciding not to add it. It would have shaved nanoseconds off an operation that costs microseconds.

---

## Protocol

Line-based, binary-safe over TCP:

```
PUT <key> <valueLen>\r\n<value bytes>\r\n  ->  +OK\r\n
GET <key>\r\n                              ->  $<len>\r\n<value>\r\n  or  $-1\r\n
DEL <key>\r\n                              ->  +OK\r\n
<unknown>                                  ->  -ERR <message>\r\n
```

Keys are whitespace-delimited text. Values are length-prefixed binary, so arbitrary byte content works fine. Partial reads are handled correctly at every layer.

---

## Known gaps

- **waitApplied in the CLI** polls the log index instead of watching the apply channel. Works correctly but is not the right long-term approach.
- **InstallSnapshot RPC** is not implemented. A follower that falls behind a compaction point cannot catch up. This only happens if a follower is down while the leader snapshots past its position.
- **LSM SSTable reads** are O(n) linear scans. A production LSM would add a sparse index and bloom filters to get this to sub-microsecond.
- **Raft membership changes** are not implemented.

---

## License

MIT
