# kvengine

A concurrent, crash-safe, Raft-replicated key-value store built from scratch in Go.

Built as a systems engineering deep-dive covering every layer from raw disk I/O to distributed consensus — not as a demonstration that it can be done, but as a proof that it is done correctly. Every correctness claim is backed by a test that would catch it if it broke.

```
go test -race ./...   # all green
```

---

## What it does

- **Durable writes** — every acknowledged write is fsynced before returning. Kill the process mid-write; the data survives.
- **Concurrent reads/writes** — safe under arbitrary goroutine concurrency, proven by the race detector and a linearizability checker.
- **Network protocol** — a binary-safe TCP protocol with explicit partial-read handling, per-connection deadlines, and backpressure isolation.
- **Leader-follower replication** — WAL streamed to followers; crash-safe resume via persisted byte offset.
- **Raft consensus** — leader election, log replication, log compaction. Survives leader failures and network partitions.
- **LSM storage engine** — a MemTable + SSTable alternative to the WAL store, with L0→L1 compaction that bounds file count.

---

## Architecture

Five layers, each independently testable:

```
cmd/kvengine/              CLI: single-node and Raft cluster modes
internal/
  wal/                     Write-ahead log: append, fsync, crash-tolerant replay
  store/                   KV store backed by WAL: enforces write-ahead invariant
  server/                  TCP server: binary-safe protocol, partial reads, deadlines
  replication/             Leader-follower WAL streaming with byte-offset resume
  raft/                    Raft consensus: election, replication, snapshots, TCP transport
  lsm/                     LSM storage engine: MemTable + SSTables + compaction
  linearcheck/             Linearizability checker (Wing-Gong model checker)
test/fault_injection/      SIGKILL-based crash durability harness
scripts/                   flamegraph.sh, fuzz.sh
docs/                      ARCHITECTURE.md, BENCHMARKS.md, TESTING.md
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for design decisions and the reasoning behind each one.

---

## Quick start

```bash
# Build
go build -o kvengine ./cmd/kvengine

# Single-node server
./kvengine data.wal serve :6380

# Talk to it (binary-safe protocol over TCP)
printf 'PUT hello 5\r\nworld\r\n' | nc -q1 127.0.0.1 6380   # +OK
printf 'GET hello\r\n'            | nc -q1 127.0.0.1 6380   # $5\r\nworld
printf 'DEL hello\r\n'            | nc -q1 127.0.0.1 6380   # +OK

# Kill the server, restart it, GET again — value survives (WAL)
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

# Write to the leader (whichever responds +OK), read from any node
printf 'PUT key 5\r\nvalue\r\n' | nc -q1 127.0.0.1 6380
printf 'GET key\r\n'            | nc -q1 127.0.0.1 6381

# Kill the leader — remaining two elect a new one in ~400ms
# Restart the killed node — it catches up automatically
```

---

## Tests

```bash
make test-race        # full suite with race detector
make fuzz-seeds       # run fuzz seed corpus (fast, no mutation)
make fuzz FUZZ_SECS=60  # run all fuzz targets for 60s each
make bench            # store, server, and LSM benchmarks
make flamegraph       # CPU flamegraph of server under load → profiles/cpu.svg
```

### What each test proves

| Package | Key tests | What they prove |
|---|---|---|
| `internal/wal` | `TestReplayTornTail`, `FuzzReadRecord` | Crash recovery tolerates torn writes; decoder never panics on garbage |
| `internal/store` | `TestRestartRecoversState`, `TestLinearizability` | WAL survives restart; concurrent ops produce linearizable results |
| `test/fault_injection` | `TestFaultInjectionTornWrites` | SIGKILL at random points never loses an acknowledged write |
| `internal/server` | `TestPartialWrites`, `TestSlowClientDoesNotBlockOthers` | Byte-at-a-time parser works; one frozen client can't stall the server |
| `internal/replication` | `TestFollowerResumeAfterDisconnect` | Follower disconnects, misses writes, restarts, catches up with no duplicates |
| `internal/raft` | `TestConvergenceAfterHeal`, `TestNoCommitWithoutQuorum` | Old leader's divergent log is overwritten on heal; no quorum = no commit |
| `internal/raft` | `TestPersistenceAcrossRestart`, `TestTCPTransportCluster` | Term/log survive node restart; real TCP cluster elects and replicates |
| `internal/raft` | `TestSimPacketLoss`, `TestSimHighDropRatePartition` | Raft's retry logic works under 20% and 90% packet drop rates |
| `internal/lsm` | `TestCompaction`, `TestTombstoneAcrossLevels` | Compaction merges correctly; deletes shadow older values across levels |
| `internal/linearcheck` | `TestConcurrentStoreLinearizable`, `TestCheckNotLinearizable` | 48 concurrent ops on a real store are linearizable; violations are detected |

---

## Benchmarks (summary)

Full analysis in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

| Operation | Latency | Bottleneck |
|---|---|---|
| WAL store Put | ~550 µs | fsync — disk round-trip |
| WAL store Get | ~51 ns | map lookup |
| TCP PUT+GET round-trip | ~630 µs | +80 µs over fsync floor |
| LSM Put (in-memory) | ~250 ns | memtable write — no fsync |
| LSM Get (SSTable) | ~8 µs | linear scan — no bloom filter yet |

**Key finding:** fsync dominates writes by 10,000×. Map sharding was explicitly benchmarked and rejected — it would shave nanoseconds off a microsecond-scale operation.

---

## Protocol

Line-based, binary-safe over TCP:

```
PUT <key> <valueLen>\r\n<value bytes>\r\n  →  +OK\r\n
GET <key>\r\n                              →  $<len>\r\n<value>\r\n  or  $-1\r\n
DEL <key>\r\n                              →  +OK\r\n
<unknown>                                  →  -ERR <message>\r\n
```

Keys are whitespace-delimited text. Values are length-prefixed binary — safe for arbitrary byte content. Partial reads handled correctly at every layer.

---

## Known gaps

Documented honestly, not hidden:

- **Raft log persistence** is complete for `currentTerm` and `votedFor`, but `waitApplied` in the CLI polls the log index rather than watching the apply channel — works correctly but should be replaced with a proper apply-notification mechanism.
- **InstallSnapshot RPC** is not implemented — a follower that falls behind a compaction point cannot catch up. Occurs only if a follower is down while the leader snapshots past its position.
- **LSM reads from SSTable** are O(n) linear scans. A production implementation would add a sparse index and bloom filters to make this O(log n) and sub-microsecond.
- **Raft membership changes** (adding/removing nodes) are not implemented.

---

## License

MIT
