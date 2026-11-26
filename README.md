# kvengine

[![CI](https://github.com/Pranjal-agl/kvengine/actions/workflows/ci.yml/badge.svg)](https://github.com/Pranjal-agl/kvengine/actions/workflows/ci.yml)

A concurrent, crash-safe, Raft-replicated key-value store built from scratch in Go.

I built this to go deep on systems correctness rather than breadth. Every layer from raw disk I/O up to distributed consensus, and every correctness claim backed by a test that would actually catch it if it broke.

```
go test -race ./...   # all green
```

---

## What it does

- **Durable writes** - every acknowledged write is fsynced before returning, with group commit so concurrent writers share an fsync. Kill the process mid-write and the data survives, including across repeated crashes.
- **Concurrent reads/writes** - safe under arbitrary goroutine concurrency, proven by the race detector and a linearizability checker.
- **Network protocol** - a binary-safe TCP protocol with explicit partial-read handling, per-connection deadlines, and backpressure isolation.
- **Leader-follower replication** - WAL streamed to followers with crash-safe resume via persisted byte offset.
- **Raft consensus** - leader election, log replication, snapshots with InstallSnapshot for lagging followers. Writes are acknowledged only once committed by a quorum. Survives leader failures and network partitions.
- **LSM storage engine** - a MemTable + SSTable alternative to the WAL store, with sparse-indexed SSTables, crash-safe reload, and L0 to L1 compaction that bounds file count.

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
  raftkv/                  KV state machine on Raft: commit-before-ack writes, snapshot restore
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

# Writes to a follower return "-ERR not leader (leader is n1)".
# Kill the leader. The other two elect a new one in about 400ms.
# Restart the killed node and it catches up automatically, via
# InstallSnapshot if it fell behind the leader's last snapshot.
# Snapshot interval: --snapshot-every N (default 1000 writes).
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
| `internal/wal` | `TestOpenTruncatesTornTail` | A torn tail is cut off (and saved aside) before new records are appended |
| `internal/store` | `TestRestartRecoversState`, `TestWritesAfterTornTailSurviveSecondRestart` | WAL state survives restart, and writes acknowledged after a crash survive the *next* crash |
| `internal/store` | `TestLinearizability`, `TestConcurrentSameKeyMemoryMatchesLog` | Concurrent writes resolve to a real-time-consistent final state; memory before restart equals WAL replay after |
| `test/fault_injection` | `TestFaultInjectionTornWrites` | SIGKILL at random points never loses an acknowledged write |
| `internal/server` | `TestPartialWrites`, `TestSlowClientDoesNotBlockOthers` | Byte-at-a-time parser works correctly; one frozen client cannot stall the server |
| `internal/replication` | `TestFollowerResumeAfterDisconnect` | Follower disconnects, misses writes, restarts, and catches up with no duplicates |
| `internal/raft` | `TestConvergenceAfterHeal`, `TestNoCommitWithoutQuorum` | Old leader log is overwritten after partition heals; no commit without quorum |
| `internal/raft` | `TestPersistenceAcrossRestart`, `TestTCPTransportCluster` | Term and log survive node restart; real TCP cluster elects and replicates correctly |
| `internal/raft` | `TestSimPacketLoss`, `TestSimHighDropRatePartition` | Raft retry logic works correctly under 20% and 90% packet drop rates |
| `internal/raft` | `TestVoteNotResetBySameTermHeartbeat` | A node never votes twice in one term (Election Safety) |
| `internal/raft` | `TestReplicationContinuesAfterSnapshot`, `TestLaggingFollowerCatchesUpViaInstallSnapshot` | Replication keeps working after compaction; a follower behind the snapshot catches up via InstallSnapshot |
| `internal/raftkv` | `TestIsolatedLeaderDoesNotAcknowledge`, `TestFullClusterRestartRecoversState` | A leader without a quorum never returns +OK; a full-cluster restart recovers all state |
| `internal/lsm` | `TestCompaction`, `TestTombstoneInSSTableShadowsOlderSSTable` | Compaction merges levels correctly; deletes shadow older values in every layer |
| `internal/lsm` | `TestReopenAfterInterruptedCompaction` | A crash mid-compaction can't resurrect deleted or overwritten data |
| `internal/linearcheck` | `TestConcurrentStoreLinearizable`, `TestCheckNotLinearizable` | 48 concurrent ops on a real store are linearizable; violations are detected |

---

## Benchmarks

Full write-up in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

Measured on a 2-vCPU cloud VM; fsync latency dominates, so absolute numbers vary a lot by disk.

| Operation | Latency | Bottleneck |
|---|---|---|
| WAL store Put, 1 writer | ~170 us | fsync, one disk round-trip |
| WAL store Put, 64 concurrent writers | ~7 us per write | group commit: one fsync per batch |
| WAL store Get | ~33 ns | map lookup |
| TCP PUT+GET round-trip | ~175 us | fsync |
| LSM Put (in-memory) | ~60 ns | memtable write, no fsync |
| LSM Get from a 100k-entry SSTable | ~3.4 us | sparse index + one short scan |

The short version: fsync dominates writes by about 5,000x. I benchmarked map sharding before deciding not to add it, since it would have shaved nanoseconds off an operation that costs microseconds. Group commit attacks the fsync itself: with 64 concurrent writers it cuts the per-write cost by ~25x.

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

- **Reads are not linearizable in Raft mode.** Gets are served from the local state machine, so a follower (or a just-deposed leader) can return stale data. Fixing this needs ReadIndex or leader leases.
- **Raft membership changes** are not implemented; the cluster is fixed at startup.
- **Raft persistence rewrites the whole log** as JSON on every change. Snapshots keep the log short, so this is bounded, but a real implementation would append.
- **LSM MemTable has no WAL**, so unflushed writes are lost on crash (the engine is durable at Flush/Close). It also has no bloom filters, so a miss still checks every L0 file's index.
- **Single-node WAL store never compacts its log.** It grows with every write until rewritten.

---

## License

MIT
