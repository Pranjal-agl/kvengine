# kvengine

[![CI](https://github.com/Pranjal-agl/kvengine/actions/workflows/ci.yml/badge.svg)](https://github.com/Pranjal-agl/kvengine/actions/workflows/ci.yml)

A concurrent, crash-safe, Raft-replicated key-value store built from scratch in Go.

I built this to go deep on systems correctness rather than breadth. Every layer from raw disk I/O up to distributed consensus, and every correctness claim backed by a test that would actually catch it if it broke.

```
go test -race ./...   # all green
```

---

## What it does

- **Durable writes** - every acknowledged write is fsynced before returning, with group commit so concurrent writers share an fsync. Kill the process mid-write and the data survives, including across repeated crashes. The log is compacted automatically, so it doesn't grow with overwrites.
- **Concurrent reads/writes** - safe under arbitrary goroutine concurrency, proven by the race detector and a linearizability checker.
- **Network protocol** - a binary-safe TCP protocol with explicit partial-read handling, per-connection deadlines, and backpressure isolation.
- **Leader-follower replication** - WAL streamed to followers with crash-safe resume via persisted byte offset.
- **Raft consensus** - leader election, log replication, snapshots with InstallSnapshot for lagging followers, and an append-only on-disk log. Writes are acknowledged only once committed by a quorum. Survives leader failures and network partitions.
- **Membership changes** - add or remove nodes in a running cluster, one at a time (Raft thesis §4.1): `ADDNODE`, `REMOVENODE`, `MEMBERS`, and `--join` for new nodes. Removed nodes can't disrupt the cluster. Checked by a chaos test mixing membership changes, partitions, crashes and restarts.
- **Linearizable reads from any node** - GETs use ReadIndex (Raft thesis §6.4): the leader confirms it still holds a quorum before answering, and followers ask the leader for a read index and catch up before reading locally. A partitioned ex-leader refuses reads instead of serving stale data. Checked with a Wing-Gong linearizability checker over concurrent histories.
- **LSM storage engine** - a MemTable + SSTable alternative to the WAL store: write-ahead logged with group commit (survives SIGKILL mid-flush and mid-compaction), sparse indexes and bloom filters on SSTables, and leveled compaction (L0-L6 with a crash-safe manifest), so write amplification grows with the number of levels, not the data size.

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
  linearcheck/             Linearizability checker (Wing-Gong search, per-key + memoised)
test/fault_injection/      SIGKILL-based crash durability harness
scripts/                   flamegraph.sh, fuzz.sh
scripts/e2e/               End-to-end scenarios against real kvengine processes
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
# Read from any node: reads are linearizable everywhere (add --stale-reads
# to serve them from local state instead, faster but possibly stale)
printf 'PUT key 5\r\nvalue\r\n' | nc -q1 127.0.0.1 6380
printf 'GET key\r\n'            | nc -q1 127.0.0.1 6381

# Writes to a follower return "-ERR not leader (leader is n1)".
# Grow the cluster: start a new node empty, then add it via the leader
./kvengine raft --id n3 --raft-addr :7003 --kv-addr :6383 --data-dir /tmp/kv/n3 --join
printf 'ADDNODE n3 127.0.0.1:7003\r\n' | nc -q1 127.0.0.1 6380      # on the leader
printf 'MEMBERS\r\n'                    | nc -q1 127.0.0.1 6380
printf 'REMOVENODE n2\r\n'              | nc -q1 127.0.0.1 6380

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
make e2e                 # real processes: failover, membership, crashes, v1.0 migration
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
| `internal/raftkv` | `TestLinearizableHistory`, `TestDeposedLeaderCannotServeStaleRead` | Concurrent writes + reads from every node form a linearizable history; a partitioned ex-leader refuses reads |
| `internal/raft` | `TestMembershipChaos`, `TestEvenSplitHasNoQuorum` | Random adds/removes/partitions/crashes never produce two leaders in a term or divergent logs; a 2/2 split commits nothing |
| `internal/raftkv` | `TestFollowerCatchesUpAfterLargeBacklog`, `TestFollowerCatchesUpWithLargeSnapshot` | A follower 40 MB behind catches up via the log or a snapshot |
| `internal/lsm` | `TestLSMMatchesModel`, `TestLeveledCompactionInvariants` | Random ops, crashes and reopens agree with a map; levels stay sorted, disjoint and within budget |
| `test/fault_injection` | `TestStoreCompactionFaultInjection` | SIGKILL during WAL compaction (rewrite + rename) never loses an acknowledged write |
| `internal/raft` | `FuzzDecodeRPC`, `TestTCPTransportFallsBackToJSONForOldPeers` | The binary RPC codec never panics on garbage; new nodes interoperate with old JSON-only ones |
| `internal/lsm` | `TestCompaction`, `TestTombstoneInSSTableShadowsOlderSSTable` | Compaction merges levels correctly; deletes shadow older values in every layer |
| `internal/lsm` | `TestReopenAfterInterruptedCompaction` | A crash mid-compaction can't resurrect deleted or overwritten data |
| `test/fault_injection` | `TestLSMFaultInjection` | SIGKILL during constant flushes/compactions never loses an acknowledged LSM write |
| `internal/lsm` | `TestConcurrentSameKeyMemoryMatchesRecovery`, `TestBloomNoFalseNegativesAndLowFPRate` | LSM group commit keeps memory and WAL in the same order; bloom filters never miss a present key, ~0.8% false positives |
| `internal/raft` | `TestSnapshotNewerThanLogFile`, `TestTruncationSurvivesRestart` | The append-only Raft log recovers from a crash between snapshot and log rewrite; follower truncations are durable |
| `internal/linearcheck` | `TestConcurrentStoreLinearizable`, `TestCheckNotLinearizable` | 48 concurrent ops on a real store are linearizable; violations are detected |
| `scripts/e2e` | `failover`, `membership`, `crash_under_load`, `migrate` | Real processes: SIGKILLed leaders are replaced and caught up; nodes join and leave; SIGKILL of the whole cluster under load loses no acknowledged write; v1.0 data survives an upgrade |
| `internal/linearcheck` | `TestCheckScalesToLargeHistories` | The checker handles 3,000-op histories in well under a second (per-key decomposition + memoisation) |

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
| LSM Put (durable / `NoSync`) | ~160 us / ~0.5 us | WAL fsync / WAL write to the OS |
| LSM Put, 64 concurrent writers | ~7 us per write | group commit: one fsync per batch |
| LSM Get from a 100k-entry SSTable | ~3.4 us | sparse index + one short scan |
| LSM Get of an absent key (3 overlapping tables) | ~0.24 us | bloom filters skip every table |
| Raft PUT, 3 nodes over TCP (1 / 16 concurrent clients) | ~320 us / ~140 us per op | fsync on leader + follower |
| Raft GET on leader / follower (in process, TCP) | ~15 us / ~23 us | ReadIndex quorum round (+ hop to leader) |
| Raft GET with `--stale-reads` (p50, TCP) | ~40-70 us | local map lookup, no consistency guarantee |

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

In Raft mode, PUT/DEL on a follower return `-ERR not leader (leader is <id>)`, and a GET that can't confirm a quorum returns `-ERR` rather than possibly stale data. Raft mode also accepts `ADDNODE <id> <raftAddr>`, `REMOVENODE <id>` and `MEMBERS`.

A malformed PUT closes the connection: without a valid length the server can't tell where the value ends, and must not execute its bytes as commands.

Keys are whitespace-delimited text. Values are length-prefixed binary, so arbitrary byte content works fine. Partial reads are handled correctly at every layer.

---

## Known gaps and deliberate choices

- **No leader leases.** Linearizable reads confirm leadership with a quorum heartbeat round (~15 us on loopback; concurrent reads share one). Leases would skip that round trip, but they are only safe if clocks never drift more than the lease margin; the project chooses the check that doesn't depend on clocks.
- **Snapshots are whole and in memory.** The state machine is an in-memory map, and snapshots are built, stored and sent as one piece (up to 2 GiB on the wire), not streamed in chunks. Data must fit in RAM in Raft mode.
- **No leadership transfer**, and a membership change doesn't first catch up a new node as a non-voting learner. Adding a node that is far behind briefly makes commits wait for a majority that includes it.
- **LSM compaction runs inline** on the write that triggers it, so that write (and those queued behind it) waits for the compaction. Production engines compact in the background.
- **The WAL store can't compact while replicating** (legacy leader/follower mode streams the log by byte offset).
- **Upgrading from v1.0 must be done offline.** Stop every node, then start them all on the new binary: the data (v1.0's Raft log and snapshots) is read back. A rolling upgrade isn't supported, because v1.0 nodes misread log entries written by newer leaders (newer releases store keys and values as base64, which v1.0 would apply literally). v1.0 also has the safety bugs fixed since, so it shouldn't share a cluster with anything.
- **Keys can't contain whitespace** in the text protocol (values are binary-safe).
- **Crash tests kill processes, not machines.** SIGKILL tests cover process crashes; power loss also depends on the fsync and rename ordering being honored by the OS and disk, which the code follows but these tests can't simulate.

---

## License

MIT
