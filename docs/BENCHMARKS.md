# Benchmarks

Numbers below are from a 2-vCPU cloud VM. Before/after comparisons were run back to back on that same machine. fsync latency depends heavily on the disk (an earlier version of this doc measured ~550 us per fsync on different hardware), so compare ratios rather than absolute numbers.

```bash
go test -bench=. -benchtime=3s ./internal/store/...
go test -bench=. -benchtime=3s ./internal/server/...
go test -bench=. -benchtime=3s ./internal/lsm/...
```

---

## WAL-backed store

```
BenchmarkPutSequential              ~170 us/op
BenchmarkGetSequential              ~33 ns/op
BenchmarkPutParallel                ~170 us/op   (GOMAXPROCS=2 writers, different keys)
BenchmarkPutSameKeyParallel         ~177 us/op   (GOMAXPROCS=2 writers, one key)
```

A single writer pays one fsync per Put. Get costs about 33 ns, roughly 5,000x cheaper than Put. The RWMutex and map lookup are essentially free compared to disk I/O.

### Group commit

`BenchmarkPutConcurrentWriters` sweeps the number of concurrent writers. This is the number that matters for a server with many clients:

```
                     before (fsync per write)   after (group commit)
writers=1            ~181 us/op                 ~181 us/op
writers=8            ~163 us/op                 ~40 us/op
writers=64           ~147 us/op                 ~6.9 us/op
```

Before, every writer paid for its own fsync, so adding writers barely helped. Now, writers that arrive while an fsync is in flight are committed together by the next one. At 64 writers, that's ~21x the throughput. A single writer is unaffected because uncontended writes commit on the caller's goroutine with no handoff (see ARCHITECTURE.md).

### Why I did not shard the map

The obvious optimization for a concurrent KV store is sharding the map by key hash to reduce mutex contention. These numbers show that would be worthless. The lock costs ~50 ns. The fsync costs hundreds of microseconds. Sharding the map would improve the cheap part of the operation. Group commit attacks the expensive part.

---

## Network layer

```
BenchmarkPutGetOverTCP    ~175 us/op   (PUT+GET combined, one client)
```

A full PUT + GET over real TCP loopback costs about the same as a single WAL Put. The network layer (two round trips, request parsing, response formatting) is a small fraction of the total. The protocol is not the bottleneck.

---

## LSM engine

```
BenchmarkLSMPut               ~160 us/op   (WAL fsync per write)
BenchmarkLSMPutNoSync         ~0.5 us/op   (WAL write to the OS, no fsync)
BenchmarkLSMGet               ~30 ns/op    (MemTable lookup)
BenchmarkLSMGetAfterFlush     ~1.9 us/op   (1k-entry SSTable)
BenchmarkLSMGetLargeSSTable   ~3.4 us/op   (100k-entry SSTable)
BenchmarkLSMGetMiss           ~0.24 us/op  (absent key inside 3 overlapping tables)
```

Durable LSM Puts group-commit like the WAL store:

```
                     before (fsync per write)   after (group commit)
writers=1            ~151 us/op                 ~161 us/op
writers=8            ~160 us/op                 ~39 us/op
writers=64           ~184 us/op                 ~6.7 us/op
```

Bloom filters (10 bits/key, ~0.8% false positives) answer most misses without reading a table:

```
                              before          after
GetMiss (3 tables, in range)  ~8.1 us/op      ~0.24 us/op
```

SSTable reads use a sparse index (every 16th key) to seek near the key, then scan at most 16 entries. Before the index, reads were a linear scan of the whole file, opened fresh on every Get:

```
                              before          after
GetAfterFlush (1k entries)    ~29 us/op       ~1.9 us/op
GetLargeSSTable (100k)        ~2.6 ms/op      ~3.4 us/op
```

### WAL store vs LSM

| Property | WAL store | LSM engine |
|---|---|---|
| Write latency | ~170 us (fsync, shared under concurrency) | ~160 us (fsync, shared under concurrency); ~0.5 us with NoSync |
| Read latency, hot | ~33 ns (map lookup) | ~30 ns (memtable lookup) |
| Read latency, cold | ~33 ns (everything is in memory) | ~2-4 us (SSTable index + short scan) |
| Durability window | Per write | Per write (NoSync: survives process crash, not power loss) |
| Log growth | Forever without checkpointing | Bounded by compaction |
| Memory | All data | MemTable + sparse indexes (data can exceed RAM) |
| Complexity | Low | Medium |

An earlier version measured LSM Put at ~60 ns, but that was an in-memory write with no WAL: unflushed data was lost on crash. With a WAL, the honest comparison is fsync against fsync. The LSM's advantages are memory (data can exceed RAM) and bounded files, not raw write speed.

What's left compared with production LSMs (LevelDB, RocksDB): a block cache, and a multi-level compaction scheme. Here every compaction rewrites all of L1.

---

## Raft reads

End-to-end over the TCP transport: a 3-node cluster on one machine, one client, a write to the leader followed immediately by a read from a follower, 200 times.

```
                         linearizable (default)    --stale-reads
stale follower reads     0 / 200                   200 / 200
follower GET p50 / p99   ~230 / ~400 us            ~70 / ~170 us
leader GET p50 / p99     ~90 / ~280 us             ~40 / ~100 us
```

(Includes the Python test client's own overhead.) The stale column is the reason ReadIndex exists: a follower only learns the new commit index on the next heartbeat (every 50 ms), so a read right after a write is *always* stale there. A linearizable read pays one quorum heartbeat round trip, plus a hop to the leader when served by a follower.

## Raft cluster, in process

`internal/raftkv/bench_test.go` runs a 3-node cluster in one process over real loopback TCP with Raft state on disk, so the numbers include serialization, syscalls and fsync. Each row is a change made on this branch, measured back to back:

```
                                   Put        Put x16/op   Get(leader)  Get x16/op   Get(follower)
baseline                           2.06 ms    1.19 ms      69 us        72 us        121 us
+ read batching                       -          -            -         13.6 us         -
+ pooled TCP connections              -          -         23 us         5.9 us      36 us
+ append-only Raft log             0.48 ms    0.25 ms         -            -            -
+ batched leader fsync             0.36 ms    0.19 ms         -            -            -
```

```bash
go test -run xxx -bench Cluster -benchtime=3s ./internal/raftkv/
```

---

## Profiling the server

`scripts/flamegraph.sh` runs the server under load, captures a 15-second CPU profile via pprof, and generates a flamegraph SVG:

```bash
chmod +x scripts/flamegraph.sh
./scripts/flamegraph.sh
# output: profiles/cpu.svg
```

Based on the benchmark numbers, `syscall.Fsync` should be the widest bar in the flamegraph. The hot path is:

```
server.handleConn -> server.handlePut -> store.Put -> wal.WAL.Sync -> syscall.Fsync
```

If you ever want to improve write throughput, that is where to look. Not the parser, not the mutex.

---

## The pattern

Every performance decision in this project was made after looking at numbers, not before:

- No map sharding: benchmarks showed the mutex is thousands of times cheaper than fsync.
- No protocol optimization: the network layer is a small fraction of the fsync floor.
- Group commit: added once a concurrent-writers benchmark showed fsync-per-write left ~20x throughput on the table under load. The first version (a committer goroutine) cost ~30 us per write on the TCP benchmark from goroutine wakeups, so it was replaced with the leader pattern that keeps uncontended writes on the caller's goroutine.
- LSM sparse index: a 100k-entry table made the linear scan's cost (milliseconds) obvious.
- Raft write path: a CPU profile of cluster Puts showed 60% of time in persisting state (whole-log JSON rewrites with create/fsync/rename/dir-fsync) and ~7% dialing TCP. Fixing those two in that order cut Put latency 5.7x. Shortening the log first showed log length wasn't the main cost; the per-write file dance was.

Running benchmarks before deciding whether to optimize is what prevents wasted effort on changes that would not matter, and running them after is what catches the ones that cost more than they should.
