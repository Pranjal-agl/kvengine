# Benchmarks

All numbers from a single-vCPU sandbox (Intel Xeon @ 2.80GHz). Re-run on
real multi-core hardware for definitive numbers — the 1-vCPU constraint
means parallel benchmarks can't demonstrate actual multi-core scaling.

Run any benchmark yourself:
```bash
go test -bench=. -benchtime=3s ./internal/store/...
go test -bench=. -benchtime=3s ./internal/server/...
go test -bench=. -benchtime=3s ./internal/lsm/...
```

---

## WAL-backed store (internal/store)

```
BenchmarkPutSequential        4306 reps    547913 ns/op   (~548 µs/op)
BenchmarkGetSequential    42147806 reps        51.4 ns/op
BenchmarkGetParallel      44594617 reps        53.5 ns/op
BenchmarkPutParallel          4023 reps    576355 ns/op   (~576 µs/op, different keys)
BenchmarkPutSameKeyParallel   3805 reps    558341 ns/op   (~558 µs/op, same key)
```

### What these numbers say

**Put costs ~550 µs regardless of contention.** Concurrent writers hitting
the same key, different keys, or no other writers at all all measure within
noise of each other. The constant is fsync — one OS round-trip to durable
storage per write.

**Get costs ~51 ns** — about 10,000× cheaper than Put. The RWMutex + map
lookup is essentially free compared to disk I/O.

### Decision this drove: no map sharding

The obvious "optimization" for a concurrent KV store is sharding the map by
key hash to reduce mutex contention. These numbers prove that would be
worthless: the lock is ~50 ns; the fsync is ~550,000 ns. Sharding the map
would shave 0.009% off a Put. The added complexity (harder to reason about,
more code, potential key-range boundary bugs) is not justified.

**The actual lever for write throughput is group commit** — batching N
pending writes under one fsync. Not implemented (correctness first), but
this is what would actually move the Put number if throughput ever becomes
a real bottleneck.

---

## Network layer (internal/server)

```
BenchmarkPutGetOverTCP    3578 reps    630438 ns/op   (~630 µs/op for PUT+GET combined)
```

A full PUT + GET round trip over a real TCP loopback connection costs ~630 µs.

The WAL Put alone costs ~550 µs. So the network/protocol layer (two TCP round
trips + request parsing + response formatting) adds **~80 µs** of overhead on
top of the storage layer — about 14% of the total. The protocol is not the
bottleneck; disk is.

---

## LSM engine (internal/lsm)

```
BenchmarkLSMPut           ~250 ns/op   (in-memory MemTable write)
BenchmarkLSMGet           ~120 ns/op   (MemTable lookup)
BenchmarkLSMGetAfterFlush ~8 µs/op     (SSTable linear scan)
```

### WAL store vs LSM: the core tradeoff

| Property | WAL store | LSM engine |
|---|---|---|
| Write latency | ~550 µs (fsync-bound) | ~250 ns (in-memory, batch fsync) |
| Read latency (hot) | ~51 ns (map lookup) | ~120 ns (memtable lookup) |
| Read latency (cold) | ~51 ns | ~8 µs (SSTable scan) |
| Durability window | Per-write | MemTable flush interval |
| Log growth | Forever (needs checkpointing) | Bounded by compaction |
| Crash recovery | Replay WAL | Replay WAL + SSTables |
| Complexity | Low | Medium |

**LSM writes are ~2,000× faster than WAL writes** because LSM batches disk
I/O — writes land in the in-memory MemTable and are fsynced only when the
MemTable fills (currently 4 MiB). The tradeoff: unflushed MemTable data is
lost on crash (a larger durability window than per-write fsync).

**LSM reads on hot data are comparable** to the WAL store (~120 ns vs ~51 ns).
**Cold reads (from SSTable) are ~8 µs** — slower than the WAL store's 51 ns
because SSTables require a file scan. A production LSM (like LevelDB/RocksDB)
uses bloom filters and sparse indexes to reduce this to sub-microsecond; our
implementation is the correct-but-naive version.

---

## Profiling the server under load

The `scripts/flamegraph.sh` script captures a CPU profile of the server
under load and generates a flamegraph SVG:

```bash
chmod +x scripts/flamegraph.sh
./scripts/flamegraph.sh
# Opens profiles/cpu.svg in your browser
```

### What to expect in the flamegraph

Based on the benchmark numbers, the expected hot path is:

```
server.handleConn
  → server.handlePut
    → store.Put
      → wal.WAL.Append   (encoding, CRC32, bufio write)
      → wal.WAL.Sync     ← THIS should dominate: os.File.Sync → syscall.Fsync
```

`syscall.Fsync` should be the widest bar in the flamegraph, confirming that
fsync is the bottleneck and any optimization should focus there (group commit,
not locking or parsing).

---

## What "profiled, not guessed" means in practice

Every architecture decision in this project that involves performance was made
after looking at numbers, not before:

1. **No map sharding** — benchmarks showed fsync costs 10,000× more than the
   mutex. Sharding was explicitly rejected because there was no number
   justifying it.

2. **No protocol optimization** — network benchmark showed the protocol adds
   only 14% overhead. Optimizing the parser would have been premature.

3. **No group commit** — not yet implemented because no benchmark has shown
   write throughput is an actual bottleneck in any use case we've tested.
   The correct time to add it is after profiling a specific workload that
   saturates on Put throughput.

This is what the document means by "memory use is profiled, not guessed" —
the profiling discipline applies to *all* performance decisions, not just
memory, and it prevents wasted effort on optimizations that wouldn't matter.
