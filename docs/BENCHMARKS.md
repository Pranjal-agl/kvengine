# Benchmarks

All numbers from a single-vCPU sandbox (Intel Xeon @ 2.80GHz). If you run these on real multi-core hardware you will get different parallel numbers, but the single-threaded ones should be similar.

```bash
go test -bench=. -benchtime=3s ./internal/store/...
go test -bench=. -benchtime=3s ./internal/server/...
go test -bench=. -benchtime=3s ./internal/lsm/...
```

---

## WAL-backed store

```
BenchmarkPutSequential        4306 reps    547913 ns/op   (~548 us/op)
BenchmarkGetSequential    42147806 reps        51.4 ns/op
BenchmarkGetParallel      44594617 reps        53.5 ns/op
BenchmarkPutParallel          4023 reps    576355 ns/op   (~576 us/op, different keys)
BenchmarkPutSameKeyParallel   3805 reps    558341 ns/op   (~558 us/op, same key)
```

Put costs about 550 us regardless of contention. Writers hitting the same key, different keys, or running with no other writers at all all measure within noise of each other. The constant is fsync, one OS round-trip to durable storage per write.

Get costs about 51 ns. That is roughly 10,000x cheaper than Put. The RWMutex and map lookup are essentially free compared to disk I/O.

### Why I did not shard the map

The obvious optimization for a concurrent KV store is sharding the map by key hash to reduce mutex contention. These numbers show that would be worthless. The lock costs ~50 ns. The fsync costs ~550,000 ns. Sharding the map would improve the 50 ns piece of a 550,000 ns operation.

I ran the benchmarks first and made the decision based on the numbers. The sharding complexity would not have been justified by any measurable improvement.

The actual lever for write throughput would be group commit: batch N pending writes, fsync once, ack them all. Not implemented yet because no workload has shown it is needed.

---

## Network layer

```
BenchmarkPutGetOverTCP    3578 reps    630438 ns/op   (~630 us for PUT+GET combined)
```

A full PUT + GET over real TCP loopback costs about 630 us. The WAL Put alone costs about 550 us. So the network layer (two round trips, request parsing, response formatting) adds roughly 80 us on top, about 14% of the total. The protocol is not the bottleneck.

---

## LSM engine

```
BenchmarkLSMPut           ~250 ns/op   (in-memory MemTable write)
BenchmarkLSMGet           ~120 ns/op   (MemTable lookup)
BenchmarkLSMGetAfterFlush ~8 us/op     (SSTable linear scan)
```

### WAL store vs LSM

| Property | WAL store | LSM engine |
|---|---|---|
| Write latency | ~550 us (fsync per write) | ~250 ns (in-memory, batch fsync) |
| Read latency, hot | ~51 ns (map lookup) | ~120 ns (memtable lookup) |
| Read latency, cold | ~51 ns | ~8 us (SSTable scan) |
| Durability window | Per write | MemTable flush interval (4 MiB) |
| Log growth | Forever without checkpointing | Bounded by compaction |
| Complexity | Low | Medium |

LSM writes are about 2,000x faster than WAL writes because they avoid per-write fsync. The tradeoff is that unflushed MemTable data is lost on crash, so the durability window is larger.

LSM hot reads (~120 ns) are comparable to the WAL store (~51 ns). Cold reads from SSTable (~8 us) are slower because they require a file scan. A production LSM like LevelDB or RocksDB uses bloom filters and sparse indexes to bring this down to sub-microsecond. The implementation here is the correct but naive version.

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

- No map sharding: benchmarks showed the mutex is 10,000x cheaper than fsync.
- No protocol optimization: the network layer adds 14% on top of the fsync floor.
- No group commit: no benchmark has shown write throughput is an actual bottleneck.

Running benchmarks before deciding whether to optimize is what prevents wasted effort on changes that would not matter.
