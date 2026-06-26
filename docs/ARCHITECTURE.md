# Architecture

This doc covers design decisions and why I made them, not just what the code does. If you want to change the WAL format or the concurrency model, read this first. There's usually a reason things are the way they are.

---

## Storage engine: WAL + crash recovery

### The core rule: write-ahead

A write is never visible in memory until it is on disk. The order is always: encode the record, append to the log buffer, flush and fsync, then update the in-memory map. If we updated memory first and crashed before the fsync, we would have served a value that crash recovery can never reconstruct.

### On-disk record format

Each record is a self-describing, checksummed frame:

```
byte offset   field        size      notes
0             CRC32        4 bytes   IEEE CRC32 of the body below
4             Length       4 bytes   length of body in bytes
8             body:
  8           Type         1 byte    1 = Put, 2 = Delete
  9           KeyLen       4 bytes
  13          Key          KeyLen bytes
  13+KeyLen   ValLen       4 bytes   (0 for Delete)
  ...         Value        ValLen bytes
```

All integers are little-endian. The CRC32 covers the body (Type + KeyLen + Key + ValLen + Value) but not the CRC or Length fields themselves. Checksumming your own length prefix is circular.

Length-prefixing instead of delimiters: delimiters need escaping inside binary key/value data. Length-prefixing is simpler, and the CRC is what actually detects torn writes.

### Crash recovery

On crash mid-append, the last record may be torn: some bytes made it to disk, others did not. `wal.Replay` treats the first invalid or truncated record as end-of-log rather than an error. It stops there and returns everything before it.

This is safe because of the write-ahead rule: a record is only appended before it is committed. If a record is torn, the Put or Delete that wrote it either never returned success to the client, or the fsync failed and the client got an error. Either way, dropping the torn tail loses nothing acknowledged.

What this does not handle yet:
- Mid-file corruption (not just a torn tail). If a record in the middle of the file is corrupt, replay stops there and silently drops everything after it. The most common crash modes only tear the tail, so this is acceptable for now but worth fixing eventually.
- No whole-file checksum. We rely on fsync semantics being honored by the OS and filesystem, which holds on Linux with default mount options.

### fsync policy

Every Put and Delete fsyncs before returning. This is the simplest correct policy: every acknowledged write is durable. It is also slow, one fsync per write.

The right next step for throughput would be group commit: batch N pending writes, fsync once, ack them all. I have not added this because no benchmark has shown it is needed. See BENCHMARKS.md for why the current numbers do not justify it.

### Concurrency model

The store uses a single `sync.RWMutex`. Reads take RLock, writes take Lock. The WAL has its own mutex around Append and Sync, so concurrent writers serialize there. A writer holds the store lock only while updating the in-memory map after fsync returns, not during fsync itself, so fsync latency from one writer does not block readers.

I benchmarked map sharding before deciding against it. The result: Put costs ~550 us regardless of contention level. The mutex costs ~50 ns. Sharding would improve the 50ns piece of a 550,000ns operation. See BENCHMARKS.md for the full numbers.

---

## Concurrency correctness

### How it was tested

`internal/store/concurrent_test.go` has two tests:

`TestConcurrentStress`: 50 goroutines, 200 ops each, random Put/Get/Delete over a 20-key space so collisions are frequent. Run under `go test -race`. Clean.

`TestLinearizability`: 30 goroutines over 8 keys. Records each write's completion order via a global atomic counter (valid because the WAL mutex serializes all writes into a real total order), then replays that exact order against a plain-map reference model and checks the store's final state matches. This proves the values are correct, not just that it did not crash.

### Decision: no map sharding

Benchmarks showed fsync costs about 10,000x more than the mutex. Sharding would shave nanoseconds off a microsecond operation. Not worth the complexity.

---

## Networking

### Protocol

A line-based, binary-safe text protocol over TCP, similar to RESP but simpler:

```
PUT <key> <valueLen>\r\n<value bytes>\r\n  ->  +OK\r\n
GET <key>\r\n                              ->  $<len>\r\n<value>\r\n  or  $-1\r\n
DEL <key>\r\n                              ->  +OK\r\n
<unknown>                                  ->  -ERR <message>\r\n
```

Command lines are text and space-delimited. Values are length-prefixed binary so arbitrary byte content works without escaping.

### Partial read handling

Two mechanisms, neither assuming a full message arrives in one Read call:

Command lines: `readLine` reads byte-by-byte from a `*bufio.Reader` until it hits `\n`, bounded by a size limit to prevent unbounded memory growth from a client that never sends one.

Value bytes: `io.ReadFull`, which loops until exactly N bytes are read or an error occurs.

`TestPartialWrites` sends a full request one byte at a time over `net.Pipe` (the most adversarial framing case short of real network jitter) and verifies the server handles it identically to a normal request.

### Deadlines and backpressure

Every connection gets a read deadline re-armed before each new command. A silent client gets its connection closed. A slow but active client gets a fresh window each time it makes progress.

Backpressure policy: one goroutine per connection, synchronous blocking I/O. If a client stops reading responses, the write eventually blocks on the OS send buffer, which blocks only that connection's goroutine. `TestSlowClientDoesNotBlockOthers` proves a frozen client does not stall the server for everyone else.

### Benchmark

A full PUT + GET round trip over real TCP loopback costs about 630 us. The WAL Put alone costs about 550 us. The network and protocol layer adds roughly 80 us on top of the fsync floor. The protocol is not the bottleneck.

---

## Replication

### Wire protocol

```
follower -> leader: 8-byte little-endian int64  (follower's current byte offset)
leader  -> follower: raw WAL encoded bytes from that offset, indefinitely
```

The bytes streamed by the leader are exactly the same format as the on-disk WAL. The follower decodes them with `wal.ReadRecord`, the same function used for crash recovery. One format, two uses, no separate replication encoding to maintain.

### Consistency model: async

The leader fsyncs locally then streams. Put and Delete return to the client before the follower has applied the record. A follower may lag. If the leader crashes before streaming a record, the follower misses it, but the leader's WAL still has it.

This is a deliberate choice. Synchronous replication would require either a quorum protocol (that is Raft) or explicit at-least-one-follower semantics. Async replication is simpler and useful for the leader-follower phase before Raft.

### Crash-safe resume

After applying each record, the follower atomically writes its current leader-stream byte offset to disk: write to a tmp file, fsync, rename. On restart it reads this file, sends that offset to the leader, and resumes from exactly that point. No record is applied twice, no record is lost.

`TestFollowerResumeAfterDisconnect` verifies this: write batch 1, cancel the follower (crash sim), write batch 2, restart the follower, assert all records present with no duplicates.

---

## Raft consensus

### Algorithm

Standard Raft per the Ongaro and Ousterhout paper. The key invariants:

Election Safety: at most one leader per term. Enforced by each node voting for at most one candidate per term (votedFor persisted before granting any vote) and requiring a majority.

Log Matching: if two logs have an entry with the same index and term, they are identical up to that index. Enforced by the AppendEntries consistency check (PrevLogIndex, PrevLogTerm).

Leader Completeness: committed entries are never lost. Enforced by the vote restriction: a candidate must have a log at least as up-to-date as the voter's.

### Persistence

Before responding to any RPC that changes state, the node persists currentTerm, votedFor, and the log to disk via an atomic write (write tmp, fsync, rename). Without this, a restarted node could vote twice in the same term.

### Transport

Two transports:

`MemTransport`: in-process, direct method calls. Used in tests. Supports partition injection.

`TCPTransport`: real TCP connections, JSON-encoded RPCs. Used when running actual processes.

`SimTransport`: in-process with a seeded PRNG controlling message drops. Any test failure with seed S can be reproduced exactly by re-running with seed S.

### Snapshots

`TakeSnapshot(index, data)` compacts the log up to the given index. The sentinel at log[0] is replaced with a dummy entry carrying the last included term so prevLogTerm lookups still work. The snapshot data (whatever the state machine passes in) is written atomically to disk.

---

## LSM storage engine

The LSM engine in `internal/lsm` is an alternative to the WAL-backed store. The core tradeoff: writes are much faster because they go to an in-memory MemTable and are only fsynced when the MemTable fills, but unflushed data is lost on crash.

### Write path

Writes go to the MemTable, a sorted in-memory map. When the MemTable exceeds the size limit (4 MiB), it flushes to an SSTable file on disk and a new MemTable starts.

### Read path

Search order: MemTable first, then L0 SSTables newest-to-oldest, then L1. A tombstone in a newer layer shadows any value in an older layer. This is the key correctness property: MemTable.Delete must stop the search before it reaches L0, otherwise a deleted key would reappear.

### Compaction

When L0 has too many SSTables (default 4), they are merged with L1 into a new L1 SSTable. Compaction:
- Applies entries oldest-to-newest so newer values override older ones correctly.
- Drops tombstoned keys so deleted data does not accumulate forever.
- Removes the old files from disk.

Two bugs were caught during testing. First, the initial merge went newest-to-oldest in the L0 loop, so the oldest entries were overwriting the newest ones. Second, the MemTable tombstone lookup returned false for missing keys and tombstoned keys the same way, so the engine would fall through to L0 and find a stale value after a delete. Both required tests to surface and were fixed before merging.

---

## Linearizability checker

`internal/linearcheck` implements a Wing-Gong style model checker for the KV store.

It records concurrent operations as (start time, end time, type, key, value, result) tuples and tries to find a valid sequential ordering consistent with real time: for each step, try every operation whose start time is before the earliest end time in the remaining set, apply it to a sequential reference model, and recurse. If the list empties, the history is linearizable.

This is exponential in the worst case but fast in practice for small histories because the partial order from timestamps prunes most of the search space early.

`TestCheckNotLinearizable` checks that an impossible history is correctly rejected: two concurrent Gets returning different values for the same key with no intervening Put between them.

---

## Fuzzing

Five fuzz targets across three packages:

`FuzzReadRecord` and `FuzzWALAppendReplay` in `internal/wal`: feed arbitrary bytes to the record decoder and the full append-replay cycle. One real crash was found during seed testing: a length field of 0xFFFFFFFF caused a 4 GB allocation. Fixed by adding a 32 MiB sanity cap in ReadRecord.

`FuzzServerProtocol` in `internal/server`: feed arbitrary bytes to the connection handler. Must never panic, hang, or corrupt store state.

`FuzzHandleRequestVote` and `FuzzHandleAppendEntries` in `internal/raft`: feed arbitrary JSON to the RPC handlers.

Run them with `make fuzz FUZZ_SECS=60` or the individual commands in docs/TESTING.md.

---

## References

Redis (redis/redis): good reference for protocol design and event loop structure.

TigerBeetle (tigerbeetle/tigerbeetle): the best example of deterministic simulation testing. Worth reading for the testing methodology even if you are not writing Zig.

bbolt (etcd-io/bbolt): small, readable Go B+tree engine. Good model for Go storage package structure.

hashicorp/raft: production Raft in Go. Read alongside the original paper.
