# Architecture

This doc covers design decisions and why I made them, not just what the code does. If you want to change the WAL format or the concurrency model, read this first. There's usually a reason things are the way they are.

---

## Storage engine: WAL + crash recovery

### The core rule: write-ahead

A write is never visible in memory until it is on disk. The order is always: encode the record, append to the log buffer, flush and fsync, then update the in-memory map. If we updated memory first and crashed before the fsync, we would have served a value that crash recovery can never reconstruct.

A second rule falls out of the first: memory must be updated in the same order as the log. If two writes to one key hit the log as A, B but memory as B, A, readers see A while recovery rebuilds B. An earlier version had exactly this bug (append and map update were under different locks); group commit below fixes it structurally.

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

Dropping it from the *replay* is not enough, though: the torn bytes also have to be removed from the *file*. Otherwise the next writes are appended after the garbage, and the next replay stops at the garbage again, losing every write acknowledged since the first crash. `wal.Open` therefore scans for the end of the last valid record, copies anything after it to `<wal>.corrupt-<offset>`, truncates, and fsyncs before accepting appends. (`TestWritesAfterTornTailSurviveSecondRestart` reproduces the original bug.)

What this does not handle yet:
- Mid-file corruption (not just a torn tail). If a record in the middle of the file is corrupt, recovery stops there and everything after it is moved to the `.corrupt-<offset>` side file rather than replayed. The bytes are preserved for inspection, but the records are not recovered automatically.
- No whole-file checksum. We rely on fsync semantics being honored by the OS and filesystem, which holds on Linux with default mount options.

### fsync policy

Every Put and Delete is fsynced before it returns, so every acknowledged write is durable. Concurrent writers share fsyncs through group commit (next section), so the cost is one fsync per *batch*, not per write.

### Concurrency model

Reads take an `RWMutex` read lock on the map and never touch the WAL.

Writes use leader-based group commit, the same pattern as LevelDB's write queue. Each writer appends its request to a queue. If no commit is in progress, that writer becomes the leader: it takes everything queued (up to 256 writes) as one batch, appends the batch to the WAL, fsyncs once, applies the batch to the map in log order under the map's write lock, and wakes the batch's writers. If more writers queued up during the fsync, it hands leadership to the first of them.

Properties:
- One leader at a time, and each batch is applied in log order, so memory order always matches log order.
- An uncontended write commits on the caller's goroutine. An earlier version used a dedicated committer goroutine, which cost a goroutine wakeup per write (~30 us on the TCP benchmark); the leader pattern removed that.
- Under contention, writers that arrive during an fsync share the next one. With 64 concurrent writers the per-write cost drops from ~150 us to ~7 us.
- The map lock is held only while applying a batch to memory, never during fsync, so slow disks don't block readers.

I benchmarked map sharding before deciding against it. The mutex costs ~50 ns, an fsync costs hundreds of microseconds. Sharding would improve the cheap part. See BENCHMARKS.md.

---

## Concurrency correctness

### How it was tested

`internal/store/concurrent_test.go` has two tests:

`TestConcurrentStress`: 50 goroutines, 200 ops each, random Put/Get/Delete over a 20-key space so collisions are frequent. Run under `go test -race`. Clean.

`TestLinearizability`: 30 goroutines over 8 keys. Records each write's invocation and return time from a global atomic counter. For each key, the final value must come from a write that no other write on that key strictly follows in real time; anything else means writes were reordered or lost. An earlier version used completion order as *the* order, which is unsound: two overlapping writes can be linearized either way, and with group commit, writes sharing an fsync return at the same moment in arbitrary order.

`TestConcurrentSameKeyMemoryMatchesLog`: concurrent writers hammer three keys, then the test compares memory before a restart with WAL replay after it. This caught the append/map-update ordering bug.

The full Wing-Gong check (including reads) lives in `internal/linearcheck`.

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

A full PUT + GET round trip over real TCP loopback costs about the same as a single WAL Put: the network and protocol layer is a small fraction of the fsync floor. The protocol is not the bottleneck. See BENCHMARKS.md.

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

Election Safety: at most one leader per term. Enforced by each node voting for at most one candidate per term (votedFor persisted before granting any vote) and requiring a majority. votedFor is cleared only when the term *increases*. An earlier version cleared it on every AppendEntries, including the first heartbeat from the leader the node had just voted for, which let a delayed RequestVote from a second candidate in the same term win a second vote (`TestVoteNotResetBySameTermHeartbeat`).

New leaders append a no-op entry for their term (§8 of the paper). A leader can only commit entries from its own term by counting replicas, so without the no-op, leftover entries from earlier terms — all of them, after a full-cluster restart — would stay uncommitted until the next client write.

Log Matching: if two logs have an entry with the same index and term, they are identical up to that index. Enforced by the AppendEntries consistency check (PrevLogIndex, PrevLogTerm).

Leader Completeness: committed entries are never lost. Enforced by the vote restriction: a candidate must have a log at least as up-to-date as the voter's.

### Persistence

Before responding to any RPC that changes state, the node persists currentTerm, votedFor, the log, and the log's starting index to disk via an atomic write (write tmp, fsync, rename, fsync dir). Without this, a restarted node could vote twice in the same term. Heartbeats that change nothing don't trigger a write.

If persisting fails, the node panics. A node that can't make its vote or log durable can't safely participate, and carrying on would hide the problem until a restart silently broke an invariant.

### Transport

Three transports:

`MemTransport`: in-process, direct method calls. Used in tests. Supports partition injection.

`TCPTransport`: real TCP connections, JSON-encoded RPCs (RequestVote, AppendEntries, InstallSnapshot). Used when running actual processes.

`SimTransport`: in-process with a seeded PRNG controlling message drops. Any test failure with seed S can be reproduced exactly by re-running with seed S.

### Snapshots

`TakeSnapshot(index, data)` compacts the log up to the given index. The sentinel at log[0] is replaced with a dummy entry carrying the last included term so prevLogTerm lookups still work. The snapshot data (whatever the state machine passes in) is written atomically to disk.

Raft indices are absolute and never reset, but after compaction `log[0]` stands for `snapshotIndex`, not 0. All log access goes through helpers (`lastIndex`, `termAt`, `entryAt`) that translate. An earlier version indexed the slice directly with Raft indices, so after the first snapshot commits stalled or `applyLoop` panicked with index out of range; the CLI hit this after 100 writes.

The snapshot file and the state file are written separately, snapshot first. The state file records `LogStart`, the absolute index of its `log[0]`, so if a crash lands between the two writes, restart can tell which entries the newer snapshot already covers.

**InstallSnapshot.** When a follower's nextIndex is at or below the leader's snapshot, the entries it needs no longer exist, so the leader sends its snapshot instead (at most one in flight per peer). The follower keeps any log suffix that agrees with the snapshot's last entry, discards the rest, and delivers the snapshot to the state machine through `applyCh` (`ApplyMsg.SnapshotValid`). `applyLoop` is the only sender on `applyCh`, so snapshots and commands are always delivered in order.

### The KV state machine (internal/raftkv)

`raftkv.KV` sits between the TCP server and Raft:

- **Writes** are proposed, then the caller waits for the entry at the returned (index, term) to be applied. If a different entry is applied at that index (a new leader overwrote it), the write fails with `ErrLost`. If nothing happens within the timeout, it fails with `ErrTimeout` (outcome unknown). A write is acknowledged only once a quorum has it *and* it's applied locally, so a leader that loses its quorum never returns +OK, and a client always reads its own acknowledged writes from the leader.
- **Reads** come from the local state machine and are not linearizable (see Known gaps in the README).
- **State** is an in-memory store. The Raft log plus snapshots are the durable copy, so on boot the state machine is restored from the latest snapshot and the log is replayed from there. A separate WAL would only add a second fsync per write and a file that never shrinks.
- **Encoding**: commands are JSON with `[]byte` fields (base64), and store snapshots are a list of base64 key/value pairs. Plain JSON strings would replace invalid UTF-8 with U+FFFD and silently corrupt binary keys and values.

---

## LSM storage engine

The LSM engine in `internal/lsm` is an alternative to the WAL-backed store. The core tradeoff: writes are much faster because they go to an in-memory MemTable and are only fsynced when the MemTable fills, but unflushed data is lost on crash (the MemTable has no WAL yet).

### SSTable format and reads

Each entry is `[1]kind [4]keyLen [4]valLen key value`, sorted by key, where kind is put or tombstone. (Tombstones used to be a magic value string, which meant storing that exact string deleted the key.)

On open or write, each SSTable builds a sparse in-memory index holding the offset of every 16th key. A lookup checks the table's key range, binary-searches the index, seeks, and scans at most 16 entries, stopping early because keys are sorted. Point reads in a 100k-entry table take ~3 us instead of ~2.6 ms for the original linear scan.

### Durability and recovery

SSTables are written to a `.tmp` file, fsynced, renamed, and the directory fsynced. `Open` reloads them. Compaction always gives its L1 output a higher ID than every input and makes it visible before deleting the inputs, so if a crash interrupts the deletions, `Open` knows any file with a lower ID than the newest L1 has already been merged and discards it. Compaction writes an L1 file even when everything was deleted, because that file's ID is what marks the inputs obsolete.

### Write path

Writes go to the MemTable, an in-memory map that is sorted when it is flushed. When the MemTable exceeds the size limit (4 MiB), it flushes to an SSTable file on disk and a new MemTable starts.

### Read path

Search order: MemTable first, then L0 SSTables newest-to-oldest, then L1. A tombstone in a newer layer shadows any value in an older layer. This is the key correctness property: MemTable.Delete must stop the search before it reaches L0, otherwise a deleted key would reappear.

### Compaction

When L0 has too many SSTables (default 4), they are merged with L1 into a new L1 SSTable. Compaction:
- Applies entries oldest-to-newest so newer values override older ones correctly.
- Drops tombstoned keys so deleted data does not accumulate forever.
- Removes the old files from disk.

Three bugs were caught by tests. First, the initial merge went newest-to-oldest in the L0 loop, so the oldest entries were overwriting the newest ones. Second, the MemTable tombstone lookup returned false for missing keys and tombstoned keys the same way, so the engine would fall through to L0 and find a stale value after a delete. Third, the same mistake one level down: `SSTable.get` reported a tombstone as "not found", so a delete that had been flushed to L0 didn't hide the value in an older L0 or L1 file (`TestTombstoneInSSTableShadowsOlderSSTable`).

---

## Linearizability checker

`internal/linearcheck` implements a Wing-Gong style model checker for the KV store.

It records concurrent operations as (start time, end time, type, key, value, result) tuples and tries to find a valid sequential ordering consistent with real time: for each step, try every operation whose start time is before the earliest end time in the remaining set, apply it to a sequential reference model, and recurse. If the list empties, the history is linearizable.

This is exponential in the worst case but fast in practice for small histories because the partial order from timestamps prunes most of the search space early.

`TestCheckNotLinearizable` checks that an impossible history is correctly rejected: two concurrent Gets returning different values for the same key with no intervening Put between them.

---

## Fuzzing

Six fuzz targets across three packages:

`FuzzReadRecord` and `FuzzWALAppendReplay` in `internal/wal`: feed arbitrary bytes to the record decoder and the full append-replay cycle. One real crash was found during seed testing: a length field of 0xFFFFFFFF caused a 4 GB allocation. Fixed by adding a 32 MiB sanity cap in ReadRecord.

`FuzzServerProtocol` in `internal/server`: feed arbitrary bytes to the connection handler. Must never panic, hang, or corrupt store state.

`FuzzHandleRequestVote` and `FuzzHandleAppendEntries` in `internal/raft`: feed arbitrary JSON to the RPC handlers.

`FuzzInstallSnapshotThenAppend` in `internal/raft`: installs a snapshot at an arbitrary index, then sends an arbitrary AppendEntries, covering every case of absolute-to-slice index translation (prevLogIndex below, at, inside, and past the compacted log). Checks commitIndex and lastApplied invariants as well as panics.

Run them with `make fuzz FUZZ_SECS=60` or the individual commands in docs/TESTING.md.

---

## References

Redis (redis/redis): good reference for protocol design and event loop structure.

TigerBeetle (tigerbeetle/tigerbeetle): the best example of deterministic simulation testing. Worth reading for the testing methodology even if you are not writing Zig.

bbolt (etcd-io/bbolt): small, readable Go B+tree engine. Good model for Go storage package structure.

hashicorp/raft: production Raft in Go. Read alongside the original paper.
