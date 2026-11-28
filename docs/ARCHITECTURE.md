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

### Log compaction

The log would otherwise grow with every write, even when the same key is overwritten again and again. `Compact()` rewrites it to contain only the live data, one Put per key. It also runs automatically once the log is over `CompactMinBytes` (64 MiB) *and* `CompactRatio` (4×) times the live size. Live size is tracked incrementally as records are applied. Compaction runs as the group-commit leader, so no write can land between taking the snapshot and swapping in the new log, while reads continue. The new log is fsynced before it is atomically renamed over the old one, and an interrupted compaction's temp file is removed on open, so a crash at any point leaves exactly one complete log. `TestStoreCompactionFaultInjection` SIGKILLs a writer that compacts every few dozen writes: all trials recover exactly, with the log bounded at about 15 KB over 2,000 writes.

Compaction is turned off once the log has been handed to the legacy replication leader (`WAL()`), because followers resume by byte offset into the file.

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

A malformed PUT (missing, extra, negative or non-numeric length) closes the connection. The client has almost certainly sent the value anyway, and without a valid length the server can't skip it. An earlier version replied `-ERR` and kept going, so the value bytes were parsed as the next command: a value of `DEL victim` deleted `victim` (`TestMalformedPutCannotInjectCommands`).

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

Before responding to any RPC that changes state, the node makes the change durable. Without this, a restarted node could vote twice in the same term or forget entries it acknowledged. Heartbeats that change nothing don't trigger a write.

The state is split across two files (`internal/raft/persist.go`):

- **Term and vote** go in a small JSON file, rewritten atomically (tmp, fsync, rename, fsync dir). This happens only on elections.
- **The log** is an append-only file built on `internal/wal`, so records are checksummed and a torn tail is cut off on open. Appending costs one write plus one fsync, regardless of log length. Each record carries its absolute index, and writing entry *i* supersedes any entries ≥ *i* on replay. That is how a follower's truncation of a conflicting suffix is recorded without rewriting the file. The file starts with a base record (the snapshot index and term) and is rewritten only when a snapshot compacts the log.

An earlier version rewrote everything, log included, as one JSON file on every change: create, fsync, rename and fsync the directory, for each entry. Replacing that cut a 3-node cluster's Put from ~2.1 ms to ~0.5 ms. Old-format files are migrated on first start.

**Crash between snapshot and log.** The snapshot file is written first, then the log file is rewritten. If a crash lands in between, the log's base is older than the snapshot. Restore keeps only the entries after the snapshot, and only if the log agrees with the snapshot at its last index. It then rewrites the file: otherwise a log that *conflicted* with the snapshot would keep those entries on disk, and on the next restart they would take down any entries appended in between (`TestSnapshotNewerThanLogFile` fails without that rewrite).

**Leader batching (thesis §10.2.1).** A leader appends proposals to memory and replicates them immediately; a background loop fsyncs everything pending in one batch. The leader counts itself toward a majority only for entries it has made durable (`durableIndex`), so a committed entry is always on disk on a majority. Followers still persist before replying. With 16 concurrent clients this brings per-write cost to ~0.19 ms.

If persisting fails, the node panics. A node that can't make its vote or log durable can't safely participate, and carrying on would hide the problem until a restart silently broke an invariant.

### Transport

Three transports:

`MemTransport`: in-process, direct method calls. Used in tests. Supports partition injection.

`TCPTransport`: real TCP connections, JSON-encoded RPCs (RequestVote, AppendEntries, InstallSnapshot, ReadIndex). Used when running actual processes. Connections are persistent: each carries one request at a time, and up to 4 idle ones per peer are pooled. Concurrent RPCs use separate connections, so a slow InstallSnapshot never delays a heartbeat. A dead pooled connection is retried once on a fresh one, which is safe because every Raft RPC is idempotent. `Close` also closes inbound connections, so a stopped node can't keep answering on connections that were already open. Dialing per RPC used to cost a handshake on every heartbeat; pooling cut leader read latency over loopback from 69 µs to 23 µs.

`SimTransport`: in-process with a seeded PRNG controlling message drops. Any test failure with seed S can be reproduced exactly by re-running with seed S.

### Snapshots

`TakeSnapshot(index, data)` compacts the log up to the given index. The sentinel at log[0] is replaced with a dummy entry carrying the last included term so prevLogTerm lookups still work. The snapshot data (whatever the state machine passes in) is written atomically to disk.

Raft indices are absolute and never reset, but after compaction `log[0]` stands for `snapshotIndex`, not 0. All log access goes through helpers (`lastIndex`, `termAt`, `entryAt`) that translate. An earlier version indexed the slice directly with Raft indices, so after the first snapshot commits stalled or `applyLoop` panicked with index out of range; the CLI hit this after 100 writes.

The snapshot file and the state file are written separately, snapshot first. The state file records `LogStart`, the absolute index of its `log[0]`, so if a crash lands between the two writes, restart can tell which entries the newer snapshot already covers.

**InstallSnapshot.** When a follower's nextIndex is at or below the leader's snapshot, the entries it needs no longer exist, so the leader sends its snapshot instead (at most one in flight per peer). The follower keeps any log suffix that agrees with the snapshot's last entry, discards the rest, and delivers the snapshot to the state machine through `applyCh` (`ApplyMsg.SnapshotValid`). `applyLoop` is the only sender on `applyCh`, so snapshots and commands are always delivered in order.

### The KV state machine (internal/raftkv)

`raftkv.KV` sits between the TCP server and Raft:

- **Writes** are proposed, then the caller waits for the entry at the returned (index, term) to be applied. If a different entry is applied at that index (a new leader overwrote it), the write fails with `ErrLost`. If nothing happens within the timeout, it fails with `ErrTimeout` (outcome unknown). A write is acknowledged only once a quorum has it *and* it's applied locally, so a leader that loses its quorum never returns +OK, and a client always reads its own acknowledged writes from the leader.
- **Reads** are linearizable by default via ReadIndex (next section). `--stale-reads` (`Options.StaleReads`) serves them from local state instead.
- **State** is an in-memory store. The Raft log plus snapshots are the durable copy, so on boot the state machine is restored from the latest snapshot and the log is replayed from there. A separate WAL would only add a second fsync per write and a file that never shrinks.
- **Encoding**: commands are JSON with `[]byte` fields (base64), and store snapshots are a list of base64 key/value pairs. Plain JSON strings would replace invalid UTF-8 with U+FFFD and silently corrupt binary keys and values.

### Membership changes

Servers join and leave one at a time (thesis §4.1, `internal/raft/membership.go`). A configuration maps each member ID to its transport address. A change is an ordinary log entry (`Kind == EntryConfig`) carrying the complete new configuration. Each server uses the latest configuration in its log *as soon as it is appended*, committed or not, and falls back to an earlier one if that entry is truncated. Elections, commit counting and ReadIndex all count majorities of the current configuration. The configuration as of a snapshot point is stored in the snapshot and sent with InstallSnapshot, so a node that joins after the change was compacted away still learns it.

Single-server changes are safe because any majority of the old configuration overlaps any majority of the new one. The rules that keep them safe, each with a test that fails when the rule is removed:

- **One change at a time.** A new change is refused until the previous config entry has committed.
- **Commit in the current term first.** A leader must have committed an entry in its term before proposing a change. This fixes the bug in the original single-server algorithm described on raft-dev in 2015; the election no-op makes it cheap.
- **A removed leader** keeps leading until its removal commits, without counting itself toward majorities, then steps down (§4.2.2).
- **No disruption** (§4.2.3). A server that has heard from a leader within the minimum election timeout ignores RequestVote entirely, without even adopting the candidate's term. A removed server stops getting heartbeats, so it keeps timing out and campaigning with ever-higher terms; this rule makes the cluster ignore it. In the chaos test's final dump, removed nodes sat at terms 31–32 while the real cluster stayed at term 16.
- **CheckQuorum.** A leader that hasn't heard from a majority within the maximum election timeout steps down, without changing the term. Ignoring RequestVotes (the previous rule) depends on this. Otherwise a leader cut off from the majority, but still reaching a few followers, keeps those followers ignoring candidates, and no one can gather a majority. In a 4-node cluster where the leader reaches only one node, nothing committed until the partition healed. The chaos test exposed this under CPU contention, by sometimes committing nothing at all, and `TestLeaderWithoutQuorumStepsDown` reproduces it deterministically.
- **Non-members never campaign.** That is also how a new node waits: `--join` starts it with an empty configuration, and it stays passive until a leader adds it and replicates the configuration to it.

`TestMembershipChaos` runs random adds, removes, partitions, crashes and restarts (with state on disk) under a stream of proposals. A monitor samples every node's role and term for two leaders in one term, and at the end every node's applied sequence is checked against the others. Writing it surfaced two lessons about the test itself. First, unpaced chaos (thousands of crashes a second) committed nothing, so the safety checks passed vacuously; it now asserts progress. Second, random chaos almost never produces an exact 2/2 split, so it missed an injected "a tie counts as a majority" bug. `TestEvenSplitHasNoQuorum` covers that case directly.

It also found a real bug: after a membership change, the newest committed entry is a config entry, which (like a no-op) is never delivered on `applyCh`, so the read index pointed at an index the state machine could never reach. Every linearizable read hung until the next write. `LogEntry.delivered()` is now the single definition used by both `applyLoop` and ReadIndex.

### Wire encoding

RPCs between nodes use a hand-written binary codec (`codec.go`): uvarints, length-prefixed fields, and every length bounds-checked against the remaining input, so garbage can't cause a panic or a huge allocation (`FuzzDecodeRPC`). JSON had been ~25% of CPU on the write path. Binary requests use new type codes. A server still answers JSON requests in JSON, and a client that gets hung up on (which is what older versions do with an unknown type) retries in JSON and remembers that peer, so clusters can be upgraded one node at a time. That was verified with a cluster of two old nodes and one new node, with the new node leading.

AppendEntries batches are capped at 4 MiB of commands. Without the cap, a follower more than 32 MiB behind (the old message limit) received an oversized message on every attempt, never caught up, and campaigned forever. Snapshots get a separate 2 GiB message limit, since they carry the whole state machine.

### Linearizable reads: ReadIndex

Reading from local state is fast but not safe. A follower may not have applied the latest writes yet; with local reads, 200 out of 200 follower reads issued right after a write came back stale in an end-to-end test, because followers learn the commit index on the *next* heartbeat. Worse, a leader cut off by a partition keeps believing it is leader until it times out, and meanwhile the majority can elect a new leader and accept writes it will never see.

ReadIndex (Raft thesis §6.4, `internal/raft/readindex.go`) makes reads linearizable without writing anything to the log:

1. The leader waits until it has committed an entry from its current term (the election no-op). Before that, its commitIndex may lag what an earlier leader committed.
2. It records `readIndex = commitIndex`.
3. It starts a new heartbeat *round*: `hbRound` is bumped, and every AppendEntries/InstallSnapshot carries the round number current when it was built. Any reply from a peer that accepts our term (success or log mismatch) records `ackRound[peer]`. Once a majority (counting itself) has acked a round ≥ the read's round, no newer leader existed when the read arrived, so nothing past readIndex could have been committed.
4. The caller waits until its state machine has applied readIndex, then reads locally.

Followers send a `ReadIndex` RPC to the leader, which runs steps 1-3 and returns the index; the follower then waits to apply it locally. Only the leader answers, so a request can't loop. If the leader is unknown or can't confirm within the timeout, the read fails (`-ERR` on the wire) instead of falling back to local state.

One detail: the commit index right after an election is the leader's no-op, which is never delivered on `applyCh`. A state machine tracking progress from `applyCh` would wait for it forever, so ReadIndex steps back to the last entry that carries a command. The state there is identical.

**Batching.** At most one confirmation round is in flight. Reads that arrive meanwhile are assigned the *next* round, which starts the moment the current one reaches a quorum. Every read still waits for a round that began after it arrived, so safety is unchanged, but N concurrent reads cost about one round trip instead of N broadcasts. `TestConcurrentReadsShareRounds` holds a round open, fires 20 reads, and checks they complete in 2 rounds.

Cost: one heartbeat round trip per read on the leader (~90 µs p50 end to end over TCP), plus a hop to the leader on followers (~230 µs). 16 concurrent leader reads cost ~6 µs each. Leader leases would remove the round trip, at the price of assuming bounded clock drift.

The proof is in the tests: `TestLinearizableHistory` records concurrent writes through the leader and reads from all three nodes, then runs the Wing-Gong checker in `internal/linearcheck` on the history. With ReadIndex disabled, it and `TestDeposedLeaderCannotServeStaleRead` fail every time.

---

## LSM storage engine

The LSM engine in `internal/lsm` is an alternative to the WAL-backed store. Where the WAL store keeps every key in memory, the LSM keeps only the MemTable and sparse indexes in memory and the data in sorted files, so it can hold more data than RAM.

### Write-ahead log

Every write is appended to a WAL and fsynced before it reaches the MemTable, so a reader never sees a value that recovery couldn't rebuild. Each MemTable generation has its own WAL file (`wal-NNNNNN.log`). On flush, the SSTable is made durable *before* its WAL is deleted, so at every instant each write is in at least one of the two. A WAL that survives a crash next to its SSTable is replayed on open; that only re-applies values the SSTable already has, in order, which is harmless. `Open` replays leftover WALs oldest first (`TestReplayOrderAfterInterruptedFlush` fails if the order is reversed) and flushes the result.

Writes use the same leader-based group commit as the WAL store: concurrent writers share one fsync, and each batch is applied to the MemTable in queue order with one leader at a time, so WAL order always equals MemTable order (`TestConcurrentSameKeyMemoryMatchesRecovery` fails if a batch is applied out of order). With 64 writers a durable Put costs ~7 µs instead of ~184 µs. Readers never wait on a writer's fsync. A first version had writers take turns on a mutex and found their write already committed only when their turn came; those serial wake-ups capped the gain at ~27 µs per write. `Options.NoSync` skips the per-write fsync: writes still reach the OS before returning, so they survive a process crash, but not a power failure.

Before the WAL, the engine's fast writes (~60 ns) were fast because they weren't durable: anything not yet flushed was lost on crash. A durable LSM Put now costs one fsync (~160 µs), the same as the WAL store; `NoSync` costs ~0.5 µs.

`TestLSMFaultInjection` runs a writer with a 2 KB MemTable, so it is constantly flushing and compacting, and SIGKILLs it at random points. The recovered state must equal the model after exactly the acknowledged operations, or one more (the operation in flight may or may not have landed). With the WAL disabled it fails on the first trial.

### SSTable format and reads

Each entry is `[1]kind [4]keyLen [4]valLen key value`, sorted by key, where kind is put or tombstone. (Tombstones used to be a magic value string, which meant storing that exact string deleted the key.)

On open or write, each SSTable builds a sparse in-memory index holding the offset of every 16th key. A lookup checks the table's key range, binary-searches the index, seeks, and scans at most 16 entries, stopping early because keys are sorted. Point reads in a 100k-entry table take ~3 us instead of ~2.6 ms for the original linear scan.

Each SSTable also gets a Bloom filter over its keys (`bloom.go`), built during the same scan, so the file format doesn't change. It uses 10 bits per key and 7 probes derived from one FNV-1a hash (Kirsch-Mitzenmacher), for a ~0.8% false-positive rate (0.79% measured). A "no" is exact, so a lookup for an absent key skips the table without touching its index or the disk. Tombstones are in the filter, because a delete must still be found to stop the search. A miss across three overlapping tables went from ~8 µs to ~0.24 µs; the memory cost is 1.25 bytes per key.

### Leveled compaction

The first design had two levels, and every L0 compaction merged *all* of L1 into one file. Write amplification therefore grew linearly with the data: 6× at 5 MB, 17× at 16 MB. The engine now uses the LevelDB scheme (`levels.go`):

- **Levels:** L1–L6 hold files with disjoint key ranges, sorted by key, each about `TargetFileSize`; each level's budget is 10× the previous one. A lookup reads at most one file per level (binary search on file ranges, then the bloom filter).
- **Compaction:** when L0 has 4 files, they merge with only the L1 files they overlap. When a level exceeds its budget, one of its files (chosen round-robin through the key space) merges with the files it overlaps one level down. Outputs are split at `TargetFileSize`.
- **Tombstones:** a tombstone is dropped only when no deeper level has a file covering its key. Otherwise it must keep shadowing the older value below. The randomized model test catches dropping them unconditionally.

Write amplification for scattered keys is now 5.9× / 10.2× / 13.7× at 200k / 600k / 1.8M keys, growing with the number of levels rather than the data size.

### Durability and recovery

SSTables are written to a `.tmp` file, fsynced, renamed, and the directory fsynced. Which files are live is recorded in a `MANIFEST`: a log of atomic edits (files added, files deleted), each a checksummed `internal/wal` record. Every change follows the same order: make the new SSTables durable, append the edit and fsync it, then delete the old files. On open, the manifest decides what is live, and any other `.sst` file (output of a compaction interrupted before its edit, or input whose deletion was interrupted) is removed. The manifest is rewritten from scratch after 1,000 edits. Directories written before the manifest existed are opened with the old two-level rule, then given one.

### Write path

Writes go to the MemTable, an in-memory map that is sorted when it is flushed. When the MemTable exceeds the size limit (4 MiB by default, `Options.MemTableSize`), it flushes to an SSTable file on disk and a new MemTable and WAL start.

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

The plain search is factorial, and that bit for real: CI hit a 60-op history from the Raft read tests that ran for over nine minutes. Two standard optimisations fix it:

- **Per-key decomposition.** Linearizability is local (Herlihy & Wing): a history over independent objects is linearizable iff each object's sub-history is. Each key is an independent register, so each is checked separately.
- **Memoisation.** The search state is (set of ops already linearized, register value). Any path reaching a state already known to fail is cut off immediately. Lowe's checker and Porcupine use the same trick.

The old checker didn't finish an 80-op generated history in 60 seconds. The new one checks 3,000 heavily overlapping ops in about 0.1 s (`TestCheckScalesToLargeHistories`), and it still rejects histories with a single corrupted read (`TestCheckFindsViolationInLargeHistory`).

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
