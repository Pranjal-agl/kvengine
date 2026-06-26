# Architecture

This doc records *design decisions and why*, not just what the code does. Read
this before changing the WAL format or concurrency model — there's usually a
reason something is the way it is, and if there isn't, that's noted too.

---

## Phase 1: Storage engine (WAL + crash recovery)

### Core invariant: write-ahead

A write is never visible in memory until it is durable on disk. Order is
always: encode record → append to log buffer → flush + fsync → only then
mutate the in-memory map. This is the entire point of a WAL — if we update
memory first and crash before fsync, we'd have served a value that crash
recovery can't reconstruct.

### On-disk record format

Each record is a self-describing, checksummed frame, append-only:

```
byte offset   field        size      notes
0             CRC32        4 bytes   IEEE CRC32 of everything in `body` below
4             Length        4 bytes   length of `body` in bytes
8             body:
  8           Type          1 byte    1 = Put, 2 = Delete
  9           KeyLen        4 bytes
  13          Key           KeyLen bytes
  13+KeyLen   ValLen        4 bytes   (0 for Delete)
  ...         Value         ValLen bytes
```

All integers little-endian. CRC32 covers `body` (Type + KeyLen + Key + ValLen
+ Value) but not the CRC/Length header fields themselves — checksumming your
own length-prefix is circular.

**Why length-prefixed + CRC, not a delimiter:** delimiters require escaping
inside binary key/value data. Length-prefixing is simpler and the CRC is what
actually matters for detecting torn writes (see below).

### Crash recovery semantics

On crash mid-append, the *last* record in the file may be a torn write —
some bytes made it to disk, others didn't (no fsync happened for the
in-flight write, or fsync itself was interrupted). `wal.Replay` handles this
by treating **the first invalid/truncated record encountered as end-of-log**,
not as an error to surface to the caller:

- If `io.ReadFull` can't read a full 8-byte header → stop, return everything
  read so far.
- If it can't read `Length` bytes for the body → stop (torn body).
- If the CRC doesn't match → stop (the bytes belong to a record that started
  writing but didn't finish, or were corrupted).

This is correct because of the write-ahead invariant: a record is only ever
appended to the log *before* it's considered committed. If it's torn, the
caller's `Put`/`Delete` either never returned success (so the client never
believed the write happened) or the fsync genuinely failed and they got an
error. Either way, dropping a torn tail record loses no acknowledged write.

**What this does NOT yet handle (open TODOs, see ROADMAP):**
- Mid-file corruption (not just a torn tail) — e.g. bit rot on a record in
  the *middle* of the file. Current code would stop replay early at that
  point and silently lose everything after it. A real implementation needs
  either per-record fsync boundaries it can trust, or to keep scanning past
  a bad record using resync (scan forward for the next plausible header).
  Worth fixing before calling this "production" but acceptable for a first
  pass since the most common crash mode (kill -9 / power loss) only tears
  the tail.
- No checksumming of the *file* as a whole, no torn-write detection if the
  filesystem reorders writes without barriers (we rely on fsync semantics
  being honored by the OS/filesystem; on most Linux filesystems with default
  mount options this holds).

### fsync policy

Phase 1 fsyncs on every `Put`/`Delete` (`Store.Put` calls `log.Sync()` before
returning). This is the simplest-to-reason-about policy: every acknowledged
write is durable, full stop. It is also slow — one fsync per write.

**Stretch/future work:** group commit (batch N pending writes, fsync once,
ack them all) trades a small latency window for much higher throughput. Not
implemented yet — get correctness first, then profile, then optimize. Do not
add this prematurely; it complicates the durability story and should only go
in once there's a benchmark proving it's needed.

### Concurrency (current)

`Store` uses a single `sync.RWMutex` guarding the in-memory map. Reads take
`RLock`, writes take `Lock`. The WAL itself has its own mutex around
`Append`/`Sync` so concurrent writers serialize correctly there too — but
note this means a writer holds the *store* lock only while updating the map
(after fsync returns), not during the fsync itself, so fsync latency from one
writer doesn't block readers. It does block other writers via the WAL's own
mutex, which is correct (writes must be ordered in the log).

**This is a known bottleneck, intentionally not yet optimized** (see Phase 2
in ROADMAP) — e.g. sharding the map by key hash to reduce write contention,
or separating the "append to log" critical section from the "update map"
critical section more carefully. Don't shard prematurely; phase 2 is where
this gets revisited with real concurrent benchmarks driving the decision.

---

## Phase 2: Concurrency (results)

**Status: stress-tested, benchmarked, decision made — no sharding.**

### Race and linearizability proof

`internal/store/concurrent_test.go` has two tests:
- `TestConcurrentStress`: 50 goroutines × 200 ops each, random Put/Get/Delete
  over a small 20-key space (so collisions are frequent, not avoided), run
  under `go test -race`. Clean.
- `TestLinearizability`: 30 goroutines × 150 ops over an 8-key space,
  recording each write's *completion order* via a global atomic counter
  (valid because the WAL mutex already serializes all writes into a real
  total order), then replaying that exact order against a plain-map
  reference model and asserting the real Store's final state matches
  exactly. This is the proof that concurrent access doesn't corrupt or lose
  updates — not just "doesn't crash," but "produces the value a correct
  sequential execution would have produced."

### Benchmark results (see `internal/store/bench_test.go`)

Run on the dev sandbox (**1 vCPU** — see caveat below):

```
BenchmarkPutSequential        4306 reps    547913 ns/op   (~548 µs/op)
BenchmarkGetSequential    42147806 reps        51.4 ns/op
BenchmarkGetParallel      44594617 reps        53.5 ns/op
BenchmarkPutParallel          4023 reps    576355 ns/op   (~576 µs/op, different keys)
BenchmarkPutSameKeyParallel   3805 reps    558341 ns/op   (~558 µs/op, all same key)
```

### Decision: do not shard the map

The numbers say the map mutex is not the bottleneck, by about **four orders
of magnitude**:

- `Put` costs ~550 µs regardless of whether writers target different keys,
  the same key, or run sequentially with no contention at all. If the map
  lock were a meaningful cost, same-key (worst case for any lock) would be
  visibly slower than different-key — it isn't, within noise.
- `Get` costs ~51-53 ns — about 10,000x cheaper than `Put`. The map itself
  is essentially free; `fsync` is the entire cost of a write.

This is exactly what `docs/ARCHITECTURE.md`'s Phase 1 section predicted:
every `Put`/`Delete` calls `wal.Sync()` (flush + fsync) before returning,
and fsync is a disk round-trip measured in hundreds of microseconds —
overwhelming anything happening in-memory. Sharding the map would shave
nanoseconds off an operation that costs microseconds; not worth the added
complexity (and sharding wouldn't even help the real bottleneck, since the
WAL itself is a single append-only file serialized by one mutex regardless
of how the map is structured).

**The real lever for write throughput, if needed later, is group commit**
(batch N pending writes, fsync once, ack them all) — noted as a Phase 1 TODO
in `docs/ROADMAP.md` and intentionally not implemented yet. That's the
correct next optimization *if* benchmarks of Phase 3 (networking) show write
throughput is a real-world bottleneck — not before, and not the map lock.

**Honest caveat:** this sandbox has 1 vCPU, so `BenchmarkGetParallel` and
`BenchmarkPutParallel` aren't really testing multi-core contention —
`GOMAXPROCS` effectively caps actual parallelism. The *qualitative* conclusion (fsync
cost dwarfs map-lock cost by ~10,000x) is robust to this since it's a
single-threaded comparison (same-key vs different-key vs sequential Put, all
~equal). But before fully closing the book on "no sharding needed," it's
worth re-running these benchmarks on real multi-core hardware to confirm the
RWMutex read-side doesn't degrade under genuine concurrent CPU pressure.
Flagged here so a future session doesn't treat this as settled beyond what
the evidence actually supports.

## Phase 3: Networking (results)

**Status: implemented and tested.** `internal/server` has a working TCP
server in front of `Store`.

### Protocol

A simple, binary-safe, line-based text protocol (RESP-inspired but
simplified — no nested arrays, no need for them here):

```
PUT <key> <valueLen>\r\n<valueLen bytes of value>\r\n   -> +OK\r\n
GET <key>\r\n                                            -> $<len>\r\n<value>\r\n   or   $-1\r\n  (miss)
DEL <key>\r\n                                            -> +OK\r\n  (idempotent)
<anything unrecognized>                                  -> -ERR <message>\r\n
```

Keys are plain text (space/CR/LF-delimited, like Redis's inline commands).
Values are **binary-safe** via explicit length-prefixing — this is the part
that actually matters, since values are arbitrary bytes and can't be
delimiter-scanned safely.

**Why text framing for the command line but length-prefixed for the value:**
command lines are short, human-debuggable with `nc`, and never contain
attacker-controlled binary data by construction (the key is the only
variable part, and in this protocol keys can't contain whitespace). The
value, in contrast, is wherever arbitrary bytes live, so it gets the robust
treatment.

### Partial read/write handling (the actual must-have)

Two distinct mechanisms, both deliberately NOT assuming a full message
arrives in one `Read()`:
- **Command lines**: `readLine()` reads byte-by-byte from a `*bufio.Reader`
  (which itself buffers underlying socket reads efficiently) until it hits
  `\n`, bounded by `maxLineSize` to prevent unbounded memory growth from a
  client that never sends one.
- **Value bytes**: `io.ReadFull()` — loops internally until exactly N bytes
  are read or a real error/EOF occurs. This is the standard library's
  correct primitive for exactly this problem; no reason to hand-rewrite it.

`TestPartialWrites` (in `internal/server/server_test.go`) proves this isn't
just incidentally correct: it uses `net.Pipe` (a fully synchronous in-memory
connection — the most adversarial framing case available without real
network jitter) and writes a request **one byte at a time** with sleeps
between writes. The server handles it identically to a normal request.

### Deadlines

Every connection gets a read deadline re-armed before each new command
(`defaultIdleTimeout` = 30s, overridable via `Server.IdleTimeout` — tests use
short timeouts to stay fast). A client that goes silent gets its connection
closed; a client that's slow but still sending gets a fresh window each
time progress is made. The value-read phase of `PUT` re-arms the deadline
again before reading potentially-large value bytes, so a big value from a
slow client isn't unfairly penalized by the same window meant for "did this
client send anything at all."

### Backpressure policy: per-connection goroutine + blocking I/O

One goroutine per connection (`Serve`'s accept loop spawns a goroutine per
`Accept()`), and all reads/writes are synchronous/blocking. This is a
deliberate choice over an async/buffered-queue model:

- If a client stops reading its responses, the server's `bufio.Writer`
  flush eventually blocks on the OS socket send buffer filling up — and
  **that blocks only that one connection's goroutine**, not the server.
  `TestSlowClientDoesNotBlockOthers` proves this directly: a connection that
  sends a request and never reads the response does not prevent a second,
  independent connection from completing promptly.
- The tradeoff: a goroutine-per-connection model doesn't scale to extreme
  connection counts (C10M-style) the way an event-loop/epoll model would.
  For this project's scope, goroutines are cheap enough (a few KB stack
  each) that this isn't a real concern until connection counts get into the
  tens of thousands — revisit only if a benchmark shows it's the actual
  bottleneck, per the project's general "don't optimize without numbers"
  philosophy.

### Benchmark: end-to-end over real TCP

`BenchmarkPutGetOverTCP` (in `internal/server/bench_test.go`) does a real
`PUT` + `GET` round trip per iteration over an actual TCP loopback
connection (not a mock):

```
BenchmarkPutGetOverTCP    3578 reps    630438 ns/op   (~630 µs/op for PUT+GET combined)
```

Consistent with the Phase 2 finding: `Put` alone was ~550µs (fsync-bound).
Adding a GET, plus two real network round trips, plus protocol
parsing/framing, brings the combined PUT+GET pair to ~630µs — meaning the
network/protocol layer adds roughly **~80µs of overhead** on top of the
fsync floor, not a multiple of it. The protocol layer is not the
bottleneck; durability still is.

**Not yet done (see ROADMAP):** a direct load-test comparison against a
real server like Redis for an external baseline number — useful context,
not done yet, and the sandbox's 1 vCPU limits how meaningful a raw
throughput comparison would be anyway.

## Phase 4: Replication (results)

**Status: implemented and tested.** `internal/replication` has `Leader` and
`Follower` types; four integration tests covering basic catch-up, delete
propagation, disconnect/resume, and lagged-follower catch-up.

### Wire protocol

```
follower → leader: 8-byte little-endian int64  (follower's current offset)
leader  → follower: raw WAL encoded bytes from that offset, indefinitely
```

The raw bytes streamed by the leader are **exactly the same format as the
on-disk WAL** — the same CRC32-checksummed, length-prefixed frames. The
follower decodes them with `wal.ReadRecord`, the same function used for
crash recovery. One format, two uses: no separate replication encoding to
maintain or get wrong.

### Consistency model: async

The leader fsyncs locally (durability is guaranteed on the leader) and then
streams. `Put`/`Delete` return to the client before the follower has applied
the record. A follower may lag the leader by an arbitrary number of records;
if the leader crashes before a record is streamed, the follower loses it —
but the leader's own WAL has it, so it's not globally lost.

This is the right choice for Phase 4: it's simpler, correct, and useful.
Synchronous replication (leader waits for follower ack before returning to
client) would give stronger consistency but requires either a quorum protocol
(Raft) or explicit "at-least-one-follower" durability semantics — that's
Phase 5 territory.

### Offset tracking and crash-safe resume

The follower tracks its position in the leader's byte stream and persists it
atomically after each applied record (`write tmp → fsync → rename` so the
file is never half-written). On restart, the follower reads this file and
sends that offset to the leader, which resumes streaming from exactly that
point — so no record is applied twice and no record applied before the crash
is lost (it's already in the follower's own WAL).

`TestFollowerResumeAfterDisconnect` (in `internal/replication/replication_test.go`)
proves this end-to-end: write batch 1 → let follower catch up → cancel
follower (simulating crash) → write batch 2 → restart follower → assert all
records from both batches are present, correct, with no duplicates, using
`fs.Len()` to detect any extra entries.

### Follower's own WAL

The follower's `store.Put`/`store.Delete` write through to the follower's
own WAL, so the follower is itself crash-recoverable — it can restart
(replaying its own WAL to rebuild its in-memory state), then resume from the
leader at the persisted offset. No special-case recovery path; the follower
is just a store that happens to be populated via replication rather than
client writes.

### WAL changes (additive, backward-compatible)

`internal/wal/wal.go` gained:
- `durableOff int64` — byte position of end of last fsynced record
- `pendingBytes int64` — bytes buffered but not yet fsynced; added to
  `durableOff` on `Sync()`
- `notify chan struct{}` — closed+replaced on each `Sync()` so `StreamFrom`
  waiters can block on "no new data" via a `select`, not a busy-poll
- `DurableOffset() int64` — exported accessor
- `StreamFrom(fromOffset int64, dst io.Writer, done <-chan struct{}) error`
  — opens a separate read fd, copies raw bytes up to `durableOff`, blocks
  when caught up until new data arrives or `done` is closed
- `ReadRecord(r *bufio.Reader) (Record, int64, error)` — exported version
  of the record decoder, also returns frame byte size for offset tracking

All existing WAL tests pass unchanged — the changes are purely additive.

## Phase 5 (stretch): Raft

Goals: replace manual leader-follower with real consensus — leader election,
log replication with the Raft safety properties, membership changes
(optional). Test harness should inject network partitions and verify
linearizability of client-visible operations across the partition.

---

## Reference codebases (for when stuck on a specific phase)

- **Redis** (github.com/redis/redis) — protocol design, event loop.
- **TigerBeetle** (github.com/tigerbeetle/tigerbeetle) — deterministic
  simulation testing philosophy; worth reading even though it's Zig, for the
  *testing methodology*, not the code itself.
- **bbolt** (github.com/etcd-io/bbolt) — small, readable Go B+tree engine;
  good model for how to structure a Go storage package.
- **hashicorp/raft** (github.com/hashicorp/raft) — production Raft in Go,
  read alongside the original paper (Ongaro & Ousterhout, "In Search of an
  Understandable Consensus Algorithm").
