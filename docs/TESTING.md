# Testing guide

Everything you need to verify kvengine works, from unit tests to a
real 3-node Raft cluster on your own machine.

---

## 0. Prerequisites

```bash
# You need Go 1.22+
go version          # should print go1.22.x or newer

# Clone and build
cd kvengine
go build ./...      # should produce no output (= no errors)
```

---

## 1. Run the full automated test suite (start here)

This single command runs every test across all packages with the race
detector enabled:

```bash
go test -race ./...
```

Expected output — every package should say `ok`, no `FAIL` lines:

```
ok  kvengine/internal/linearcheck
ok  kvengine/internal/raft
ok  kvengine/internal/replication
ok  kvengine/internal/server
ok  kvengine/internal/store
ok  kvengine/internal/wal
ok  kvengine/test/fault_injection
```

Run it a second time with `-count=3` to catch any timing-sensitive flakiness
(particularly in the Raft election tests):

```bash
go test -race -count=3 -timeout 3m ./internal/raft/...
```

---

## 2. What each package's tests prove

### `internal/wal` — crash durability

```bash
go test -race -v ./internal/wal/...
```

- `TestAppendReplayRoundTrip`: write records, close, reopen, verify they're all there
- `TestReplayMissingFile`: opening a non-existent WAL returns empty (not an error)
- `TestReplayTornTail`: manually write half a record to simulate a crash mid-append;
  replay must recover the clean prefix and silently drop the torn record

### `internal/store` — KV store correctness

```bash
go test -race -v ./internal/store/...
```

- `TestPutGetDelete`: basic round-trip
- `TestRestartRecoversState`: the crash-recovery integration test — write data, close (simulating shutdown), reopen, verify state survived via WAL replay
- `TestConcurrentStress`: 50 goroutines hammering the same 20 keys; must be race-clean
- `TestLinearizability`: 30 goroutines on 8 keys, records completion order, replays against reference model — proves the *values* are correct, not just "didn't crash"
- Benchmarks: `go test -bench=. -benchtime=2s ./internal/store/...`

### `test/fault_injection` — kill-9 durability proof

```bash
go test -v ./test/fault_injection/...
```

This test takes ~30s. It spawns a real subprocess, writes records in a loop with
fsync after each, kills it with SIGKILL at a random point 15 times, and verifies
after each kill that no confirmed write was lost and no corruption crept in.

Skip it during fast iteration with `-short`:

```bash
go test -short ./...   # skips fault injection
```

### `internal/server` — networking

```bash
go test -race -v ./internal/server/...
```

- `TestProtocolBasic`: PUT/GET/DEL over a real TCP connection, exact wire format
- `TestPartialWrites`: sends a request one byte at a time over `net.Pipe` — proves the parser doesn't assume whole-message reads
- `TestIdleTimeout`: a silent connection gets closed within `IdleTimeout`
- `TestConcurrentClients`: 20 clients simultaneously, each verifies its own write
- `TestSlowClientDoesNotBlockOthers`: a frozen client doesn't stall the server for others

### `internal/replication` — leader-follower streaming

```bash
go test -race -v ./internal/replication/...
```

- `TestReplicationBasic`: 20 leader writes replicate to follower
- `TestReplicationDelete`: DEL propagates correctly
- `TestFollowerResumeAfterDisconnect`: the key test — writes batch 1, cancels follower (crash sim), writes batch 2, restarts follower, verifies all records present with no duplicates
- `TestReplicationLagThenCatchUp`: fresh follower joins a leader that already has 30 records; replays all of them from offset 0

### `internal/raft` — Raft consensus

```bash
go test -race -v -timeout 60s ./internal/raft/...
```

- `TestLeaderElection`: 3-node cluster elects exactly one leader (Election Safety)
- `TestLeaderElectionFiveNodes`: same with 5 nodes
- `TestBasicReplication`: all 3 nodes apply 4 commands in the same order
- `TestPartitionLeaderFromFollowers`: cuts leader off, new leader elected, old can't commit
- `TestConvergenceAfterHeal`: ex-leader's divergent log gets overwritten, all nodes agree on 10 commands after heal
- `TestNoCommitWithoutQuorum`: isolated leader cannot commit — quorum is required
- `TestPersistenceAcrossRestart`: node restarted from disk recovers term and log (no voting twice)
- `TestTCPTransportCluster`: 3 nodes communicating over **real TCP loopback sockets** (not in-memory), elect a leader and agree on 5 commands
- `TestSnapshotTruncatesLog`: log compacted to 6 entries after snapshot at index 5

### `internal/linearcheck` — mathematical correctness proof

```bash
go test -race -v ./internal/linearcheck/...
```

- `TestCheckLinearizable`: a simple sequential history passes
- `TestCheckNotLinearizable`: a provably impossible history is correctly rejected
- `TestCheckDeleteLinearizable`: Put → Delete → Get(miss) is valid
- `TestConcurrentStoreLinearizable`: 6 goroutines × 8 ops on 3 keys against a real Store; records every operation and verifies the entire history is linearizable

---

## 3. Manual testing — single-node server

Build the binary:

```bash
go build -o kvengine ./cmd/kvengine
```

Start a single-node KV server:

```bash
./kvengine data.wal serve :6380
```

In a second terminal, talk to it with `nc` (the protocol is text):

```bash
# PUT a value (key=hello, value length=5, then the 5 bytes)
printf 'PUT hello 5\r\nworld\r\n' | nc -q1 127.0.0.1 6380

# GET it back
printf 'GET hello\r\n' | nc -q1 127.0.0.1 6380
# Expected: $5\r\nworld\r\n

# DELETE it
printf 'DEL hello\r\n' | nc -q1 127.0.0.1 6380

# GET again (should be nil)
printf 'GET hello\r\n' | nc -q1 127.0.0.1 6380
# Expected: $-1\r\n
```

Kill the server (Ctrl-C), restart it, and GET again — the value survives
because it was fsynced to `data.wal`:

```bash
./kvengine data.wal serve :6380 &
printf 'GET hello\r\n' | nc -q1 127.0.0.1 6380
# $-1\r\n  (deleted before crash — correct)
```

Clean up:

```bash
rm -f data.wal kvengine
```

---

## 4. Manual testing — 3-node Raft cluster

This runs 3 real processes on your machine forming a Raft cluster.
Open **3 terminals**.

**Terminal 1 — node 0:**
```bash
mkdir -p /tmp/kv/{n0,n1,n2}
./kvengine raft \
  --id n0 \
  --raft-addr 127.0.0.1:7000 \
  --kv-addr 127.0.0.1:6380 \
  --peers "n1=127.0.0.1:7001,n2=127.0.0.1:7002" \
  --data-dir /tmp/kv/n0
```

**Terminal 2 — node 1:**
```bash
./kvengine raft \
  --id n1 \
  --raft-addr 127.0.0.1:7001 \
  --kv-addr 127.0.0.1:6381 \
  --peers "n0=127.0.0.1:7000,n2=127.0.0.1:7002" \
  --data-dir /tmp/kv/n1
```

**Terminal 3 — node 2:**
```bash
./kvengine raft \
  --id n2 \
  --raft-addr 127.0.0.1:7002 \
  --kv-addr 127.0.0.1:6382 \
  --peers "n0=127.0.0.1:7000,n1=127.0.0.1:7001" \
  --data-dir /tmp/kv/n2
```

Wait ~1 second for leader election (you'll see `Raft listening` in each terminal).
Then in a **4th terminal**:

```bash
# Write to the cluster leader (try each port if you get "not leader")
printf 'PUT mykey 5\r\nhello\r\n' | nc -q1 127.0.0.1 6380
# or 6381, or 6382 — whichever node is the leader

# Read from any node (reads are local)
printf 'GET mykey\r\n' | nc -q1 127.0.0.1 6381
```

### Test leader failover

Kill the leader node (Ctrl-C in its terminal). Within ~400ms, the remaining
two nodes elect a new leader. Writes to the new leader should succeed:

```bash
printf 'PUT afterfail 2\r\nok\r\n' | nc -q1 127.0.0.1 6381
```

Restart the killed node — it rejoins, catches up from the new leader's log,
and serves reads again.

### Clean up

```bash
rm -rf /tmp/kv kvengine
```

---

## 5. Run benchmarks

```bash
# Store layer (shows fsync is the bottleneck, not the mutex)
go test -bench=. -benchtime=3s ./internal/store/...

# Server over real TCP
go test -bench=. -benchtime=3s ./internal/server/...
```

Compare `BenchmarkPutSequential` vs `BenchmarkPutParallel` vs
`BenchmarkPutSameKeyParallel` — they should all be within noise of each other
(~550µs), confirming fsync dominates and map sharding would not help.

---

## 6. Specific edge-case scenarios to manually verify

These aren't automated but worth poking at manually with the running server:

**Empty value:**
```bash
printf 'PUT empty 0\r\n\r\n' | nc -q1 127.0.0.1 6380
printf 'GET empty\r\n' | nc -q1 127.0.0.1 6380
# Expected: $0\r\n\r\n
```

**Binary value (the protocol is binary-safe):**
```bash
printf 'PUT bin 3\r\n\x01\x02\x03\r\n' | nc -q1 127.0.0.1 6380
printf 'GET bin\r\n' | nc -q1 127.0.0.1 6380
# Expected: $3\r\n<three binary bytes>\r\n
```

**Unknown command (should not close the connection):**
```bash
{ printf 'INVALID\r\n'; printf 'GET hello\r\n'; } | nc -q1 127.0.0.1 6380
# First line: -ERR unknown command "INVALID"
# Second line: value or $-1 — connection was NOT closed
```

---

## 7. What "ALL GREEN" means

When `go test -race ./...` passes:

- **No data races**: the Go race detector checked every actual concurrent
  memory access at runtime. This is not a static analysis guess — it's
  proof that no two goroutines touched shared memory unsafely during the
  test run.
- **WAL crash safety**: a real torn write on disk is correctly handled,
  confirmed by a real SIGKILL to a real subprocess.
- **Concurrency correctness**: 48 concurrent KV operations are
  mathematically proven linearizable — not just "didn't crash," but
  "produced results consistent with some valid sequential ordering."
- **Raft safety**: election safety, log matching, and no-commit-without-quorum
  are all demonstrated, including through injected network partitions.
- **Persistence**: a Raft node stopped and restarted from disk recovers
  its term and log without violating any invariants.
