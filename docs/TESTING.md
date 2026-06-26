# Testing

How to verify everything works, from the automated test suite to running a real 3-node cluster.

---

## Prerequisites

```bash
go version   # need 1.22 or newer
go build ./...   # should produce no output
```

---

## Full automated suite

This runs every test across all packages with the race detector:

```bash
go test -race ./...
```

Expected output, every package should say ok:

```
ok  kvengine/internal/linearcheck
ok  kvengine/internal/lsm
ok  kvengine/internal/raft
ok  kvengine/internal/replication
ok  kvengine/internal/server
ok  kvengine/internal/store
ok  kvengine/internal/wal
ok  kvengine/test/fault_injection
```

Run it a few times to catch any timing-sensitive flakiness in the Raft tests:

```bash
go test -race -count=3 -timeout 3m ./internal/raft/...
```

---

## Package by package

### internal/wal

```bash
go test -race -v ./internal/wal/...
```

- `TestAppendReplayRoundTrip`: write records, close, reopen, verify they are all there
- `TestReplayTornTail`: manually truncate a record mid-write to simulate a crash; replay must recover the clean prefix and silently drop the torn one
- `FuzzReadRecord`: seed corpus includes a max-length field (0xFFFFFFFF) that previously caused a 4 GB allocation; the sanity cap in ReadRecord catches it

### internal/store

```bash
go test -race -v ./internal/store/...
```

- `TestRestartRecoversState`: write data, close, reopen, verify state survived via WAL replay
- `TestConcurrentStress`: 50 goroutines hammering 20 keys; must pass `go test -race` cleanly
- `TestLinearizability`: 30 goroutines on 8 keys, records completion order, replays against a reference model; proves the values are correct not just that it did not crash

### test/fault_injection

```bash
go test -v ./test/fault_injection/...
```

This one takes about 30 seconds. It spawns a real subprocess, writes records in a loop with fsync after each, kills it with SIGKILL at a random point, and verifies no confirmed write was lost. Runs 15 times with different kill timing.

Skip during fast iteration:

```bash
go test -short ./...
```

### internal/server

```bash
go test -race -v ./internal/server/...
```

- `TestPartialWrites`: sends a request one byte at a time over `net.Pipe`; the parser must handle it identically to a normal request
- `TestIdleTimeout`: a silent connection gets closed within IdleTimeout
- `TestSlowClientDoesNotBlockOthers`: a frozen client that never reads responses does not prevent other clients from completing

### internal/replication

```bash
go test -race -v ./internal/replication/...
```

- `TestFollowerResumeAfterDisconnect`: writes batch 1, cancels the follower (crash sim), writes batch 2, restarts the follower, verifies all records present with no duplicates and the right count

### internal/raft

```bash
go test -race -v -timeout 60s ./internal/raft/...
```

- `TestConvergenceAfterHeal`: leader gets partitioned, new leader elected, old leader's divergent log gets overwritten when partition heals, all three nodes agree on the same 10 commands
- `TestNoCommitWithoutQuorum`: isolated leader cannot commit; proves quorum is actually required
- `TestPersistenceAcrossRestart`: node stopped and restarted from disk recovers term and log correctly
- `TestTCPTransportCluster`: 3 nodes over real TCP loopback sockets, not in-memory; elect a leader and replicate 5 commands
- `TestSimPacketLoss`: 20% packet drop rate; Raft must still elect a leader and commit entries
- `TestSimHighDropRatePartition`: 90% drop rate to one follower; cluster still makes progress with the other follower

### internal/lsm

```bash
go test -race -v ./internal/lsm/...
```

- `TestTombstoneAcrossLevels`: Delete in MemTable must shadow the value in L0, not fall through to it
- `TestCompaction`: merges L0 into L1 correctly; updated keys show the new value, deleted keys are gone
- `TestSSTableFileCountBounded`: after 20 rounds of writes and flushes, total SSTable file count stays bounded

### internal/linearcheck

```bash
go test -race -v ./internal/linearcheck/...
```

- `TestCheckNotLinearizable`: two Gets returning different values with no Put between them is correctly identified as impossible
- `TestConcurrentStoreLinearizable`: 6 goroutines, 8 ops each, 3 keys; records every operation against a real Store and verifies the full history is linearizable

---

## Manual testing: single node

```bash
go build -o kvengine ./cmd/kvengine
./kvengine data.wal serve :6380
```

In another terminal:

```bash
printf 'PUT hello 5\r\nworld\r\n' | nc -q1 127.0.0.1 6380
# +OK

printf 'GET hello\r\n' | nc -q1 127.0.0.1 6380
# $5
# world

printf 'DEL hello\r\n' | nc -q1 127.0.0.1 6380
# +OK

printf 'GET hello\r\n' | nc -q1 127.0.0.1 6380
# $-1
```

Kill the server (Ctrl-C), restart it with the same command, and GET again. The value should still be gone because the DEL was fsynced before the kill.

To verify the WAL actually persists: write a value, kill the server, restart it, and GET. The value should come back.

```bash
rm -f data.wal kvengine
```

---

## Manual testing: 3-node Raft cluster

Open 3 terminals, all in the same directory.

**Terminal 1:**
```bash
mkdir -p /tmp/kv/{n0,n1,n2}
./kvengine raft --id n0 --raft-addr 127.0.0.1:7000 --kv-addr 127.0.0.1:6380 \
  --peers "n1=127.0.0.1:7001,n2=127.0.0.1:7002" --data-dir /tmp/kv/n0
```

**Terminal 2:**
```bash
./kvengine raft --id n1 --raft-addr 127.0.0.1:7001 --kv-addr 127.0.0.1:6381 \
  --peers "n0=127.0.0.1:7000,n2=127.0.0.1:7002" --data-dir /tmp/kv/n1
```

**Terminal 3:**
```bash
./kvengine raft --id n2 --raft-addr 127.0.0.1:7002 --kv-addr 127.0.0.1:6382 \
  --peers "n0=127.0.0.1:7000,n1=127.0.0.1:7001" --data-dir /tmp/kv/n2
```

Wait about a second for election, then in a 4th terminal:

```bash
# Try each port until one returns +OK. That node is the leader.
printf 'PUT mykey 5\r\nhello\r\n' | nc -q1 127.0.0.1 6380
printf 'PUT mykey 5\r\nhello\r\n' | nc -q1 127.0.0.1 6381
printf 'PUT mykey 5\r\nhello\r\n' | nc -q1 127.0.0.1 6382

# Read from any node (reads are local)
printf 'GET mykey\r\n' | nc -q1 127.0.0.1 6381
```

**Failover test:**

Kill the leader (Ctrl-C in its terminal). The other two elect a new leader in about 400ms. Try writing to them:

```bash
printf 'PUT afterfail 2\r\nok\r\n' | nc -q1 127.0.0.1 6381
printf 'PUT afterfail 2\r\nok\r\n' | nc -q1 127.0.0.1 6382
```

One of them will accept it. Restart the killed node and it will catch up automatically from the new leader.

```bash
rm -rf /tmp/kv kvengine
```

---

## Edge cases worth trying manually

**Empty value:**
```bash
printf 'PUT empty 0\r\n\r\n' | nc -q1 127.0.0.1 6380
printf 'GET empty\r\n' | nc -q1 127.0.0.1 6380
# $0
```

**Unknown command (connection should stay open):**
```bash
{ printf 'INVALID\r\n'; printf 'GET hello\r\n'; } | nc -q1 127.0.0.1 6380
# -ERR unknown command "INVALID"
# $-1  (connection was not closed)
```

---

## Benchmarks

```bash
# Store layer
go test -bench=. -benchtime=3s ./internal/store/...

# Server over real TCP
go test -bench=. -benchtime=3s ./internal/server/...

# LSM engine
go test -bench=. -benchtime=3s ./internal/lsm/...
```

Compare `BenchmarkPutSequential` vs `BenchmarkPutParallel` vs `BenchmarkPutSameKeyParallel`. They should all be within noise of each other around 550 us. This confirms fsync is the bottleneck, not the lock.

---

## Fuzzing

```bash
# Fast: just run the seed corpus
make fuzz-seeds

# Full: mutate inputs for 60 seconds per target
make fuzz FUZZ_SECS=60
```

Any crash found is saved to `testdata/fuzz/<target>/<hash>` and will be replayed automatically on the next `go test` run.

---

## Flamegraph

```bash
make flamegraph
# generates profiles/cpu.svg
```

This starts the server, runs concurrent load against it for 15 seconds, and captures a CPU profile via pprof. Open the SVG in a browser. `syscall.Fsync` should be the widest bar.
