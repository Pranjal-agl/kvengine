# Fault injection

`fault_test.go` proves crash durability against a real killed process,
rather than just a hand-crafted torn write (that unit test lives in
`internal/wal/wal_test.go::TestReplayTornTail`).

## How it works

1. `faultwriter/main.go` is a tiny standalone program: open a WAL, write N
   records in a loop, fsync after each, print `WROTE <i>` to stdout right
   after each fsync succeeds.
2. The test builds that binary, runs it as a subprocess, and sends it
   `SIGKILL` after a random short delay (0-7ms) — landing at an
   unpredictable point in the write stream each trial.
3. After the process is confirmed dead, the test runs `wal.Replay` on the
   file it was writing and checks two things:
   - **Durability**: every write the subprocess had already confirmed
     (printed `WROTE` for) is present in the replayed records. Losing one of
     these would mean fsync lied.
   - **Correctness**: the replayed records are an exact, gap-free,
     uncorrupted prefix of the intended sequence — no partial/garbage
     records leak through.
4. Repeated across many trials (`trials` const in the test) so the kill
   timing lands at different points in the write stream each run — early,
   late, mid-record, between records.

## Running it

```bash
go test ./test/fault_injection/...          # run it
go test -short ./...                         # skip it (it's slower; spawns processes)
```

## Extending this harness later

- **Phase 2 (concurrency)**: run multiple faultwriter-style processes
  concurrently against the *same* WAL path isn't valid (one WAL = one
  writer), but once `internal/store` gets a concurrent stress test, this
  harness's "kill mid-write, verify recovery" pattern should be repeated
  with concurrent writers active to confirm recovery still holds.
- **Phase 4 (replication)**: extend this pattern to kill a *follower*
  mid-stream and verify it can resume/recover correctly from the leader's
  WAL stream without duplicating or losing entries.
- **Deterministic simulation (stretch)**: the current approach uses real
  OS-level kills with random timing, which is realistic but not
  reproducible — a failing trial can't be replayed exactly. A more advanced
  version (TigerBeetle-style) would inject faults at the I/O layer
  deterministically (e.g. a fake filesystem that can be told "fail/truncate
  the Nth write") so failures are 100% reproducible from a seed. Worth doing
  if this project goes deep on the testing side; not required for Phase 1.
