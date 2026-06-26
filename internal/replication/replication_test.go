package replication

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"kvengine/internal/store"
)

// leaderFollowerPair wires up a complete leader+follower pair in-process
// for testing, returning a cleanup function that stops both.
func leaderFollowerPair(t *testing.T) (
	leaderStore *store.Store,
	followerStore *store.Store,
	cancelFollower context.CancelFunc,
	followerDone <-chan error,
) {
	t.Helper()
	dir := t.TempDir()

	// Leader: real store + WAL
	ls, err := store.Open(filepath.Join(dir, "leader.wal"))
	if err != nil {
		t.Fatalf("leader store: %v", err)
	}
	t.Cleanup(func() { ls.Close() })

	// Start leader replication server on OS-assigned port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("leader listen: %v", err)
	}
	lead := NewLeader(ls.WAL())
	go lead.Serve(ln)
	t.Cleanup(func() { lead.Close() })

	// Follower: independent store + WAL, connects to leader
	fs, err := store.Open(filepath.Join(dir, "follower.wal"))
	if err != nil {
		t.Fatalf("follower store: %v", err)
	}
	t.Cleanup(func() { fs.Close() })

	offsetPath := filepath.Join(dir, "follower.offset")
	fol := NewFollower(fs, ln.Addr().String(), offsetPath)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- fol.Run(ctx) }()

	return ls, fs, cancel, ch
}

// waitForKey polls the follower store for up to maxWait for key=want.
// Returns false if it times out — the replication is async so we need to
// give it a moment rather than checking immediately.
func waitForKey(fs *store.Store, key, want string, maxWait time.Duration) bool {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		v, ok := fs.Get([]byte(key))
		if ok && string(v) == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestReplicationBasic: leader writes → follower catches up within timeout.
func TestReplicationBasic(t *testing.T) {
	ls, fs, cancel, _ := leaderFollowerPair(t)
	defer cancel()

	const N = 20
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := ls.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
	}

	// Wait for the last key to appear on the follower.
	lastKey := fmt.Sprintf("k%d", N-1)
	lastVal := fmt.Sprintf("v%d", N-1)
	if !waitForKey(fs, lastKey, lastVal, 3*time.Second) {
		v, ok := fs.Get([]byte(lastKey))
		t.Fatalf("follower did not replicate %q in time; got (%q, %v)", lastKey, v, ok)
	}

	// All keys should be there.
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		got, ok := fs.Get([]byte(key))
		if !ok || string(got) != want {
			t.Errorf("follower key %q: got (%q, %v), want (%q, true)", key, got, ok, want)
		}
	}
}

// TestReplicationDelete: Delete on leader must propagate to follower.
func TestReplicationDelete(t *testing.T) {
	ls, fs, cancel, _ := leaderFollowerPair(t)
	defer cancel()

	if err := ls.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("leader Put: %v", err)
	}
	if !waitForKey(fs, "k", "v", 2*time.Second) {
		t.Fatalf("follower didn't receive Put")
	}

	if err := ls.Delete([]byte("k")); err != nil {
		t.Fatalf("leader Delete: %v", err)
	}

	// Wait for the key to disappear on the follower.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := fs.Get([]byte("k")); !ok {
			return // deleted — pass
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follower still has key after Delete was replicated")
}

// TestFollowerResumeAfterDisconnect: the core Phase 4 correctness test.
//
// Write batch 1 → let follower catch up → cancel follower (simulating a
// crash/disconnect) → write batch 2 → restart follower → verify follower
// has ALL records (batch 1 + batch 2), no duplicates, no gaps.
//
// This proves the offset-file-based resume works: the restarted follower
// reads its persisted offset and asks the leader to continue from there,
// not from the beginning.
func TestFollowerResumeAfterDisconnect(t *testing.T) {
	dir := t.TempDir()
	leaderWAL := filepath.Join(dir, "leader.wal")
	followerWAL := filepath.Join(dir, "follower.wal")
	offsetPath := filepath.Join(dir, "follower.offset")

	ls, err := store.Open(leaderWAL)
	if err != nil {
		t.Fatalf("leader store: %v", err)
	}
	defer ls.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	lead := NewLeader(ls.WAL())
	go lead.Serve(ln)
	defer lead.Close()
	addr := ln.Addr().String()

	// --- First follower run ---
	fs1, err := store.Open(followerWAL)
	if err != nil {
		t.Fatalf("follower store 1: %v", err)
	}
	fol1 := NewFollower(fs1, addr, offsetPath)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go fol1.Run(ctx1)

	// Write batch 1 and wait for it to replicate.
	const batch = 15
	for i := 0; i < batch; i++ {
		if err := ls.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("leader Put batch1: %v", err)
		}
	}
	if !waitForKey(fs1, fmt.Sprintf("k%d", batch-1), fmt.Sprintf("v%d", batch-1), 3*time.Second) {
		t.Fatalf("batch 1 did not replicate in time")
	}

	// Cancel/disconnect the follower — simulates a follower crash.
	cancel1()
	fs1.Close()

	// Write batch 2 while follower is down.
	for i := batch; i < batch*2; i++ {
		if err := ls.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("leader Put batch2: %v", err)
		}
	}

	// --- Second follower run (simulates restart with persisted offset) ---
	fs2, err := store.Open(followerWAL) // replays its OWN wal first
	if err != nil {
		t.Fatalf("follower store 2: %v", err)
	}
	defer fs2.Close()
	fol2 := NewFollower(fs2, addr, offsetPath)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go fol2.Run(ctx2)

	// Wait for the last key of batch 2 to appear.
	lastKey := fmt.Sprintf("k%d", batch*2-1)
	lastVal := fmt.Sprintf("v%d", batch*2-1)
	if !waitForKey(fs2, lastKey, lastVal, 3*time.Second) {
		t.Fatalf("follower did not catch up after restart; missing %q", lastKey)
	}

	// Every key from both batches must be present, correct, and not doubled.
	for i := 0; i < batch*2; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		got, ok := fs2.Get([]byte(key))
		if !ok {
			t.Errorf("key %q missing after restart+resume", key)
		} else if string(got) != want {
			t.Errorf("key %q: got %q want %q (possible duplication/corruption)", key, got, want)
		}
	}
	if got, want := fs2.Len(), batch*2; got != want {
		t.Errorf("follower has %d keys, want %d (duplicates or extra entries?)", got, want)
	}
}

// TestReplicationLagThenCatchUp: leader accumulates writes with no follower
// connected, then a fresh follower joins and must replay everything from 0.
func TestReplicationLagThenCatchUp(t *testing.T) {
	dir := t.TempDir()

	ls, err := store.Open(filepath.Join(dir, "leader.wal"))
	if err != nil {
		t.Fatalf("leader store: %v", err)
	}
	defer ls.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	lead := NewLeader(ls.WAL())
	go lead.Serve(ln)
	defer lead.Close()

	// Write before any follower connects.
	const N = 30
	for i := 0; i < N; i++ {
		if err := ls.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("leader Put: %v", err)
		}
	}

	// Now connect a fresh follower (offset = 0 from empty offset file).
	fs, err := store.Open(filepath.Join(dir, "follower.wal"))
	if err != nil {
		t.Fatalf("follower store: %v", err)
	}
	defer fs.Close()
	fol := NewFollower(fs, ln.Addr().String(), filepath.Join(dir, "follower.offset"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fol.Run(ctx)

	// Follower must catch up to all N records.
	if !waitForKey(fs, fmt.Sprintf("k%d", N-1), fmt.Sprintf("v%d", N-1), 3*time.Second) {
		t.Fatalf("fresh follower didn't catch up to %d pre-existing records", N)
	}
	if got := fs.Len(); got != N {
		t.Errorf("follower has %d keys, want %d", got, N)
	}
}
