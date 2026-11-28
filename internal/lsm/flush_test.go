package lsm

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// memState reports whether a flush is pending and the MemTable's size.
func memState(e *Engine) (imm bool, mem int) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.imm != nil, e.memtable.size
}

// Writes carry on while a flush is in progress: the full MemTable is set
// aside and served to readers, and only the *next* full MemTable waits.
func TestWritesDontWaitForFlush(t *testing.T) {
	g := newGate()
	opts := tiny()
	opts.flushHook = g.hook
	e, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer g.release()

	// Fill the first MemTable; the flush starts and blocks.
	i := 0
	for ; ; i++ {
		if err := e.Put([]byte(fmt.Sprintf("a-%05d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if imm, _ := memState(e); imm {
			break
		}
	}
	waitFor(t, func() bool { return g.entered.Load() > 0 }, "the flush to start")

	// Writes go on into the new MemTable, without waiting for the flush.
	done := make(chan error, 1)
	go func() {
		for j := 0; ; j++ {
			if _, mem := memState(e); mem > opts.MemTableSize*3/4 {
				done <- nil
				return
			}
			if err := e.Put([]byte(fmt.Sprintf("b-%05d", j)), []byte("v")); err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writes blocked behind a flush in progress")
	}
	// Reads find data in the MemTable being flushed.
	if v, ok := e.Get([]byte("a-00000")); !ok || string(v) != "v" {
		t.Fatalf("read from the immutable MemTable: (%q, %v)", v, ok)
	}

	// Filling the second MemTable has to wait for the first flush.
	big := make([]byte, opts.MemTableSize)
	putDone := make(chan error, 1)
	go func() { putDone <- e.Put([]byte("big"), big) }()
	select {
	case err := <-putDone:
		t.Fatalf("second full MemTable didn't wait for the first flush (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	g.release()
	if err := <-putDone; err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	s := e.Stats()
	if s["flushes"].(int64) < 2 || s["write_stalls"].(int64) < 1 || s["immutable_bytes"].(int) != 0 {
		t.Fatalf("stats after: %v", s)
	}
	for j := 0; j < i; j++ {
		if _, ok := e.Get([]byte(fmt.Sprintf("a-%05d", j))); !ok {
			t.Fatalf("a-%05d lost", j)
		}
	}
}

// A failing flush reaches writers as an error, not a hang; once flushing
// works again, so do writes. Nothing is lost across a Close that couldn't
// flush.
func TestFlushErrorReachesWriters(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	dir := t.TempDir()
	opts := tiny()
	opts.flushHook = func() error {
		if fail.Load() {
			return errors.New("disk full")
		}
		return nil
	}
	e, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	var werr error
	n := 0
	for ; n < 100000 && werr == nil; n++ {
		werr = e.Put([]byte(fmt.Sprintf("k%06d", n)), []byte("vvvvvvvvvvvvvvvv"))
	}
	if werr == nil {
		t.Fatal("writes never reported the failing flush")
	}
	t.Logf("write error after %d writes: %v", n, werr)
	if s := e.Stats(); s["flush_error"] != "disk full" {
		t.Fatalf("flush_error = %q", s["flush_error"])
	}
	if err := e.Flush(); err == nil {
		t.Fatal("Flush succeeded while flushing fails")
	}
	if err := e.Close(); err == nil {
		t.Fatal("Close reported success though it couldn't flush")
	}

	// Everything acknowledged (and the failed write too: it was durable) is
	// recovered from the WALs.
	e, err = OpenWithOptions(dir, tiny())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < n; i++ {
		if _, ok := e.Get([]byte(fmt.Sprintf("k%06d", i))); !ok {
			t.Fatalf("k%06d lost", i)
		}
	}
}

// Recovery after an error clears: writes go on in the same engine.
func TestFlushRecoversAfterError(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	opts := tiny()
	opts.flushHook = func() error {
		if fail.Load() {
			return errors.New("disk full")
		}
		return nil
	}
	e, err := OpenWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; ; i++ {
		if e.Put([]byte(fmt.Sprintf("k%06d", i)), []byte("vvvvvvvvvvvvvvvv")) != nil {
			break
		}
	}
	fail.Store(false)
	waitFor(t, func() bool { imm, _ := memState(e); return !imm }, "the flush to succeed on retry")
	if err := e.Put([]byte("after"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if s := e.Stats(); s["flush_error"] != "" {
		t.Fatalf("flush_error still %q", s["flush_error"])
	}
}

// A crash while a flush is in progress loses nothing: the MemTable being
// flushed is still in its WAL files.
func TestCrashDuringFlush(t *testing.T) {
	g := newGate()
	dir := t.TempDir()
	opts := tiny()
	opts.flushHook = g.hook
	e, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ; ; n++ {
		e.Put([]byte(fmt.Sprintf("k%05d", n)), []byte("v"))
		if imm, _ := memState(e); imm {
			break
		}
	}
	for j := 0; j < 50; j++ { // some in the new MemTable too
		n++
		e.Put([]byte(fmt.Sprintf("k%05d", n)), []byte("v"))
	}
	waitFor(t, func() bool { return g.entered.Load() > 0 }, "the flush to start")
	crashed := make(chan struct{})
	go func() { crash(e); close(crashed) }()
	time.Sleep(20 * time.Millisecond)
	g.release() // the flusher writes its SSTable, then sees it's stopping
	<-crashed

	e, err = OpenWithOptions(dir, tiny())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i <= n; i++ {
		if _, ok := e.Get([]byte(fmt.Sprintf("k%05d", i))); !ok {
			t.Fatalf("k%05d lost in a crash during a flush", i)
		}
	}
}
