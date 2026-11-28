package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"kvengine/internal/wal"
)

// Background flushing (the LevelDB design).
//
// A writer that fills the MemTable doesn't write the SSTable itself any
// more. It makes the MemTable immutable (imm), starts a new one with a new
// WAL file, and carries on; the flusher goroutine writes imm to an L0
// SSTable, installs it with a manifest edit, and only then deletes imm's
// WAL files. Reads check MemTable → imm → L0 → deeper levels.
//
// At most one immutable MemTable exists. If the next MemTable fills before
// the previous flush has finished, the writer waits (memory stays bounded
// at two MemTables); it also waits while L0 is at l0StopWrites, as before.
// If the flush keeps failing, the waiting writer gets the error rather
// than waiting forever.
//
// Crash safety is unchanged: every write is in a WAL before it is visible,
// a WAL file is deleted only after the SSTable holding its data is durable
// and in the manifest, and Open replays leftover WALs oldest first (imm's
// WALs are older than the current MemTable's).

// swapMemTable makes the current MemTable immutable and starts a new one
// with a fresh WAL, then wakes the flusher. An empty MemTable isn't kept;
// its WAL files are simply deleted. Called with wmu and mu held (or during
// Open), and only when imm is nil.
func (e *Engine) swapMemTable() error {
	if e.imm != nil {
		return errors.New("lsm: internal error: swapMemTable with a flush pending")
	}
	e.nextID++
	path := filepath.Join(e.dir, walName(e.nextID))
	w, err := wal.Open(path)
	if err != nil {
		return fmt.Errorf("lsm: open wal: %w", err)
	}
	if e.wal != nil {
		e.wal.Close() // its file stays until the flush is durable
	}
	if len(e.memtable.data) > 0 {
		e.imm, e.immWALs = e.memtable, e.walFiles
		e.memtable = newMemTable()
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	} else {
		for _, old := range e.walFiles {
			os.Remove(old) // nothing in them
		}
	}
	e.wal, e.walFiles = w, []string{path}
	return nil
}

// flushImm writes the immutable MemTable to an L0 SSTable, installs it, and
// deletes the WAL files it came from. Runs on the flusher goroutine (or in
// Open, before that starts), with no locks held on entry: imm is never
// modified, so it can be read unlocked.
func (e *Engine) flushImm() error {
	e.mu.Lock()
	imm, wals := e.imm, e.immWALs
	if imm == nil {
		e.mu.Unlock()
		return nil
	}
	e.nextID++
	id := e.nextID
	e.mu.Unlock()

	if e.opts.flushHook != nil {
		if err := e.opts.flushHook(); err != nil {
			return err
		}
	}
	sst, err := writeSSTable(e.dir, 0, id, imm.sorted())
	if err != nil {
		return err
	}
	e.bytesWritten.Add(sst.size)

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopping.Load() && !e.closed {
		// Abandoned (a test's simulated crash): leave it to recovery.
		return discard([]*SSTable{sst}, errFlushAborted)
	}
	if err := e.applyEdit([]*SSTable{sst}, nil); err != nil {
		return discard([]*SSTable{sst}, err)
	}
	e.imm, e.immWALs = nil, nil
	for _, f := range wals {
		os.Remove(f)
	}
	e.flushes.Add(1)
	e.bgCond.Broadcast()
	e.signalCompaction()
	return nil
}

var errFlushAborted = errors.New("lsm: flush abandoned: engine stopping")

// flushLoop is the flusher goroutine. A failed flush is retried after
// compactRetryDelay; the error is kept in flushErr until one succeeds.
func (e *Engine) flushLoop() {
	defer close(e.flushDone)
	for {
		select {
		case <-e.stopCh:
			return
		case <-e.flushCh:
		}
		for !e.stopping.Load() {
			err := e.flushImm()
			if errors.Is(err, errFlushAborted) {
				return
			}
			e.mu.Lock()
			e.flushErr = err
			e.bgCond.Broadcast()
			e.mu.Unlock()
			if err == nil {
				break
			}
			e.opts.Logf("lsm: background flush failed (retrying in %v): %v", compactRetryDelay, err)
			select {
			case <-e.stopCh:
				return
			case <-time.After(compactRetryDelay):
			}
		}
	}
}

// makeRoom is called by a writer (wmu held) whose MemTable is full: it
// waits until the previous flush is done and L0 is below l0StopWrites,
// then swaps the MemTable. If flushing or compaction is failing, it
// returns that error instead of waiting forever.
func (e *Engine) makeRoom() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	stalled := false
	for !e.closed && ((e.imm != nil && e.flushErr == nil) ||
		(len(e.levels[0]) >= l0StopWrites && e.bgErr == nil)) {
		if !stalled {
			e.writeStalls.Add(1)
			stalled = true
		}
		e.bgCond.Wait()
	}
	if e.imm != nil {
		return fmt.Errorf("lsm: the previous MemTable can't be flushed: %w", e.flushErr)
	}
	if len(e.levels[0]) >= l0StopWrites {
		return fmt.Errorf("lsm: L0 is full and compaction is failing: %w", e.bgErr)
	}
	return e.swapMemTable()
}

// flushAllLocked waits for a pending flush, then flushes the MemTable and
// waits for that too, so everything written so far is in SSTables. Called
// with wmu and mu held.
func (e *Engine) flushAllLocked() error {
	if err := e.waitImmLocked(); err != nil {
		return err
	}
	if len(e.memtable.data) == 0 {
		return nil
	}
	if err := e.swapMemTable(); err != nil {
		return err
	}
	return e.waitImmLocked()
}

// waitImmLocked waits until no flush is pending, or returns the error if
// the flush is failing. Called with mu held.
func (e *Engine) waitImmLocked() error {
	for e.imm != nil && e.flushErr == nil {
		e.bgCond.Wait()
	}
	if e.imm != nil {
		return fmt.Errorf("lsm: flush failed: %w", e.flushErr)
	}
	return nil
}
