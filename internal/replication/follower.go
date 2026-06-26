package replication

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"kvengine/internal/store"
	"kvengine/internal/wal"
)

// retryDelay is how long the follower waits before reconnecting after a
// disconnect. Kept short here (mostly for tests); a real deployment would
// use exponential backoff.
const retryDelay = 100 * time.Millisecond

// Follower connects to a Leader, receives its WAL byte stream, decodes each
// record, and applies it to a local Store — giving the follower a durable,
// crash-recoverable copy of the leader's state.
//
// Offset persistence: after applying each record the follower writes its
// current leader-stream byte offset to offsetPath (atomically: write tmp →
// fsync → rename). On crash + restart, it reads this file and resumes from
// that point — so no acknowledged record is applied twice, and no record
// applied before the crash is lost (it's already in the follower's own WAL
// via store.Put/Delete).
//
// Idempotency: the Store's Put/Delete are already idempotent by key (later
// writes win), and the offset file ensures we never re-apply old records
// after a restart, so double-application is not a concern in practice.
type Follower struct {
	store      *store.Store
	leaderAddr string
	offsetPath string // path to the persisted leader-stream byte offset file
}

func NewFollower(s *store.Store, leaderAddr, offsetPath string) *Follower {
	return &Follower{store: s, leaderAddr: leaderAddr, offsetPath: offsetPath}
}

// Run connects to the leader and applies records until ctx is cancelled.
// On disconnect it retries after retryDelay. This is the main entry point —
// call it in a goroutine; it returns when ctx.Done() is closed.
func (f *Follower) Run(ctx context.Context) error {
	for {
		err := f.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err() // clean shutdown
		}
		if err != nil {
			// transient error (disconnect, refused connection, etc) — retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}
}

// runOnce handles one connection to the leader: send offset, read records,
// apply them. Returns on any error (caller retries).
func (f *Follower) runOnce(ctx context.Context) error {
	offset, err := f.readOffset()
	if err != nil {
		return fmt.Errorf("follower: read offset: %w", err)
	}

	conn, err := net.DialTimeout("tcp", f.leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("follower: dial: %w", err)
	}
	defer conn.Close()

	// Terminate on ctx cancellation by closing the conn from another goroutine.
	// This unblocks any pending Read on conn so runOnce can return promptly.
	stopConn := make(chan struct{})
	defer close(stopConn)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-stopConn:
		}
	}()

	// Send our current offset to the leader so it knows where to resume.
	if err := binary.Write(conn, binary.LittleEndian, offset); err != nil {
		return fmt.Errorf("follower: send offset: %w", err)
	}

	r := bufio.NewReader(conn)
	for {
		rec, frameSize, err := wal.ReadRecord(r)
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("follower: decode record: %w", err)
		}

		// Apply to the local store. store.Put/Delete writes to the
		// follower's own WAL (for follower crash recovery) and updates
		// the in-memory map. This is the same path a local writer would
		// take — the follower's store is crash-safe independently.
		switch rec.Type {
		case wal.RecordPut:
			if err := f.store.Put(rec.Key, rec.Value); err != nil {
				return fmt.Errorf("follower: apply put: %w", err)
			}
		case wal.RecordDelete:
			if err := f.store.Delete(rec.Key); err != nil {
				return fmt.Errorf("follower: apply delete: %w", err)
			}
		}

		offset += frameSize
		if err := f.writeOffset(offset); err != nil {
			return fmt.Errorf("follower: persist offset: %w", err)
		}
	}
}

// readOffset reads the persisted leader-stream offset from offsetPath.
// Returns 0 if the file doesn't exist (fresh follower, start from beginning).
func (f *Follower) readOffset() (int64, error) {
	file, err := os.Open(f.offsetPath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var offset int64
	if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
		return 0, err
	}
	return offset, nil
}

// writeOffset atomically persists offset to offsetPath using the
// write-tmp → fsync → rename pattern, so the file is never half-written —
// a crash mid-write leaves the old offset file intact rather than a
// corrupted one.
func (f *Follower) writeOffset(offset int64) error {
	tmp := f.offsetPath + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	file.Close()
	return os.Rename(tmp, f.offsetPath)
}
