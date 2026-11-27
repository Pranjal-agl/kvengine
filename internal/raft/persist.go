package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"kvengine/internal/wal"
)

// Durable Raft state lives in two files (§5.4 of the paper says what must
// survive a crash: currentTerm, votedFor and the log):
//
//   - statePath (JSON): currentTerm and votedFor. Small, and only changes
//     on elections, so it is rewritten atomically (tmp → fsync → rename →
//     fsync dir) each time.
//   - statePath + ".log": the log, append-only, built on internal/wal (so
//     records are checksummed and a torn tail from a crash is cut off on
//     open). Appending entries costs one write + one fsync, no matter how
//     long the log is. The file is only rewritten when a snapshot compacts
//     the log.
//
// Log file records (wal.Record, integers little-endian):
//
//	base:  Type=RecordDelete  Key=[8]index  Value=[8]term
//	       The log starts at index (the snapshot sentinel). Always the first
//	       record of a rewritten file.
//	entry: Type=RecordPut     Key=[8]index  Value=[8]term [1]hasCommand command
//	       Writing entry i discards any entries >= i first — that is how a
//	       follower's truncation of a conflicting suffix is recorded.
//
// Older versions kept the whole log inside the JSON file; those files are
// migrated on first start.

// persistentState is the JSON file. Log and LogStart appear only in files
// written by older versions.
type persistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	LogStart    *uint64    `json:"log_start,omitempty"`
	Log         []LogEntry `json:"log,omitempty"`
}

func (n *Node) logPath() string { return n.statePath + ".log" }

// The persist functions must be called with n.mu held, BEFORE replying to
// the RPC (or sending the RPCs) that depend on the change. A node that
// cannot persist cannot safely vote or acknowledge entries, so failures
// panic (as etcd/raft does for storage errors) rather than letting the node
// carry on with state that would be lost on restart. All are no-ops for
// in-memory nodes (statePath "") and after Stop.

// persistMeta durably records currentTerm and votedFor.
func (n *Node) persistMeta() {
	if n.statePath == "" || n.stopped() {
		return
	}
	data, err := json.Marshal(persistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
	if err != nil {
		panic(fmt.Sprintf("raft: persist meta: %v", err))
	}
	if err := writeFileAtomic(n.statePath, data); err != nil {
		panic(fmt.Sprintf("raft: persist meta: %v", err))
	}
}

// persistEntries durably appends the log entries from index `from` to the
// end of the log. Entries already on disk at >= from are superseded. If
// earlier entries were never made durable (a leader's unflushed proposals),
// writing starts from those instead, so the file never has gaps.
func (n *Node) persistEntries(from uint64) {
	if n.statePath == "" || n.stopped() {
		return
	}
	if from > n.durableIndex+1 {
		from = n.durableIndex + 1
	}
	if from <= n.snapshotIndex {
		from = n.snapshotIndex + 1
	}
	for i := from; i <= n.lastIndex(); i++ {
		if err := n.logw.Append(entryRecord(i, n.entryAt(i))); err != nil {
			panic(fmt.Sprintf("raft: persist entries: %v", err))
		}
	}
	if err := n.logw.Sync(); err != nil {
		panic(fmt.Sprintf("raft: persist entries: %v", err))
	}
	n.durableIndex = n.lastIndex()
}

// rewriteLog replaces the log file with the current in-memory log (base
// record + entries), atomically. Used after compaction.
func (n *Node) rewriteLog() {
	if n.statePath == "" || n.stopped() {
		return
	}
	if err := n.writeLogFile(); err != nil {
		panic(fmt.Sprintf("raft: rewrite log: %v", err))
	}
}

func (n *Node) writeLogFile() error {
	path := n.logPath()
	tmp := path + ".tmp"
	os.Remove(tmp)
	w, err := wal.Open(tmp)
	if err != nil {
		return err
	}
	err = w.Append(baseRecord(n.snapshotIndex, n.snapshotTerm))
	for i := n.snapshotIndex + 1; err == nil && i <= n.lastIndex(); i++ {
		err = w.Append(entryRecord(i, n.entryAt(i)))
	}
	if err == nil {
		err = w.Sync()
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if n.logw != nil {
		n.logw.Close()
	}
	n.logw, err = wal.Open(path)
	if err == nil {
		n.durableIndex = n.lastIndex()
	}
	return err
}

func baseRecord(index, term uint64) wal.Record {
	return wal.Record{Type: wal.RecordDelete, Key: u64(index), Value: u64(term)}
}

func entryRecord(index uint64, e LogEntry) wal.Record {
	v := make([]byte, 9, 9+len(e.Command))
	binary.LittleEndian.PutUint64(v, e.Term)
	if e.Command != nil {
		v[8] = 1
		v = append(v, e.Command...)
	}
	return wal.Record{Type: wal.RecordPut, Key: u64(index), Value: v}
}

func u64(x uint64) []byte { return binary.LittleEndian.AppendUint64(nil, x) }

// replayLog rebuilds (start, log) from log file records.
func replayLog(recs []wal.Record) (start uint64, log []LogEntry, err error) {
	for i, r := range recs {
		if len(r.Key) != 8 || len(r.Value) < 8 {
			return 0, nil, fmt.Errorf("raft: log record %d malformed", i)
		}
		idx := binary.LittleEndian.Uint64(r.Key)
		term := binary.LittleEndian.Uint64(r.Value)
		switch r.Type {
		case wal.RecordDelete: // base
			start, log = idx, []LogEntry{{Term: term}}
		case wal.RecordPut: // entry
			if log == nil {
				return 0, nil, errors.New("raft: log file has entries before its base record")
			}
			if idx <= start || idx > start+uint64(len(log)) {
				return 0, nil, fmt.Errorf("raft: log record %d has index %d outside [%d, %d]",
					i, idx, start+1, start+uint64(len(log)))
			}
			if len(r.Value) < 9 {
				return 0, nil, fmt.Errorf("raft: log record %d malformed", i)
			}
			e := LogEntry{Term: term}
			if r.Value[8] == 1 {
				e.Command = append([]byte{}, r.Value[9:]...)
			}
			log = append(log[:idx-start], e)
		default:
			return 0, nil, fmt.Errorf("raft: log record %d has unknown type %d", i, r.Type)
		}
	}
	return start, log, nil
}

// restoreState loads term, vote and log from disk. Called once during
// NewNode, after the snapshot (if any) has set snapshotIndex/snapshotTerm.
// Missing files mean a fresh node.
func (n *Node) restoreState() error {
	if n.statePath == "" {
		return nil
	}
	var ps persistentState
	data, err := os.ReadFile(n.statePath)
	switch {
	case os.IsNotExist(err):
	case err != nil:
		return fmt.Errorf("raft: read state: %w", err)
	default:
		if err := json.Unmarshal(data, &ps); err != nil {
			return fmt.Errorf("raft: decode state: %w", err)
		}
	}
	n.currentTerm, n.votedFor = ps.CurrentTerm, ps.VotedFor

	logw, recs, err := wal.OpenAndReplay(n.logPath())
	if err != nil {
		return fmt.Errorf("raft: open log: %w", err)
	}
	n.logw = logw

	var start uint64
	var log []LogEntry
	legacy := len(ps.Log) > 0
	if legacy {
		start, log = n.snapshotIndex, ps.Log // old files: log[0] was the snapshot sentinel
		if ps.LogStart != nil {
			start = *ps.LogStart
		}
	} else if len(recs) > 0 {
		if start, log, err = replayLog(recs); err != nil {
			return err
		}
	}

	if log != nil {
		switch {
		case start == n.snapshotIndex:
			n.log = log
		case start < n.snapshotIndex:
			// Snapshot is newer than the log (crash between writing the
			// two). Keep only the entries after the snapshot, and only if
			// the log agrees with the snapshot at its last index.
			pos := n.snapshotIndex - start
			n.log = []LogEntry{{Term: n.snapshotTerm}}
			if pos < uint64(len(log)) && log[pos].Term == n.snapshotTerm {
				n.log = append(n.log, log[pos+1:]...)
			}
		default:
			return fmt.Errorf("raft: persisted log starts at %d but snapshot ends at %d", start, n.snapshotIndex)
		}
		n.log[0].Command = nil
	}

	// Make the file match memory exactly when it doesn't already: a fresh
	// file needs its base record, a migrated legacy log needs moving out
	// of the JSON, and a log trimmed to a newer snapshot must not keep
	// entries that conflict with it.
	if len(recs) == 0 || legacy || start != n.snapshotIndex {
		if err := n.writeLogFile(); err != nil {
			return fmt.Errorf("raft: write log: %w", err)
		}
	}
	n.durableIndex = n.lastIndex()
	if legacy {
		data, _ := json.Marshal(persistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor})
		if err := writeFileAtomic(n.statePath, data); err != nil {
			return fmt.Errorf("raft: migrate state: %w", err)
		}
	}
	return nil
}

// writeFileAtomic writes data to path via tmp → fsync → rename → fsync dir.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}
