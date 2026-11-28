// Package wal implements an append-only write-ahead log: the durability
// foundation for internal/store. See docs/ARCHITECTURE.md for the on-disk
// record format and crash recovery semantics before changing this file.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// RecordType distinguishes the kind of mutation a log record represents.
type RecordType uint8

const (
	RecordPut RecordType = iota + 1
	RecordDelete
)

// Record is one logical mutation: a Put (Key, Value) or a Delete (Key only).
type Record struct {
	Type  RecordType
	Key   []byte
	Value []byte
}

// WAL is an append-only log file. Safe for concurrent use: Append and Sync
// are both guarded by mu, so concurrent writers are serialized into a single
// total order on disk (required — the log's order *is* the source of truth).
//
// Phase 4 additions: durableOff tracks the byte position of the end of the
// last fsynced record, and notify is a channel that's closed+replaced each
// time durableOff advances. This lets replication's StreamFrom block
// efficiently on "no new data" without polling.
type WAL struct {
	mu           sync.Mutex
	file         *os.File
	w            *bufio.Writer
	durableOff   int64         // end of last fsynced record; safe to stream up to
	pendingBytes int64         // bytes appended but not yet fsynced
	notify       chan struct{} // closed when durableOff advances; replaced with a new chan
}

// Open opens (creating if necessary) the log file at path for appending.
//
// If the file ends in a torn or corrupt record (the normal aftermath of a
// crash mid-append), Open truncates the file back to the end of the last
// valid record before appending anything. Without this, new records would
// be written *after* the garbage, and the next Replay — which stops at the
// first bad record — would silently drop every one of them. The discarded
// bytes are copied to "<path>.corrupt-<offset>" first, so nothing is
// destroyed if the damage turns out to be mid-file bit rot rather than a
// torn tail.
//
// durableOff is initialised to the (possibly truncated) file size so that
// an existing log is immediately streamable to replication followers.
func Open(path string) (*WAL, error) {
	w, _, err := open(path, false)
	return w, err
}

// OpenAndReplay is Open plus Replay in a single pass over the file: it
// returns every valid record (for rebuilding in-memory state) along with a
// WAL ready for appending. Prefer this over Replay+Open, which reads the
// file twice.
func OpenAndReplay(path string) (*WAL, []Record, error) {
	return open(path, true)
}

func open(path string, collect bool) (*WAL, []Record, error) {
	records, validEnd, size, err := scan(path, collect)
	if err != nil {
		return nil, nil, err
	}
	if validEnd < size {
		if err := quarantineTail(path, validEnd); err != nil {
			return nil, nil, err
		}
	}

	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	if created {
		// A new file's directory entry is only durable once the directory
		// itself is fsynced; otherwise a crash could lose the whole log.
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	return &WAL{
		file:       f,
		w:          bufio.NewWriter(f),
		durableOff: validEnd,
		notify:     make(chan struct{}),
	}, records, nil
}

// scan reads path from the start and returns the valid records (if
// collect), the byte offset just past the last valid record, and the file
// size. A missing file is an empty log.
func scan(path string, collect bool) (records []Record, validEnd, size int64, err error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, 0, 0, nil
	}
	if err != nil {
		return nil, 0, 0, fmt.Errorf("wal: open for scan: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("wal: stat: %w", err)
	}
	r := bufio.NewReader(f)
	for {
		rec, n, err := ReadRecord(r)
		if err != nil {
			break
		}
		validEnd += n
		if collect {
			records = append(records, rec)
		}
	}
	return records, validEnd, info.Size(), nil
}

// quarantineTail copies the bytes after validEnd to a side file, fsyncs it,
// then truncates the log to validEnd and fsyncs the log.
func quarantineTail(path string, validEnd int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("wal: open for truncate: %w", err)
	}
	defer f.Close()

	sidePath := fmt.Sprintf("%s.corrupt-%d", path, validEnd)
	side, err := os.Create(sidePath)
	if err != nil {
		return fmt.Errorf("wal: create %s: %w", sidePath, err)
	}
	if _, err := io.Copy(side, io.NewSectionReader(f, validEnd, 1<<62)); err != nil {
		side.Close()
		return fmt.Errorf("wal: save corrupt tail: %w", err)
	}
	if err := side.Sync(); err != nil {
		side.Close()
		return fmt.Errorf("wal: fsync corrupt tail: %w", err)
	}
	side.Close()

	if err := f.Truncate(validEnd); err != nil {
		return fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync after truncate: %w", err)
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: fsync dir: %w", err)
	}
	return nil
}

// encode frames a record as:
//
//	[4]byte CRC32   (of body)
//	[4]byte Length  (of body)
//	body: [1]Type [4]KeyLen Key [4]ValLen Value
//
// All integers little-endian. See docs/ARCHITECTURE.md for the rationale.
func encode(rec Record) []byte {
	body := make([]byte, 0, 1+4+len(rec.Key)+4+len(rec.Value))
	body = append(body, byte(rec.Type))
	body = appendUint32(body, uint32(len(rec.Key)))
	body = append(body, rec.Key...)
	body = appendUint32(body, uint32(len(rec.Value)))
	body = append(body, rec.Value...)

	crc := crc32.ChecksumIEEE(body)
	out := make([]byte, 0, 8+len(body))
	out = appendUint32(out, crc)
	out = appendUint32(out, uint32(len(body)))
	out = append(out, body...)
	return out
}

func appendUint32(b []byte, v uint32) []byte {
	return binary.LittleEndian.AppendUint32(b, v)
}

// Append writes rec to the in-process buffer. Not durable until Sync.
func (w *WAL) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := encode(rec)
	if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("wal: append: %w", err)
	}
	w.pendingBytes += int64(len(b))
	return nil
}

// Sync flushes the buffered writer and fsyncs the underlying file, then
// advances durableOff and signals any waiting StreamFrom callers.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.durableOff += w.pendingBytes
	w.pendingBytes = 0
	// Close the old notify channel to wake all StreamFrom waiters, then
	// replace it so the next wait cycle works correctly.
	old := w.notify
	w.notify = make(chan struct{})
	close(old)
	return nil
}

// Flush hands buffered records to the OS without fsyncing. They survive
// a process crash but not a power failure, and are not yet streamable to
// replication followers (durableOff only advances on Sync).
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return nil
}

// DurableOffset returns the byte position of the end of the most recently
// fsynced record — the safe upper bound for replication streaming.
func (w *WAL) DurableOffset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.durableOff
}

// StreamFrom copies raw WAL bytes starting at fromOffset to dst,
// blocking when caught up to the durable frontier and resuming each time
// Sync() advances it. Returns nil only when done is closed (clean shutdown).
// Returns an error if writing to dst fails (follower disconnected).
//
// Opens a separate read fd so it never interferes with the append fd.
func (w *WAL) StreamFrom(fromOffset int64, dst io.Writer, done <-chan struct{}) error {
	f, err := os.Open(w.file.Name())
	if err != nil {
		return fmt.Errorf("wal: StreamFrom open: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(fromOffset, io.SeekStart); err != nil {
		return fmt.Errorf("wal: StreamFrom seek: %w", err)
	}

	pos := fromOffset
	for {
		w.mu.Lock()
		target := w.durableOff
		ch := w.notify
		w.mu.Unlock()

		if pos < target {
			n, err := io.CopyN(dst, f, target-pos)
			pos += n
			if err != nil {
				return err // write to dst failed — follower gone
			}
			// Flush if dst is buffered (e.g. *bufio.Writer over a net.Conn).
			if flusher, ok := dst.(interface{ Flush() error }); ok {
				if err := flusher.Flush(); err != nil {
					return err
				}
			}
			continue
		}

		// Caught up — block until new data is fsynced or shutdown.
		select {
		case <-done:
			return nil
		case <-ch:
			// durableOff advanced; loop and copy the new bytes.
		}
	}
}

// Path returns the underlying file's name.
func (w *WAL) Path() string {
	return w.file.Name()
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("wal: close flush: %w", err)
	}
	return w.file.Close()
}

// Replay reads every valid record from the log at path, in append order.
// Returns (nil, nil) if path doesn't exist (fresh store). Stops at the
// first corrupt or truncated record — see docs/ARCHITECTURE.md for why.
// Replay is read-only; Open/OpenAndReplay are what repair a torn tail.
func Replay(path string) ([]Record, error) {
	records, _, _, err := scan(path, true)
	return records, err
}

// ReadRecord decodes one record from r, returning the record and the number
// of bytes consumed (= 8 + body length). Used by both Replay and the
// replication follower to decode the leader's raw byte stream.
// Returns io.EOF at a clean end of stream, io.ErrUnexpectedEOF for torn data.
func ReadRecord(r *bufio.Reader) (Record, int64, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, 0, err
	}
	crcWant := binary.LittleEndian.Uint32(header[0:4])
	length := binary.LittleEndian.Uint32(header[4:8])

	// Sanity cap: a single record larger than 32 MiB is almost certainly
	// corrupt (or a fuzz input). Treat it as a torn/invalid record.
	const maxBodySize = 32 << 20 // 32 MiB
	if length > maxBodySize {
		return Record{}, 0, fmt.Errorf("wal: record body too large (%d bytes)", length)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Record{}, 8, io.ErrUnexpectedEOF
	}
	if crc32.ChecksumIEEE(body) != crcWant {
		return Record{}, int64(8 + length), fmt.Errorf("wal: checksum mismatch")
	}
	if len(body) < 9 {
		return Record{}, int64(8 + length), fmt.Errorf("wal: malformed record body")
	}

	typ := RecordType(body[0])
	keyLen := binary.LittleEndian.Uint32(body[1:5])
	valLenOffset := 5 + int(keyLen)
	if valLenOffset+4 > len(body) {
		return Record{}, int64(8 + length), fmt.Errorf("wal: malformed key length")
	}
	key := body[5:valLenOffset]
	valLen := binary.LittleEndian.Uint32(body[valLenOffset : valLenOffset+4])
	valStart := valLenOffset + 4
	if valStart+int(valLen) > len(body) {
		return Record{}, int64(8 + length), fmt.Errorf("wal: malformed value length")
	}
	val := body[valStart : valStart+int(valLen)]

	return Record{
		Type:  typ,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), val...),
	}, int64(8 + length), nil
}
