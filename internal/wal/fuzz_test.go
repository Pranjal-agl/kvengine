package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// FuzzReadRecord feeds arbitrary bytes to the WAL record decoder. The
// decoder must never panic, infinite-loop, or allocate unbounded memory
// regardless of input. This covers torn writes, bit-flips, truncation,
// and malformed length/CRC fields — exactly the failure modes that occur
// in real crash scenarios.
func FuzzReadRecord(f *testing.F) {
	// Seed corpus: a valid Put record so the fuzzer starts from something
	// meaningful and mutates toward interesting edge cases.
	f.Add(validRecord(RecordPut, []byte("key"), []byte("value")))
	f.Add(validRecord(RecordDelete, []byte("k"), nil))

	// Interesting edge cases as additional seeds.
	f.Add([]byte{})                                               // empty
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})                         // zero CRC, no body
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) // max header
	f.Add(bytes.Repeat([]byte{0xAA}, 32))                         // garbage

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic under any input.
		r := bufio.NewReader(bytes.NewReader(data))
		ReadRecord(r) // return values ignored; we're testing for panics/hangs
	})
}

// FuzzWALAppendReplay fuzzes the full append→replay cycle: write
// arbitrary key/value pairs, close, replay. Must never corrupt state.
func FuzzWALAppendReplay(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("k"), bytes.Repeat([]byte{0xFF}, 256))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		// Enforce a max size to avoid OOM on huge allocations.
		if len(key) > 4096 || len(value) > 65536 {
			return // not t.Skip() — Skip panics inside fuzz callbacks
		}
		path := filepath.Join(t.TempDir(), "fuzz.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Record{Type: RecordPut, Key: key, Value: value}); err != nil {
			w.Close()
			return
		}
		if err := w.Sync(); err != nil {
			w.Close()
			return
		}
		w.Close()

		records, err := Replay(path)
		if err != nil {
			t.Fatalf("Replay returned error on valid write: %v", err)
		}
		if len(records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(records))
		}
		if string(records[0].Key) != string(key) || string(records[0].Value) != string(value) {
			t.Fatalf("round-trip mismatch: key=%q value=%q, got key=%q value=%q",
				key, value, records[0].Key, records[0].Value)
		}
		os.Remove(path)
	})
}

// validRecord builds a correctly encoded WAL frame for use as a fuzz seed.
func validRecord(typ RecordType, key, value []byte) []byte {
	body := []byte{byte(typ)}
	klen := make([]byte, 4)
	binary.LittleEndian.PutUint32(klen, uint32(len(key)))
	body = append(body, klen...)
	body = append(body, key...)
	vlen := make([]byte, 4)
	binary.LittleEndian.PutUint32(vlen, uint32(len(value)))
	body = append(body, vlen...)
	body = append(body, value...)

	crc := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc, crc32.ChecksumIEEE(body))
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(body)))

	var out []byte
	out = append(out, crc...)
	out = append(out, length...)
	out = append(out, body...)
	return out
}
