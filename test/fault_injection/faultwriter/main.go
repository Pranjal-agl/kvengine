// faultwriter is a standalone helper process for the fault-injection test
// in fault_test.go. It is NOT a unit test itself — it's invoked as a
// subprocess that gets killed (SIGKILL) at an unpredictable point, so the
// parent test can verify that internal/wal.Replay correctly recovers
// whatever prefix of writes made it to disk.
//
// Usage: faultwriter <wal-path> <num-records>
// Writes records key-0000, key-0001, ... sequentially, fsyncing after each,
// and prints "WROTE <n>" to stdout after each successful fsync so the
// parent test knows how far it got before being killed.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"kvengine/internal/wal"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: faultwriter <wal-path> <num-records>")
		os.Exit(2)
	}
	path := os.Args[1]
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad num-records:", err)
		os.Exit(2)
	}

	w, err := wal.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer w.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		if err := w.Append(wal.Record{Type: wal.RecordPut, Key: []byte(key), Value: []byte(val)}); err != nil {
			fmt.Fprintln(os.Stderr, "append:", err)
			os.Exit(1)
		}
		if err := w.Sync(); err != nil {
			fmt.Fprintln(os.Stderr, "sync:", err)
			os.Exit(1)
		}
		// Unbuffered, flushed immediately so the parent can read progress
		// in real time and decide when to kill us.
		fmt.Printf("WROTE %d\n", i)

		// Small pacing delay so the parent's randomized kill timing can
		// land anywhere across the full stream (start, middle, end) rather
		// than always within the first handful of records, which would
		// happen if all 300 writes completed faster than the parent's
		// kill-delay resolution.
		time.Sleep(200 * time.Microsecond)
	}
}
