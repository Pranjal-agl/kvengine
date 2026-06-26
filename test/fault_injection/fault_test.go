// Package faultinjection proves the "data survives a crash" must-have for
// real, by repeatedly killing a live writer process with SIGKILL at random
// points and checking that internal/wal.Replay recovers exactly the
// fsynced prefix — no acknowledged write lost, no corruption admitted.
//
// This complements (does not replace) the unit-level
// internal/wal.TestReplayTornTail, which hand-crafts a single torn write.
// This test proves the same property against a real OS-level kill of a
// real process, across many randomized trials.
package faultinjection

import (
	"bufio"
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"kvengine/internal/wal"
)

func TestFaultInjectionTornWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("fault injection spawns many subprocesses; skipped with -short")
	}

	bin := filepath.Join(t.TempDir(), "faultwriter")
	build := exec.Command("go", "build", "-o", bin, "./faultwriter")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build faultwriter helper: %v\n%s", err, out)
	}

	const trials = 15
	const recordsPerTrial = 300

	for trial := 0; trial < trials; trial++ {
		walPath := filepath.Join(t.TempDir(), fmt.Sprintf("trial-%d.wal", trial))

		cmd := exec.Command(bin, walPath, fmt.Sprintf("%d", recordsPerTrial))
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("trial %d: stdout pipe: %v", trial, err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("trial %d: start: %v", trial, err)
		}

		lastAcked := -1
		done := make(chan struct{})
		go func() {
			defer close(done)
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				var n int
				if _, err := fmt.Sscanf(scanner.Text(), "WROTE %d", &n); err == nil {
					lastAcked = n
				}
			}
		}()

		// Random delay so the kill lands at a different, unpredictable point
		// in the write stream on each trial — sometimes very early,
		// sometimes mid-stream, sometimes near/after completion. Paired with
		// the 200us per-write pacing in faultwriter, 300 records take ~60ms
		// total, so a 0-70ms range covers the whole stream including a few
		// trials where the writer finishes cleanly before being killed.
		time.Sleep(time.Duration(rand.Intn(70)) * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_ = cmd.Wait() // expected to report a kill signal exit; error ignored
		<-done

		records, err := wal.Replay(walPath)
		if err != nil {
			t.Fatalf("trial %d: Replay returned an error; it should always tolerate a torn tail, got: %v", trial, err)
		}

		// Durability: every write the process confirmed (printed WROTE for,
		// meaning fsync had already returned) must be present after replay.
		if lastAcked >= 0 && len(records) < lastAcked+1 {
			t.Fatalf("trial %d: lost an acknowledged write — process confirmed index %d, replay only recovered %d records",
				trial, lastAcked, len(records))
		}

		// Correctness: recovered records must be an exact, gap-free,
		// uncorrupted prefix in original write order.
		for i, rec := range records {
			wantKey := fmt.Sprintf("key-%04d", i)
			wantVal := fmt.Sprintf("val-%04d", i)
			if string(rec.Key) != wantKey || string(rec.Value) != wantVal {
				t.Fatalf("trial %d: record %d corrupted or out of order: got key=%q val=%q, want key=%q val=%q",
					trial, i, rec.Key, rec.Value, wantKey, wantVal)
			}
		}

		t.Logf("trial %d: acked=%d recovered=%d/%d", trial, lastAcked+1, len(records), recordsPerTrial)
	}
}
