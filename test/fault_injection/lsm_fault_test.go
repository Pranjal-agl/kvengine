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

	"kvengine/internal/lsm"
)

// opAt must match op() in lsmwriter/main.go.
func opAt(i int) (key, val string, del bool) {
	return fmt.Sprintf("key-%03d", i%400), fmt.Sprintf("val-%04d", i), i%7 == 3
}

// model returns the expected state after the first n operations.
func model(n int) map[string]string {
	m := map[string]string{}
	for i := 0; i < n; i++ {
		k, v, del := opAt(i)
		if del {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return m
}

// TestLSMFaultInjection kills a process writing to the LSM engine at random
// points — including mid-flush and mid-compaction, since the MemTable is
// tiny — and checks that every acknowledged write survived and nothing else
// changed. The recovered state must equal the model after exactly the
// acknowledged operations, or after one more (the op in flight when the
// kill landed may or may not have reached the WAL).
func TestLSMFaultInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped with -short")
	}
	bin := filepath.Join(t.TempDir(), "lsmwriter")
	if out, err := exec.Command("go", "build", "-o", bin, "./lsmwriter").CombinedOutput(); err != nil {
		t.Fatalf("build lsmwriter: %v\n%s", err, out)
	}

	const trials, ops = 15, 2000
	for trial := 0; trial < trials; trial++ {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("trial-%d", trial))
		cmd := exec.Command(bin, dir, fmt.Sprint(ops))
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		acked := -1
		done := make(chan struct{})
		go func() {
			defer close(done)
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				fmt.Sscanf(sc.Text(), "DONE %d", &acked)
			}
		}()
		time.Sleep(time.Duration(rand.Intn(450)) * time.Millisecond)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		<-done

		e, err := lsm.OpenWithOptions(dir, lsm.Options{MemTableSize: 2048, TargetFileSize: 1024, LevelSizeBase: 4096})
		if err != nil {
			t.Fatalf("trial %d: recovery failed: %v", trial, err)
		}
		matches := func(n int) bool {
			want := model(n)
			for i := 0; i < 400; i++ {
				k := fmt.Sprintf("key-%03d", i)
				v, ok := e.Get([]byte(k))
				wv, wok := want[k]
				if ok != wok || string(v) != wv {
					return false
				}
			}
			return true
		}
		if !matches(acked+1) && !matches(acked+2) {
			e.Close()
			t.Fatalf("trial %d: recovered state matches neither %d nor %d ops (acked through %d)",
				trial, acked+1, acked+2, acked)
		}
		stats := e.Stats()
		e.Close()
		t.Logf("trial %d: acked %d ops, recovered OK (%v)", trial, acked+1, stats)
	}
}
