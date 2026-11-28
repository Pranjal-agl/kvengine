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

	"kvengine/internal/store"
)

// TestStoreCompactionFaultInjection kills a writer whose WAL is compacted
// (rewritten and renamed into place) every few dozen writes. Whatever
// point the kill lands at — mid-append, mid-rewrite, around the rename —
// the reopened store must equal the model after exactly the acknowledged
// operations, or one more.
func TestStoreCompactionFaultInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns subprocesses; skipped with -short")
	}
	bin := filepath.Join(t.TempDir(), "storewriter")
	if out, err := exec.Command("go", "build", "-o", bin, "./storewriter").CombinedOutput(); err != nil {
		t.Fatalf("build storewriter: %v\n%s", err, out)
	}
	const trials, ops = 15, 2000
	for trial := 0; trial < trials; trial++ {
		path := filepath.Join(t.TempDir(), "s.wal")
		cmd := exec.Command(bin, path, fmt.Sprint(ops))
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

		s, err := store.Open(path)
		if err != nil {
			t.Fatalf("trial %d: recovery failed: %v", trial, err)
		}
		matches := func(n int) bool {
			want := model(n)
			if s.Len() != len(want) {
				return false
			}
			for k, wv := range want {
				if v, ok := s.Get([]byte(k)); !ok || string(v) != wv {
					return false
				}
			}
			return true
		}
		ok := matches(acked+1) || matches(acked+2)
		size := s.WALSize()
		s.Close()
		if !ok {
			t.Fatalf("trial %d: recovered state matches neither %d nor %d ops", trial, acked+1, acked+2)
		}
		t.Logf("trial %d: acked %d ops, recovered OK (WAL %d bytes)", trial, acked+1, size)
	}
}
