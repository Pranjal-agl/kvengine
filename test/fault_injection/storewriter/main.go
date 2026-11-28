// storewriter is a helper process for TestStoreCompactionFaultInjection:
// it applies the same deterministic put/delete stream as lsmwriter to a
// WAL-backed store with a tiny compaction threshold, so WAL rewrites (and
// their renames) are constantly in progress, printing "DONE <i>" after
// operation i returns.
//
// Usage: storewriter <wal-path> <num-ops>
package main

import (
	"fmt"
	"os"
	"strconv"

	"kvengine/internal/store"
)

// op must match opAt in lsm_fault_test.go.
func op(i int) (key, val string, del bool) {
	return fmt.Sprintf("key-%03d", i%400), fmt.Sprintf("val-%04d", i), i%7 == 3
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: storewriter <wal-path> <num-ops>")
		os.Exit(2)
	}
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		os.Exit(2)
	}
	s, err := store.OpenWithOptions(os.Args[1], store.Options{CompactMinBytes: 8 << 10, CompactRatio: 1.5})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		k, v, del := op(i)
		if del {
			err = s.Delete([]byte(k))
		} else {
			err = s.Put([]byte(k), []byte(v))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
		fmt.Printf("DONE %d\n", i)
	}
}
