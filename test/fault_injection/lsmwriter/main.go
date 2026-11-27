// lsmwriter is a helper process for TestLSMFaultInjection: it applies a
// deterministic stream of puts and deletes to an LSM engine (with a tiny
// MemTable, so flushes and compactions happen constantly) and prints
// "DONE <i>" after operation i returns. The parent kills it with SIGKILL at
// a random point and checks what survived.
//
// Usage: lsmwriter <dir> <num-ops>
package main

import (
	"fmt"
	"os"
	"strconv"

	"kvengine/internal/lsm"
)

// Op returns operation i of the stream; shared with the test via
// identical code (keep in sync with opAt in fault_test.go).
func op(i int) (key string, val string, del bool) {
	return fmt.Sprintf("key-%02d", i%40), fmt.Sprintf("val-%04d", i), i%7 == 3
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: lsmwriter <dir> <num-ops>")
		os.Exit(2)
	}
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		os.Exit(2)
	}
	e, err := lsm.OpenWithOptions(os.Args[1], lsm.Options{MemTableSize: 2048})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	for i := 0; i < n; i++ {
		k, v, del := op(i)
		if del {
			err = e.Delete([]byte(k))
		} else {
			err = e.Put([]byte(k), []byte(v))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
		fmt.Printf("DONE %d\n", i)
	}
}
