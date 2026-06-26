package server

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"kvengine/internal/store"
)

// BenchmarkPutGetOverTCP measures realistic end-to-end throughput: a real
// TCP connection (loopback), real protocol framing, real fsync-backed
// Store underneath. This is the number to compare against a known server
// (e.g. redis-benchmark) per the Phase 3 roadmap item — run this on real
// hardware, not just the sandbox, before drawing conclusions.
func BenchmarkPutGetOverTCP(b *testing.B) {
	walPath := filepath.Join(b.TempDir(), "bench.wal")
	s, err := store.Open(walPath)
	if err != nil {
		b.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	srv := New(s)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(conn, "PUT benchkey 9\r\nbenchval1\r\n")
		if _, err := r.ReadString('\n'); err != nil {
			b.Fatalf("put response: %v", err)
		}
		fmt.Fprintf(conn, "GET benchkey\r\n")
		if _, err := r.ReadString('\n'); err != nil { // length line
			b.Fatalf("get length: %v", err)
		}
		buf := make([]byte, 9)
		if _, err := readFull(r, buf); err != nil {
			b.Fatalf("get value: %v", err)
		}
		r.ReadString('\n') // trailing terminator
	}
}
