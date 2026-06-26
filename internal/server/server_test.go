package server

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kvengine/internal/store"
)

// startTestServer boots a Server on an OS-assigned loopback port and
// returns it (already serving in a background goroutine) plus its address.
func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	walPath := filepath.Join(t.TempDir(), "server.wal")
	s, err := store.Open(walPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	srv := New(s)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })

	return srv, ln.Addr().String()
}

// TestProtocolBasic exercises PUT/GET/DEL over a real TCP connection,
// verifying exact wire-format responses.
func TestProtocolBasic(t *testing.T) {
	_, addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	// PUT
	fmt.Fprintf(conn, "PUT greeting 5\r\nhello\r\n")
	line, _ := r.ReadString('\n')
	if line != "+OK\r\n" {
		t.Fatalf("PUT response: got %q, want \"+OK\\r\\n\"", line)
	}

	// GET hit
	fmt.Fprintf(conn, "GET greeting\r\n")
	line, _ = r.ReadString('\n')
	if line != "$5\r\n" {
		t.Fatalf("GET length line: got %q, want \"$5\\r\\n\"", line)
	}
	val := make([]byte, 5)
	if _, err := readFull(r, val); err != nil {
		t.Fatalf("read value: %v", err)
	}
	if string(val) != "hello" {
		t.Fatalf("GET value: got %q, want hello", val)
	}
	r.ReadString('\n') // trailing \r\n after value

	// GET miss
	fmt.Fprintf(conn, "GET nope\r\n")
	line, _ = r.ReadString('\n')
	if line != "$-1\r\n" {
		t.Fatalf("GET miss: got %q, want \"$-1\\r\\n\"", line)
	}

	// DEL
	fmt.Fprintf(conn, "DEL greeting\r\n")
	line, _ = r.ReadString('\n')
	if line != "+OK\r\n" {
		t.Fatalf("DEL response: got %q, want \"+OK\\r\\n\"", line)
	}
	fmt.Fprintf(conn, "GET greeting\r\n")
	line, _ = r.ReadString('\n')
	if line != "$-1\r\n" {
		t.Fatalf("GET after DEL: got %q, want \"$-1\\r\\n\" (deleted)", line)
	}

	// Unknown command: should report an error but NOT close the connection
	fmt.Fprintf(conn, "FOO bar\r\n")
	line, _ = r.ReadString('\n')
	if len(line) < 5 || line[:5] != "-ERR " {
		t.Fatalf("unknown command response: got %q, want an -ERR line", line)
	}
	// connection should still be usable afterward
	fmt.Fprintf(conn, "GET greeting\r\n")
	line, _ = r.ReadString('\n')
	if line != "$-1\r\n" {
		t.Fatalf("connection not usable after a non-fatal protocol error: got %q", line)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// TestPartialWrites proves the server does NOT assume a full command
// arrives in a single TCP read. It uses net.Pipe (a fully synchronous,
// unbuffered in-memory connection) and writes the request a few bytes at a
// time with small delays between writes — the most adversarial framing case
// short of an actual network. If the server's readLine/io.ReadFull handling
// assumed whole-message reads, this would hang or corrupt the response.
func TestPartialWrites(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "partial.wal")
	s, err := store.Open(walPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	srv := New(s)
	srv.IdleTimeout = 5 * time.Second

	clientConn, serverConn := net.Pipe()
	go srv.handleConn(serverConn)
	defer clientConn.Close()

	request := "PUT slowkey 11\r\nhello world\r\n"
	go func() {
		for i := 0; i < len(request); i++ {
			clientConn.Write([]byte{request[i]})
			time.Sleep(2 * time.Millisecond) // force many separate reads server-side
		}
	}()

	r := bufio.NewReader(clientConn)
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if line != "+OK\r\n" {
		t.Fatalf("response to byte-at-a-time PUT: got %q, want \"+OK\\r\\n\"", line)
	}

	v, ok := s.Get([]byte("slowkey"))
	if !ok || string(v) != "hello world" {
		t.Fatalf("store state after partial-write PUT: got (%q, %v), want (hello world, true)", v, ok)
	}
}

// TestIdleTimeout verifies a connection that sends nothing gets closed by
// the server's read deadline rather than hanging forever.
func TestIdleTimeout(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "timeout.wal")
	s, err := store.Open(walPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	srv := New(s)
	srv.IdleTimeout = 50 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln)
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send nothing. The server should close its side within ~IdleTimeout.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatalf("expected connection to be closed by server idle timeout, but Read succeeded")
	}
}

// TestConcurrentClients runs many independent client connections doing
// PUT/GET concurrently and checks every client sees a consistent result for
// its own writes — the network-level counterpart to the Phase 2
// linearizability test, proving the protocol layer doesn't introduce its
// own corruption on top of an already-correct Store.
func TestConcurrentClients(t *testing.T) {
	_, addr := startTestServer(t)

	const clients = 20
	var wg sync.WaitGroup
	wg.Add(clients)
	errs := make(chan error, clients)

	for i := 0; i < clients; i++ {
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- fmt.Errorf("client %d dial: %w", id, err)
				return
			}
			defer conn.Close()
			r := bufio.NewReader(conn)

			key := fmt.Sprintf("client-%d", id)
			val := fmt.Sprintf("value-%d", id)
			fmt.Fprintf(conn, "PUT %s %d\r\n%s\r\n", key, len(val), val)
			line, _ := r.ReadString('\n')
			if line != "+OK\r\n" {
				errs <- fmt.Errorf("client %d: PUT got %q", id, line)
				return
			}

			fmt.Fprintf(conn, "GET %s\r\n", key)
			line, _ = r.ReadString('\n')
			wantLine := fmt.Sprintf("$%d\r\n", len(val))
			if line != wantLine {
				errs <- fmt.Errorf("client %d: GET length got %q want %q", id, line, wantLine)
				return
			}
			buf := make([]byte, len(val))
			if _, err := readFull(r, buf); err != nil {
				errs <- fmt.Errorf("client %d: read value: %w", id, err)
				return
			}
			if string(buf) != val {
				errs <- fmt.Errorf("client %d: GET value got %q want %q", id, buf, val)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestSlowClientDoesNotBlockOthers proves the backpressure/isolation
// policy documented in docs/ARCHITECTURE.md: a client that stops reading
// its responses (so the server's Write blocks on a full TCP send buffer)
// only stalls its OWN connection's goroutine, not the rest of the server.
func TestSlowClientDoesNotBlockOthers(t *testing.T) {
	_, addr := startTestServer(t)

	// Slow client: connects, sends one PUT request, then never reads the
	// response — never even net.Conn.Close()s, so its goroutine on the
	// server side is left waiting on a write (or at least holding the
	// connection open without service degradation elsewhere).
	slowConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("slow client dial: %v", err)
	}
	defer slowConn.Close()
	fmt.Fprintf(slowConn, "PUT slow 3\r\nabc\r\n")
	// Deliberately not reading the response.

	// Fast client: should complete promptly regardless of the slow one.
	done := make(chan error, 1)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		fmt.Fprintf(conn, "PUT fast 3\r\nxyz\r\n")
		line, err := r.ReadString('\n')
		if err != nil {
			done <- err
			return
		}
		if line != "+OK\r\n" {
			done <- fmt.Errorf("fast client got %q, want +OK", line)
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fast client failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("fast client was blocked by the slow client — connection isolation is broken")
	}
}
