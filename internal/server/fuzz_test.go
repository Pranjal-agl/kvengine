package server

import (
	"bufio"
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"kvengine/internal/store"
)

// FuzzServerProtocol feeds arbitrary bytes to the server's connection
// handler. This covers: truncated commands, huge length fields, binary
// garbage where text is expected, CRLF/LF variations, and any input that
// could cause the server to panic, hang, or corrupt store state.
//
// The server must handle ALL inputs gracefully: return an error response
// or close the connection, never panic or block indefinitely.
func FuzzServerProtocol(f *testing.F) {
	// Seed corpus: real valid commands plus interesting malformed variants.
	f.Add([]byte("PUT k 1\r\nv\r\n"))
	f.Add([]byte("GET k\r\n"))
	f.Add([]byte("DEL k\r\n"))
	f.Add([]byte("PUT k 0\r\n\r\n"))          // empty value
	f.Add([]byte("PUT k 999999\r\n"))         // huge length, no body
	f.Add([]byte("PUT\r\n"))                  // missing args
	f.Add([]byte("\r\n"))                     // blank line
	f.Add([]byte("GET\r\n"))                  // missing key
	f.Add([]byte("PUT k -1\r\n"))             // negative length
	f.Add([]byte("UNKNOWN command here\r\n")) // unknown command
	f.Add([]byte("PUT k 3\r\nabc"))           // no trailing CRLF
	f.Add(bytes.Repeat([]byte("X"), 65537))   // line too long
	f.Add([]byte{0x00, 0x01, 0x02, 0x03})     // binary garbage

	f.Fuzz(func(t *testing.T, data []byte) {
		// Cap input to avoid OOM from huge allocations.
		if len(data) > 1<<20 { // 1 MiB
			return // not t.Skip() — panics inside fuzz callbacks
		}

		// Spin up a real server backed by a temp store.
		walPath := filepath.Join(t.TempDir(), "fuzz.wal")
		s, err := store.Open(walPath)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		srv := New(s)
		srv.IdleTimeout = 100 * time.Millisecond

		// Use net.Pipe: fully synchronous, no actual network.
		client, server := net.Pipe()

		done := make(chan struct{})
		go func() {
			defer close(done)
			srv.handleConn(server)
		}()

		// Write fuzz data to the server-side connection.
		client.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		client.Write(data)

		// Read any response (we don't care what it says, just that it doesn't panic).
		client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 4096)
		r := bufio.NewReader(client)
		r.Read(buf)
		client.Close()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("server goroutine hung — possible deadlock or infinite loop")
		}
	})
}
