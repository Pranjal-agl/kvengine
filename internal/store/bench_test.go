package store

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkPutSequential measures single-writer Put throughput. Since every
// Put fsyncs (see docs/ARCHITECTURE.md "fsync policy"), this number is
// expected to be dominated by disk fsync latency, not by the map mutex.
func BenchmarkPutSequential(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	key := []byte("benchkey")
	val := []byte("benchvalue")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(key, val); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

// BenchmarkGetSequential measures read throughput with no concurrent
// writers — the ceiling for read performance.
func BenchmarkGetSequential(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	key := []byte("benchkey")
	if err := s.Put(key, []byte("v")); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(key)
	}
}

// BenchmarkGetParallel measures read throughput under concurrent readers —
// this is what tells us whether RWMutex read-side contention is an issue at
// realistic core counts.
func BenchmarkGetParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	key := []byte("benchkey")
	if err := s.Put(key, []byte("v")); err != nil {
		b.Fatalf("seed Put: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get(key)
		}
	})
}

// BenchmarkPutParallel measures write throughput under concurrent writers
// to DIFFERENT keys — this is the number that answers "does map sharding
// matter," since every writer still funnels through the single WAL mutex
// for Append+Sync regardless of key. If this number is close to
// BenchmarkPutSequential's per-op time, the WAL/fsync is the bottleneck,
// not the map lock, and sharding the map would not help.
func BenchmarkPutParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		localCounter := atomic.AddInt64(&counter, 1)
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d-%d", localCounter, i))
			if err := s.Put(key, []byte("v")); err != nil {
				b.Fatalf("Put: %v", err)
			}
			i++
		}
	})
}

// BenchmarkPutSameKeyParallel: concurrent writers all hitting ONE key —
// worst case for any lock, included as a contention ceiling reference point.
func BenchmarkPutSameKeyParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	s, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer s.Close()

	key := []byte("hotkey")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Put(key, []byte("v")); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
	})
}

// BenchmarkPutConcurrentWriters measures per-write cost as the number of
// concurrent writers grows. With group commit, writers that arrive while an
// fsync is in flight share the next one, so ns/op should fall roughly in
// proportion to the writer count until the batch limit or CPU saturates.
// Without it, every writer pays for its own fsync and ns/op stays flat.
func BenchmarkPutConcurrentWriters(b *testing.B) {
	for _, writers := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			s, err := Open(filepath.Join(b.TempDir(), "bench.wal"))
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer s.Close()
			var next int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := atomic.AddInt64(&next, 1)
						if i > int64(b.N) {
							return
						}
						if err := s.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("v")); err != nil {
							b.Errorf("Put: %v", err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}
