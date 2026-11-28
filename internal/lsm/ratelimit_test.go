package lsm

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiterPacesAndYieldsWhenUrgent(t *testing.T) {
	var urgent atomic.Bool
	stop := make(chan struct{})
	l := newRateLimiter(1<<20, stop, urgent.Load) // 1 MiB/s

	// 256 KiB at a time: the first goes at once, the next ~250 ms later.
	start := time.Now()
	l.wait(256 << 10)
	l.wait(256 << 10)
	if d := time.Since(start); d < 200*time.Millisecond || d > 600*time.Millisecond {
		t.Fatalf("two 256 KiB reservations at 1 MiB/s took %v, want ~250ms", d)
	}

	// Falling behind: no waiting at all, and no debt left over afterwards.
	urgent.Store(true)
	start = time.Now()
	l.wait(64 << 20)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("urgent wait of 64 MiB took %v; it shouldn't wait", d)
	}
	urgent.Store(false)
	start = time.Now()
	l.wait(1 << 10)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("debt carried over from the urgent period: waited %v", d)
	}

	// Stopping cuts a wait short.
	l.wait(10 << 20) // reserves 10 s ahead
	done := make(chan struct{})
	go func() { l.wait(1); close(done) }()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait ignored stop")
	}

	var nilLimiter *rateLimiter
	nilLimiter.wait(1 << 30) // unlimited: returns at once
}
