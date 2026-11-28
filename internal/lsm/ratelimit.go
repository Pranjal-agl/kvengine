package lsm

import (
	"io"
	"sync"
	"time"
)

// rateLimiter paces compaction I/O to Options.CompactionBytesPerSec, so
// that on a small machine compaction takes a bounded share of the disk
// (and of the CPU that encodes and decodes its data) instead of whatever it
// can grab. It uses virtual time: each reservation of n bytes moves "next"
// on by n/rate, and the caller sleeps until its reservation's start.
//
// While urgent() is true (compaction is falling behind) it doesn't wait.
// A fixed limit alone turned write bursts into multi-second stalls: the
// limited compaction let L0 fill to the stop limit.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64 // bytes per second
	next   time.Time
	stop   <-chan struct{}
	urgent func() bool
}

func newRateLimiter(bytesPerSec int, stop <-chan struct{}, urgent func() bool) *rateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &rateLimiter{rate: float64(bytesPerSec), stop: stop, urgent: urgent}
}

// wait blocks until n more bytes fit under the rate, or the engine stops.
// A nil limiter never waits.
func (l *rateLimiter) wait(n int) {
	if l == nil || n <= 0 {
		return
	}
	if l.urgent != nil && l.urgent() {
		l.mu.Lock()
		l.next = time.Time{} // no debt carried into the next slack period
		l.mu.Unlock()
		return
	}
	l.mu.Lock()
	now := time.Now()
	start := l.next
	if start.Before(now) {
		start = now
	}
	l.next = start.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
	l.mu.Unlock()
	if d := time.Until(start); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-l.stop: // closing: finish (or abandon) the compaction promptly
		}
	}
}

// limitedWriter passes writes through after waiting on the limiter.
type limitedWriter struct {
	w   io.Writer
	lim *rateLimiter
}

func (lw limitedWriter) Write(p []byte) (int, error) {
	lw.lim.wait(len(p))
	return lw.w.Write(p)
}
