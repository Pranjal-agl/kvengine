//go:build race

package lsm

// raceEnabled: the race detector slows large memory copies ~10x, so tests
// that move tens of megabytes skip themselves under -race (CI also runs
// the suite without -race).
const raceEnabled = true
