package lsm

import "hash/fnv"

// bloom is a Bloom filter over an SSTable's keys (tombstones included, since
// a tombstone must be found to stop the search). A negative answer is
// exact, so Get skips the table without touching its index or the disk; a
// positive answer may be wrong with probability ~1%.
//
// Sizing: bitsPerKey = 10 with k = 7 hash functions gives a ~0.8% false
// positive rate (the optimum k for 10 bits/key is 10·ln2 ≈ 6.9). The k
// probe positions come from one 64-bit FNV-1a hash split into two 32-bit
// halves, h1 + i·h2 (Kirsch & Mitzenmacher), which is as good as k
// independent hashes for this purpose.
//
// Filters are built in memory when a table is written or opened (Open
// already scans every table to build its sparse index), so the on-disk
// format is unchanged.
type bloom struct {
	bits []uint64
	k    uint32
}

const (
	bloomBitsPerKey = 10
	bloomK          = 7
)

func bloomHash(key string) (h1, h2 uint32) {
	h := fnv.New64a()
	h.Write([]byte(key))
	sum := h.Sum64()
	return uint32(sum), uint32(sum>>32) | 1 // odd h2 cycles through all positions
}

// newBloom builds a filter for n keys from their precomputed hashes.
func newBloom(hashes [][2]uint32) *bloom {
	nbits := len(hashes) * bloomBitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	b := &bloom{bits: make([]uint64, (nbits+63)/64), k: bloomK}
	m := uint32(len(b.bits) * 64)
	for _, h := range hashes {
		for i := uint32(0); i < b.k; i++ {
			pos := (h[0] + i*h[1]) % m
			b.bits[pos/64] |= 1 << (pos % 64)
		}
	}
	return b
}

// mayContain reports whether key might be in the set. False means
// definitely not.
func (b *bloom) mayContain(key string) bool {
	if b == nil {
		return true
	}
	h1, h2 := bloomHash(key)
	m := uint32(len(b.bits) * 64)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}
