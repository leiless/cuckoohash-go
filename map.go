/*
 * Highly optimized Cuckoo hash map implementation
 * LICENSE: MIT
 */

package cuckoohash

import (
	"fmt"
	"math/bits"
	"time"
)

type Hasher interface {
	Hash64WithSeed(b []byte, seed uint64) uint64
}

// To simplify API design, we only accepts []byte as key-value
//	for struct, you can marshal/hash it before insertion
// Also note that all keys must be equal size
//
// Currently, there are three possible data-fingerprint models:
//	1)		|----------------|		data
//			|--------------|		fp (compressed fingerprint, e.g. Cuckoo filter)
//
//	2) 		|----------------|		data
//			|----------------|		fp (full fingerprint)
//
// 	3)		|----------------|		data
//			|------------------|	fp (extended fingerprint)
//
// Since we designated to support unlimited insertions:
//	1) won't be possible since data loss when data -> fp
//	3) won't be supported since it requires us to bookkeeping original data length,
//		which incur additional memory footprint, besides, it's unrealistic in real world.
// Thus we only support 2), i.e. the full fingerprint as it's.
//
// NOTE: This struct is NOT thread safe
type Map struct {
	// [*] bucket array
	// [][*] which bucket
	// [][][0][*] as key(full fingerprint, see case 2)
	// [][][1][*] as value(nullable)
	buckets [][][][]byte
	count   uint64

	// Fingerprint length
	bytesPerKey uint32
	// How many keys a bucket will store
	keysPerBucket uint32
	// Total bucket count, i.e. len(arr[:])
	bucketCount uint32
	// Invariant: bucketCount == 1 << bucketPower
	bucketPower uint32

	// Is this Map expandable
	expandable     bool
	expansionCount uint8
	// Times of hash2() got same value as hash1()
	zeroHash2Count uint64
	// Total bytes occupied of all values
	valuesByteCount uint64

	seed1 uint64
	seed2 uint64

	hasher1 Hasher
	hasher2 Hasher

	// Used in testing
	debug bool
}

func newMap(debug, expandable bool, bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 Hasher) (*Map, error) {
	if bytesPerKey == 0 {
		return nil, ErrInvalidArgument
	}
	// Keys(full fingerprint) per bucket generally greater than 1, left 1 for unit test
	if keysPerBucket == 0 {
		return nil, ErrInvalidArgument
	}
	bucketCount = nextPowerOfTwo(bucketCount)
	if bucketCount == 0 {
		return nil, ErrInvalidArgument
	}

	if hasher1 == nil || hasher2 == nil {
		return nil, ErrInvalidArgument
	}

	buckets := make([][][][]byte, bucketCount)
	for i := range buckets {
		// Key-value are allocated on demand
		buckets[i] = make([][][]byte, keysPerBucket)
	}
	// [][][*] are allocated on demand

	seed1 := uint64(time.Now().UnixNano())
	seed2 := seed1 * 17

	return &Map{
		buckets:       buckets,
		bytesPerKey:   bytesPerKey,
		keysPerBucket: keysPerBucket,
		bucketCount:   bucketCount,
		bucketPower:   uint32(bits.TrailingZeros32(bucketCount)),
		expandable:    expandable,
		seed1:         seed1,
		seed2:         seed2,
		hasher1:       hasher1,
		hasher2:       hasher2,
		debug:         debug,
	}, nil
}

// Clumsy but cheap assertion, mainly used for debugging
func (m *Map) assert(cond bool) {
	if m.debug {
		if !cond {
			panic("assertion failure")
		}
	}
}

func (m *Map) assertEQ(lhs, rhs interface{}) {
	if m.debug {
		if lhs != rhs {
			panic(fmt.Sprintf("equality assertion failure: lhs: %v rhs: %v", lhs, rhs))
		}
	}
}

func (m *Map) assertNE(lhs, rhs interface{}) {
	if m.debug {
		if lhs == rhs {
			panic(fmt.Sprintf("inequality assertion failure: val: %v", lhs))
		}
	}
}

type kvFunc = func([]byte, []byte) bool

// For each loop(read-only) on every key-value in the map
// Return true if function completed on all items
func (m *Map) forEachKV(f kvFunc) bool {
	for _, bucket := range m.buckets {
		for _, kv := range bucket {
			if kv == nil {
				continue
			}

			// if len(kv) == 1, it means value is nil, we don't store nil directly at [1]
			m.assert(len(kv) == 1 || len(kv) == 2)
			var k, v []byte
			k = kv[0]
			if len(kv) == 2 {
				v = kv[1]
			}
			if !f(k, v) {
				return false
			}
		}
	}
	return true
}

// Return a raw hash value
// uint32 is sufficient in our use case.
func (m *Map) hash1Raw(key []byte) uint32 {
	return uint32(m.hasher1.Hash64WithSeed(key, m.seed1))
}

// Return a masked(according to the bucket power) hash index
func (m *Map) hash1(key []byte) uint32 {
	return m.hash1Raw(key) & ((1 << m.bucketPower) - 1)
}

func (m *Map) hash2Raw(key []byte, h1 uint32) uint32 {
	hh := m.hasher2.Hash64WithSeed(key, m.seed2)
	h := uint32(hh)
	if h == 0 {
		hh2 := simpleHash(key)
		h = uint32(hh2)
		if h == 0 {
			for hh != 0 {
				if h = uint32(hh ^ hh2); h != 0 {
					break
				}
				hh >>= 8
			}
		}
		// Let alone if h still zero, since the possibility is rare
		// Expansion as last resort can help this situation
	}
	return h1 ^ h
}

// Return an alternative hash index to resolve hashing collision
//	it possibly equals to h1
//
// To increase load factor(best effort) of the backing buckets
//	we should return h2 such that h2 != h1
//	so the alternative hash index can be differentiated from each other
//	i.e. relocate to another bucket when hash collided
//
// We use XOR to swap between hash index and its alternative hash index
// 	instead of get two hashes and compare to squeeze out the alternative hash index
// Which means we need an inverse function such that:
//		func(input, h1) = h2
//		func(input, h2) = h1
// XOR is a good fit here
func (m *Map) hash2(key []byte, h1 uint32) uint32 {
	h2 := m.hash2Raw(key, h1) & ((1 << m.bucketPower) - 1)
	// h2 equals to h1 meaning intermediate h is zero
	if h2 == h1 && m.bucketPower != 0 {
		m.zeroHash2Count++
	}
	return h2
}
