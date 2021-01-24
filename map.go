/*
 * Highly optimized Cuckoo hash map implementation
 * LICENSE: MIT
 */

package cuckoohash

import (
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
