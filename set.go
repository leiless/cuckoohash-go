/*
 * Highly optimized Cuckoo hash set implementation
 */

package cuckoohash

import (
	"bytes"
	"fmt"
	"github.com/OneOfOne/xxhash"
	gofarm "github.com/dgryski/go-farm"
	"math/bits"
	"math/rand"
	"time"
)

type CuckooHashSet struct {
	debug bool

	arr [][][]byte
	count uint64

	bytesPerKey uint32
	keysPerBucket uint32
	buckets uint32
	bucketsPow int

	expandable bool
	expansionCount uint8
	zeroHash2Count uint64

	seed1 uint64
	seed2 uint64
}

func NewCuckooHashSet(bytesPerKey, keysPerBucket, buckets uint32) *CuckooHashSet {
	return newCuckooHashSet(false, true, bytesPerKey, keysPerBucket, buckets)
}

func newCuckooHashSet(debug, expandable bool, bytesPerKey, keysPerBucket, buckets uint32) *CuckooHashSet {
	if bytesPerKey <= 0 {
		bytesPerKey = DefaultBytesPerKey
	}
	if keysPerBucket <= 0 {
		keysPerBucket = DefaultKeysPerBucket
	}
	if buckets <= 0 {
		buckets = DefaultBuckets
	}
	buckets1 := nextPowerOfTwo(buckets)
	if buckets1 == 0 {
		panic(fmt.Sprintf("buckets too large: %v", buckets))
	}
	arr := make([][][]byte, buckets1)
	for i := range arr {
		arr[i] = make([][]byte, keysPerBucket)
	}
	// [][][data] will be allocated on demand
	return &CuckooHashSet{
		debug:          debug,
		arr:            arr,
		bytesPerKey:    bytesPerKey,
		keysPerBucket:  keysPerBucket,
		buckets:        buckets1,
		bucketsPow:     bits.TrailingZeros(uint(buckets1)),
		expandable:     expandable,
		seed1: 			uint64(time.Now().UnixNano()),
		seed2: 			uint64(time.Now().UnixNano()),
	}
}

// For each key in the hash set
func (s *CuckooHashSet) forEachKey(fn func([]byte)) {
	var arr [][]byte
	for i := range s.arr {
		arr = s.arr[i]
		for j := range arr {
			if arr[j] != nil {
				fn(arr[j])
			}
		}
	}
}

func (s *CuckooHashSet) hash1(key []byte) uint32 {
	return uint32(gofarm.Hash64WithSeed(key, s.seed1) & masks[s.bucketsPow])
}

// Return alternate hash to resolve hashing collision, it possibly equals to h1
// To reduce hashing collision, hash 2 function should satisfy:
// 		h1(key) ^ h2(key) != h1(key)
// Which means h2(key) shouldn't return zero value(unless bucket size very small, e.g. 1)
func (s *CuckooHashSet) hash2(key []byte, h1 uint32) uint32 {
	hh := xxhash.Checksum64S(key, s.seed2)
	h := hh & masks[s.bucketsPow]
	if h == 0 {
		hh2 := simpleHash(key)
		h = hh2 & masks[s.bucketsPow]
		if h == 0 {
			for hh != 0 {
				h = (hh ^ hh2) & masks[s.bucketsPow]
				if h != 0 {
					break
				}
				hh >>= 8
			}
			// Let alone if h still zero, since the possibility is rare
			// In such case, expansion is the last resort can help
		}
	}
	if h == 0 && s.bucketsPow != 0 {
		s.zeroHash2Count++
	}
	return h1 ^ uint32(h)
}

// If given key not found in the set, the fn will be called with (nil, -1) argument
// Thus caller must check nullability of the first argument of the fn
//
// Used for functions which may rewrite key binding
func (s *CuckooHashSet) keyIndexByKey(key []byte, fn func([][]byte, int) bool) bool {
	if uint32(len(key)) != s.bytesPerKey {
		return fn(nil, -1)
	}

	h1 := s.hash1(key)
	arr := s.arr[h1]
	for i := range arr {
		if arr[i] != nil && bytes.Equal(arr[i], key) {
			return fn(arr, i)
		}
	}

	h2 := s.hash2(key, h1)
	arr = s.arr[h2]
	for i := range arr {
		if arr[i] != nil && bytes.Equal(arr[i], key) {
			return fn(arr, i)
		}
	}

	return fn(nil, -1)
}

// If given key not found in the set, the fn will be called with (nil) argument
// Thus caller must check nullability of the first argument of the fn
//
// Used for functions which only read key binding
func (s *CuckooHashSet) keyByKey(key []byte, fn func([]byte) bool) bool {
	return s.keyIndexByKey(key, func(arr [][]byte, i int) bool {
		if arr == nil {
			return fn(nil)
		}
		return fn(arr[i])
	})
}

func (s *CuckooHashSet) assertCount() {
	if !s.debug { return }

	if s.buckets != 1 << s.bucketsPow {
		panic(fmt.Sprintf("buckets and bucketsPow mismatch: %v vs %v", s.buckets, s.bucketsPow))
	}
	if s.count > uint64(s.keysPerBucket * s.buckets) {
		panic(fmt.Sprintf("count overflowed bucket capacity: %v vs %v * %v", s.count, s.keysPerBucket, s.buckets))
	}

	var count uint64
	s.forEachKey(func(key []byte) {
		count++
	})
	if count != s.count {
		panic(fmt.Sprintf("count mismatch: expected %v, got %v", count, s.count))
	}
}

func (s *CuckooHashSet) Clear() {
	var arr [][]byte
	for i := range s.arr {
		arr = s.arr[i]
		for j := range arr {
			if arr[j] != nil {
				arr[j] = nil
				s.count--
			}
		}
	}

	if s.count != 0 {
		panic(fmt.Sprintf("Bad count after Clear(): %v", s.count))
	}
}

func (s *CuckooHashSet) Count() uint64 {
	s.assertCount()
	return s.count
}

func (s *CuckooHashSet) IsEmpty() bool {
	return s.Count() == 0
}

// Return estimated memory in bytes used by arr
func (s *CuckooHashSet) MemoryInBytes() uint64 {
	return uint64(s.buckets * s.keysPerBucket) + uint64(s.bytesPerKey) * s.count
}

func (s *CuckooHashSet) LoadFactor() float64 {
	return float64(s.count) / float64(s.buckets * s.keysPerBucket)
}

func (s *CuckooHashSet) Contains(key []byte) bool {
	return s.keyByKey(key, func(key []byte) bool {
		return key != nil
	})
}

func (s *CuckooHashSet) Remove(key []byte) bool {
	return s.keyIndexByKey(key, func(arr [][]byte, i int) bool {
		if arr == nil {
			return false
		}
		arr[i] = nil
		s.count--
		s.assertCount()
		return true
	})
}

func (s *CuckooHashSet) add0(key []byte, h uint32) bool {
	arr := s.arr[h]
	for i, k := range arr {
		if k == nil {
			arr[i] = key
			s.count++
			s.assertCount()
			return true
		}
	}
	// Out of luck, arr is full
	return false
}

func (s *CuckooHashSet) add1(key []byte) bool {
	h1 := s.hash1(key)
	if s.add0(key, h1) {
		return true
	}
	h2 := s.hash2(key, h1)
	if s.add0(key, h2) {
		return true
	}
	h := uint32(rand.Intn(2))
	if h == 0 {
		h = h1
	} else {
		h = h2
	}
	return s.rehashOrExpand(key, h)
}

// Return true if key added to the set and previously not in the set
// false if it already in the set
// false if the bucket is full(s.expandable = false)
// You may call Contains() to distinguish between already exists and bucket full if s.expandable = false
func (s *CuckooHashSet) Add(key []byte) bool {
	if uint32(len(key)) != s.bytesPerKey {
		panic(fmt.Sprintf("Cannot add, expected key size %v, got %v", s.bytesPerKey, len(key)))
	}
	if s.Contains(key) {
		return false
	}
	return s.add1(key)
}

// Linearly kick-out elements from existing buckets
//	and re-add those kicked out elements again into a alternate open positions
// If that fails, expand the buckets and re-add all elements in the hash set
func (s *CuckooHashSet) rehashOrExpand(key []byte, h uint32) bool {
	arr := s.arr[h]
	var newKey []byte
	for i := uint32(0); i < s.keysPerBucket; i++ {
		newKey = key
		key = arr[i]
		arr[i] = newKey

		if s.add0(key, s.hash2(key, h)) {
			return true
		}
	}
	if !s.expandable {
		// Restore initial swapped key back, key location will be shifted by 1
		arr[0] = key
		return false
	}

	if s.debug {
		debug("Bucket is full, try to expand %v", s)
	}

	// After re-add, s.buckets may not equals to s.buckets << 1, i.e. the new hash set expanded again internally.
	t := newCuckooHashSet(s.debug, true, s.bytesPerKey, s.keysPerBucket, s.buckets << 1)
	s.forEachKey(func(key []byte) {
		if ok := t.Add(key); !ok {
			panic(fmt.Sprintf("Cannot add existing keys to expanded set"))
		}
	})
	if ok := t.Add(key); !ok {
		panic(fmt.Sprintf("Cannot add new key to expanded set"))
	}
	s.replace(t)

	if s.debug {
		debug("Set expanded, %v", s)
	}
	return true
}

func (s *CuckooHashSet) replace(t *CuckooHashSet) {
	s.arr = t.arr
	s.count = t.count
	s.buckets = t.buckets
	s.bucketsPow = t.bucketsPow
	s.expansionCount += 1 + t.expansionCount
	s.zeroHash2Count += t.zeroHash2Count
	s.seed1 = t.seed1
	s.seed2 = t.seed2
	s.assertCount()
}

func (s *CuckooHashSet) String() string {
	return fmt.Sprintf("[%T debug=%v, mem=%v, loadFactor=%.2f, count=%v, bytesPerKey=%v, keysPerBucket=%v, buckets=%v, bucketsPow=%v expandable=%v expansionCount=%v zeroHash2Count=%v]",
		s, s.debug, formatBytes(s.MemoryInBytes()), s.LoadFactor(), s.count, s.bytesPerKey, s.keysPerBucket, s.buckets, s.bucketsPow, s.expandable, s.expansionCount, s.zeroHash2Count)
}

