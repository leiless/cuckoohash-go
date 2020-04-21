/*
 * Highly optimized Cuckoo hash set implementation
 */

package cuckoohash

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"hash/maphash"
	"math/bits"
	"math/rand"
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

	hasher1 hash.Hash64
	hasher2 maphash.Hash
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
	buckets = nextPowerOfTwo(buckets)
	arr := make([][][]byte, buckets)
	for i := range arr {
		arr[i] = make([][]byte, keysPerBucket)
	}
	// [][][data] will be allocated on demand
	return &CuckooHashSet{
		debug:          debug,
		arr:            arr,
		bytesPerKey:    bytesPerKey,
		keysPerBucket:  keysPerBucket,
		buckets:        buckets,
		bucketsPow:     bits.TrailingZeros(uint(buckets)),
		expandable:     expandable,
		hasher1:		fnv.New64a(),
	}
}

// For each key in the hash set
// Return true if the fn completed on all keys, false otherwise.
func (s *CuckooHashSet) forEachKey(fn func([]byte) bool) bool {
	var arr [][]byte
	for i := range s.arr {
		arr = s.arr[i]
		for j := range arr {
			if arr[j] != nil && !fn(arr[j]) {
				return false
			}
		}
	}
	return true
}

func (s *CuckooHashSet) hash1(key []byte) uint32 {
	n, err := s.hasher1.Write(key)
	if err != nil || n != len(key) {
		panic(fmt.Sprintf("hash1 failed: err: %v n: %v vs %v", err, n, len(key)))
	}
	h := s.hasher1.Sum64() & masks[s.bucketsPow]
	s.hasher1.Reset()
	return uint32(h)
}

func (s *CuckooHashSet) hash2(key []byte, h1 uint32) uint32 {
	n, err := s.hasher2.Write(key)
	if err != nil || n != len(key) {
		panic(fmt.Sprintf("hash2 failed: err: %v n: %v vs %v", err, n, len(key)))
	}
	h := s.hasher2.Sum64() & masks[s.bucketsPow]
	s.hasher2.Reset()
	if h == 0 && s.bucketsPow == 0 {
		s.zeroHash2Count++
	}
	return h1 ^ uint32(h)
}

// If given key not found in the set, the fn will be called with (nil, -1) argument
// Thus caller must check nullability of the first argument of the fn
//
// Used for functions which may rewrite key binding
func (s *CuckooHashSet) keyIndexByKey(key []byte, fn func([][]byte, int) interface{}) interface{} {
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
func (s *CuckooHashSet) keyByKey(key []byte, fn func([]byte) interface{}) interface{} {
	return s.keyIndexByKey(key, func(arr [][]byte, i int) interface{} {
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
	return s.Count() != 0
}

// Return estimated memory in bytes used by arr
func (s *CuckooHashSet) MemoryInBytes() uint64 {
	return uint64(s.buckets * s.keysPerBucket) + uint64(s.bytesPerKey) * s.count
}

func (s *CuckooHashSet) LoadFactor() float64 {
	return float64(s.count / uint64(s.buckets * s.keysPerBucket))
}

func (s *CuckooHashSet) Contains(key []byte) bool {
	return s.keyByKey(key, func(key []byte) interface{} {
		return key != nil
	}).(bool)
}

func (s *CuckooHashSet) Remove(key []byte) bool {
	return s.keyIndexByKey(key, func(arr [][]byte, i int) interface{} {
		if arr == nil {
			return false
		}
		arr[i] = nil
		s.count--
		s.assertCount()
		return true
	}).(bool)
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
	// Out of luck, s.arr[h] is full
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
// false if it already in the set(s.expandable = true)
// false if the bucket is full(expandable = false)
func (s *CuckooHashSet) Add(key []byte) bool {
	if uint32(len(key)) != s.bytesPerKey {
		panic(fmt.Sprintf("Cannot add, expected key size %v, got %v", s.bytesPerKey, len(key)))
	}
	if s.Contains(key) {
		return false
	}
	return s.add1(key)
}

func (s *CuckooHashSet) rehashOrExpand(key []byte, h uint32) bool {
	arr := s.arr[h]
	var newKey []byte
	for i := uint32(0); i < s.keysPerBucket; i++ {
		newKey = key
		key = arr[i]
		arr[i] = newKey

		if s.add0(key, h) {
			return true
		}
	}

	if !s.expandable {
		return false
	}

	if s.debug {
		debug("Bucket is full, try to expand %v", s)
	}

	s2 := newCuckooHashSet(s.debug, true, s.bytesPerKey, s.keysPerBucket, s.buckets << 1)
	s2.forEachKey(func(key []byte) bool {
		if ok := s2.Add(key); !ok {
			panic(fmt.Sprintf("Cannot add existing keys to expanded set"))
		}
		return true
	})
	if ok := s2.Add(key); !ok {
		panic(fmt.Sprintf("Cannot add new key to expanded set"))
	}

	if s.debug {
		debug("Set expanded, %v", s)
	}
	return true
}

func (s *CuckooHashSet) String() string {
	return fmt.Sprintf("[%T mem=%v, loadFactor=%.2f, debug=%v, count=%v, bytesPerKey=%v, keysPerBucket=%v, buckets=%v, bucketsPow=%v expandable=%v expansionCount=%v zeroHash2Count=%v]",
		s, s.MemoryInBytes(), s.LoadFactor(), s.debug, s.count, s.bytesPerKey, s.keysPerBucket, s.buckets, s.bucketsPow, s.expandable, s.expansionCount, s.zeroHash2Count)
}

