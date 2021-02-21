/*
 * Highly optimized Cuckoo hash map implementation
 * LICENSE: MIT
 */

package cuckoohash

import (
	"fmt"
	"math/bits"
	"math/rand"
	"time"
)

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
	// [][][*] as key-value combo(full fingerprint, see case 2)
	buckets [][][]byte
	// Count of inserted keys
	count uint64

	// Used for testing
	debug bool

	// Fingerprint length
	bytesPerKey uint32
	// How many keys a bucket will store
	keysPerBucket uint32
	// Total bucket count, i.e. len(buckets)
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

	seed1   uint64
	seed2   uint64
	hasher1 hash64WithSeedFunc
	hasher2 hash64WithSeedFunc
	r       rand.Source64
}

type hash64WithSeedFunc = func(b []byte, s uint64) uint64

func (m *Map) initBuckets() {
	buckets := make([][][]byte, m.bucketCount)
	for i := range buckets {
		buckets[i] = make([][]byte, m.keysPerBucket)
	}
	// Key-value combo, i.e. [][][*] are allocated on demand
	m.buckets = buckets
	// Reset counting
	m.count = 0
	m.valuesByteCount = 0
}

func newMap(debug bool, bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 hash64WithSeedFunc, expandable bool) (*Map, error) {
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
	// Basic sanity check for the hash functions
	_ = hasher1(nil, 0)
	_ = hasher2(nil, 0)

	seed1 := uint64(time.Now().UnixNano())
	seed2 := seed1 * 31

	m := &Map{
		debug:         debug,
		bytesPerKey:   bytesPerKey,
		keysPerBucket: keysPerBucket,
		bucketCount:   bucketCount,
		bucketPower:   uint32(bits.TrailingZeros32(bucketCount)),
		expandable:    expandable,
		seed1:         seed1,
		seed2:         seed2,
		hasher1:       hasher1,
		hasher2:       hasher2,
		r:             rand.NewSource(int64(seed1)).(rand.Source64),
	}
	m.initBuckets()
	m.sanityCheck()
	return m, nil
}

// By default, Map is expandable, pass false as last argument to cancel this behaviour
func NewMap(bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 hash64WithSeedFunc, expandableOpt ...bool) (*Map, error) {
	expandable := true
	if n := len(expandableOpt); n > 1 {
		panic(fmt.Sprintf("at most one `expandableOpt` argument can be passed, got %v", n))
	} else if n != 0 {
		expandable = expandableOpt[0]
	}
	return newMap(false, bytesPerKey, keysPerBucket, bucketCount, hasher1, hasher2, expandable)
}

// Clumsy but cheap assertion, mainly used for debugging
func (m *Map) assert(cond bool) {
	if m.debug {
		if !cond {
			panic("assertion failure")
		}
	}
}

// Type and value must both be equal
func (m *Map) assertEQ(lhs, rhs interface{}) {
	if m.debug {
		if lhs != rhs {
			panic(fmt.Sprintf("equality assertion failure: %T vs %T, %v vs %v", lhs, rhs, lhs, rhs))
		}
	}
}

// Return false to stop further iteration
type kvFunc = func([]byte, []byte) bool

// For each loop(read-only) on every key-value in the map
// Return true if function completed on all items
func (m *Map) forEachKV(f kvFunc) bool {
	for _, bucket := range m.buckets {
		for _, kv := range bucket {
			if kv != nil {
				k, v := kv[:m.bytesPerKey], kv[m.bytesPerKey:]
				if !f(k, v) {
					return false
				}
			}
		}
	}
	return true
}

type bucketIndexFunc = func([][]byte, uint32) interface{}

// Index key-value by key
//
// If given key not found in the map, the bucketIndexFunc will be called with special arguments: (nil, 0)
// Caller must check nullability of the first argument in bucketIndexFunc
//
// For functions which may rewrite key and/or value binding
func (m *Map) kvIndexByKey(key []byte, f bucketIndexFunc) interface{} {
	if uint32(len(key)) != m.bytesPerKey {
		return f(nil, 0)
	}

	h1 := m.hash1(key)
	bucket := m.buckets[h1]
	m.assertEQ(uint32(len(bucket)), m.keysPerBucket)
	for i := uint32(0); i < m.keysPerBucket; i++ {
		if bucket[i] != nil {
			if k := bucket[i][:m.bytesPerKey]; byteSliceEquals(k, key) {
				return f(bucket, i)
			}
		}
	}

	// Skip scan bucket if h2 equals to h1
	if h2 := m.hash2(key, h1); h2 != h1 {
		bucket = m.buckets[h2]
		m.assertEQ(uint32(len(bucket)), m.keysPerBucket)
		for i := uint32(0); i < m.keysPerBucket; i++ {
			if bucket[i] != nil {
				if k := bucket[i][:m.bytesPerKey]; byteSliceEquals(k, key) {
					return f(bucket, i)
				}
			}
		}
	}

	return f(nil, 0)
}

// Return a raw hash value
// uint32 is sufficient in our use case.
func (m *Map) hash1Raw(key []byte) uint32 {
	return uint32(m.hasher1(key, m.seed1))
}

// Return a masked(according to the bucket power) hash index
func (m *Map) hash1(key []byte) uint32 {
	return m.hash1Raw(key) & ((1 << m.bucketPower) - 1)
}

func (m *Map) hash2Raw(key []byte, h1 uint32) uint32 {
	hh := m.hasher2(key, m.seed2)
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

func (m *Map) containsKey(key []byte) bool {
	return m.kvIndexByKey(key, func(bucket [][]byte, _ uint32) interface{} {
		return bucket != nil
	}).(bool)
}

// This function yield a bad performance since it'll linearly scan the whole array
//	you should generally not to call this function as much as you can
func (m *Map) containsValue(val []byte) bool {
	return !m.forEachKV(func(_ []byte, v []byte) bool {
		return !byteSliceEquals(v, val)
	})
}

func (m *Map) assertCount() {
	m.assertEQ(m.bucketCount, uint32(1)<<m.bucketPower)
	m.assert(m.count <= uint64(m.bucketCount*m.keysPerBucket))

	var count uint64
	var valuesByteCount uint64
	ok := m.forEachKV(func(_ []byte, v []byte) bool {
		count++
		valuesByteCount += uint64(len(v))
		return true
	})
	m.assert(ok)
	m.assertEQ(count, m.count)
	m.assertEQ(valuesByteCount, m.valuesByteCount)
}

func (m *Map) assertPosition() {
	for i, bucket := range m.buckets {
		for _, kv := range bucket {
			if kv == nil {
				continue
			}

			k := kv[:m.bytesPerKey]
			h1 := m.hash1(k)
			if h1 != uint32(i) {
				h2 := m.hash2(k, h1)
				m.assertEQ(h2, uint32(i))
			}
		}
	}
}

// Run internal sanity check upon the Map
func (m *Map) sanityCheck() {
	if m.debug {
		m.assertCount()
		m.assertPosition()
	}
}

func (m *Map) Clear() {
	if m.debug {
		m.sanityCheck()

		snapshot := m.valuesByteCount
		valuesByteCount := uint64(0)

		for _, bucket := range m.buckets {
			for i := range bucket {
				if bucket[i] != nil {
					vLen := uint64(len(bucket[i][m.bytesPerKey:]))
					valuesByteCount += vLen
					m.valuesByteCount -= vLen
					bucket[i] = nil
					m.count--
				}
			}
		}

		m.assertEQ(snapshot, valuesByteCount)
		m.assertEQ(m.valuesByteCount, uint64(0))
		m.assertEQ(m.count, uint64(0))

		m.sanityCheck()
	} else {
		m.initBuckets()
	}
}

func (m *Map) Count() uint64 {
	m.sanityCheck()
	return m.count
}

func (m *Map) IsEmpty() bool {
	return m.Count() != 0
}

// Return estimated memory in bytes used by m.buckets
// Internal pointer byte count not included
func (m *Map) MemoryInBytes() uint64 {
	return uint64(m.bucketCount*m.keysPerBucket) +
		uint64(m.bytesPerKey)*m.count +
		m.valuesByteCount
}

func (m *Map) LoadFactor() float64 {
	return float64(m.count) / float64(m.bucketCount*m.keysPerBucket)
}

func (m *Map) Get(key []byte, defaultValue ...[]byte) []byte {
	if n := len(defaultValue); n > 1 {
		panic(fmt.Sprintf("at most one `defaultValue` argument can be passed, got %v", n))
	}

	v := m.kvIndexByKey(key, func(b [][]byte, i uint32) interface{} {
		if b != nil {
			return b[m.bytesPerKey:]
		}
		return []byte(nil)
	}).([]byte)

	if v == nil && len(defaultValue) != 0 {
		v = defaultValue[0]
	}
	return v
}

// Return true if key-val put into given bucket
func (m *Map) put0(key []byte, val []byte, h uint32) bool {
	bucket := m.buckets[h]
	for i := range bucket {
		if bucket[i] == nil {
			b := make([]byte, len(key)+len(val))
			copy(b, key)
			copy(b[len(key):], val)
			bucket[i] = b
			m.count++
			m.valuesByteCount += uint64(len(val))

			m.sanityCheck()
			return true
		}
	}
	return false
}

func (m *Map) put1(key []byte, val []byte) error {
	if uint32(len(key)) != m.bytesPerKey {
		return ErrInvalidArgument
	}

	h1 := m.hash1(key)
	if m.put0(key, val, h1) {
		return nil
	}

	h2 := m.hash2(key, h1)
	if h2 != h1 && m.put0(key, val, h2) {
		return nil
	}

	// Use deterministic selection(with the seed1 backed by m.r)
	h := h1
	if m.r.Uint64()&1 == 0 {
		h = h2
	}
	return m.rehashOrExpand(key, val, h)
}

// Return the value before Put
func (m *Map) Put(key []byte, val []byte, ifAbsentOpt ...bool) ([]byte, error) {
	var ifAbsent bool
	if n := len(ifAbsentOpt); n > 1 {
		panic(fmt.Sprintf("at most one `ifAbsentOpt` argument can be passed, got %v", n))
	} else if n != 0 {
		ifAbsent = ifAbsentOpt[0]
	}

	if ifAbsent {
		type result struct {
			b []byte
			e error
		}

		v := m.kvIndexByKey(key, func(b [][]byte, i uint32) interface{} {
			if b != nil {
				return result{
					b: b[i][m.bytesPerKey:],
				}
			}
			return result{
				e: m.put1(key, val),
			}
		}).(result)
		return v.b, v.e
	}

	if oldVal, updated := m.update(key, val); updated {
		return oldVal, nil
	}
	return nil, m.put1(key, val)
}

// Return true if old value was overwritten, false if key not found in the Map
func (m *Map) update(key []byte, val []byte) ([]byte, bool) {
	type result struct {
		oldVal  []byte
		updated bool
	}

	v := m.kvIndexByKey(key, func(bucket [][]byte, i uint32) interface{} {
		if bucket == nil {
			return result{}
		}

		oldVal := bucket[i][m.bytesPerKey:]
		m.valuesByteCount -= uint64(len(oldVal))
		b := make([]byte, len(key)+len(val))
		copy(b, key)
		copy(b[len(key):], val)
		bucket[i] = b
		m.valuesByteCount += uint64(len(val))
		m.sanityCheck()

		return result{
			oldVal:  oldVal,
			updated: true,
		}
	}).(result)

	return v.oldVal, v.updated
}

func (m *Map) rehashOrExpand(key []byte, val []byte, h uint32) error {
	bucket := m.buckets[h]

	kv := make([]byte, len(key)+len(val))
	copy(kv, key)
	copy(kv[len(key):], val)

	for i := uint32(0); i < m.keysPerBucket; i++ {
		newKV := kv
		kv = bucket[i]
		bucket[i] = newKV

		m.valuesByteCount -= uint64(len(kv[m.bytesPerKey:]))
		m.valuesByteCount += uint64(len(newKV[m.bytesPerKey:]))

		k := kv[:m.bytesPerKey]
		v := kv[m.bytesPerKey:]
		if m.put0(k, v, m.hash2(k, h)) {
			return nil
		}
	}

	if !m.expandable {
		// Restore initial swapped key/value back, key/value location will be shifted down by 1
		oldKV := bucket[0]
		bucket[0] = kv
		m.valuesByteCount -= uint64(len(oldKV[m.bytesPerKey:]))
		m.valuesByteCount += uint64(len(kv[m.bytesPerKey:]))
		m.sanityCheck()
		return ErrBucketIsFull
	}

	if m.debug {
		debug("Bucket is full, try to expand %v", m)
	}

	m.expandBucket()
	err := m.put1(key, val)
	m.assertEQ(err, nil)
	if m.debug {
		debug("After expanded: %v", *m, m)
	}
	return nil
}

// see: initBuckets
func (m *Map) expandBucket() {
	buckets := make([][][]byte, m.bucketCount<<1)
	mask := uint32((1 << m.bucketPower) - 1)
	newMask := uint32((2 << m.bucketPower) - 1)
	m.assertEQ((mask<<1)^newMask, uint32(1))

	for i := uint32(0); i < m.bucketCount; i++ {
		for j := uint32(0); j < m.keysPerBucket; j++ {
			kv := m.buckets[i][j]
			if kv == nil {
				continue
			}

			k := kv[:m.bytesPerKey]
			h1Raw := m.hash1Raw(k)
			var hRaw uint32
			if (h1Raw & mask) == i {
				hRaw = h1Raw
			} else {
				h2Raw := m.hash2Raw(k, h1Raw)
				m.assertEQ(h2Raw&mask, i)
				hRaw = h2Raw
			}

			h := hRaw & newMask
			if h == i {
				// Highest bit position of hRaw and newMask not match
			} else {
				// h equals to i | (1 << m.bucketPower)
				m.assertEQ(h, m.bucketCount+i)
			}

			buckets[h][j] = kv
		}
	}

	m.buckets = buckets
	m.bucketCount <<= 1
	m.bucketPower++
	m.expansionCount++

	m.sanityCheck()
}

func (m *Map) String() string {
	return fmt.Sprintf(
		"[%T "+
			"buckets=%p count=%v debug=%v "+
			"bytesPerKey=%v keysPerBucket=%v bucketCount=%v bucketPower=%v "+
			"expandable=%v expansionCount=%v zeroHash2Count=%v valuesByteCount=%v "+
			"seed1=%#x seed2=%#x hasher1=%p hasher2=%p r=%p "+
			"loadFactor=%v memoryInBytes=%v"+
			"]",
		Map{}, m.buckets, m.count, m.debug,
		m.bytesPerKey, m.keysPerBucket, m.bucketCount, m.bucketPower,
		m.expandable, m.expansionCount, m.zeroHash2Count, m.valuesByteCount,
		m.seed1, m.seed2, m.hasher1, m.hasher2, m.r,
		m.LoadFactor(), formatByteSize(m.MemoryInBytes()),
	)
}
