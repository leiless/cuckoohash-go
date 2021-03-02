/*
 * Highly optimized Cuckoo hash set implementation
 * LICENSE: MIT
 */

package cuckoohash

import (
	"fmt"
	"strings"
)

type Set struct {
	m Map
}

func newSet(bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 hash64WithSeedFunc, debug, expandable bool) (*Set, error) {
	m, err := newMap(bytesPerKey, keysPerBucket, bucketCount, hasher1, hasher2, debug, expandable)
	if err != nil {
		return nil, err
	}
	return &Set{m: *m}, nil
}

func NewSet(bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 hash64WithSeedFunc, expandableOpt ...bool) (*Set, error) {
	expandable := true
	if n := len(expandableOpt); n > 1 {
		panic(fmt.Sprintf("at most one `expandableOpt` argument can be passed, got %v", n))
	} else if n != 0 {
		expandable = expandableOpt[0]
	}
	return newSet(bytesPerKey, keysPerBucket, bucketCount, hasher1, hasher2, false, expandable)
}

func (s *Set) Clear() {
	s.m.Clear()
}

func (s *Set) Count() uint64 {
	return s.m.Count()
}

func (s *Set) IsEmpty() bool {
	return s.Count() == 0
}

func (s *Set) MemoryInBytes() uint64 {
	return s.m.MemoryInBytes()
}

func (s *Set) LoadFactor() float64 {
	return s.m.LoadFactor()
}

func (s *Set) Contains(key []byte) bool {
	return s.m.ContainsKey(key)
}

// Return true if key deleted from Set, false if key absent previously.
func (s *Set) Del(key []byte) bool {
	_, err := s.m.Del(key)
	// The only possible error is ErrKeyNotFound
	return err == nil
}

// Return true if key put in Set, false if the bucket if full(s.m.expandable is false)
func (s *Set) Put(key []byte) bool {
	_, err := s.m.Put(key, nil, true)
	return err == nil
}

var (
	mapTypeString = fmt.Sprintf("%T", Map{})
	setTypeString = fmt.Sprintf("%T", Set{})
)

func (s *Set) String() string {
	return strings.ReplaceAll(s.m.String(), mapTypeString, setTypeString)
}
