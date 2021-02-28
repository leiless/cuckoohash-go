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

func newSet(debug bool, bytesPerKey, keysPerBucket, bucketCount uint32, hasher1, hasher2 hash64WithSeedFunc, expandable bool) (*Set, error) {
	m, err := newMap(debug, bytesPerKey, keysPerBucket, bucketCount, hasher1, hasher2, expandable)
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
	return newSet(false, bytesPerKey, keysPerBucket, bucketCount, hasher1, hasher2, expandable)
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

func (s *Set) Del(key []byte) error {
	_, err := s.m.Del(key)
	return err
}

func (s *Set) Put(key []byte, ifAbsentOpt ...bool) error {
	var ifAbsent bool
	if n := len(ifAbsentOpt); n > 1 {
		panic(fmt.Sprintf("at most one `ifAbsentOpt` argument can be passed, got %v", n))
	} else if n != 0 {
		ifAbsent = ifAbsentOpt[0]
	}
	_, err := s.m.Put(key, nil, ifAbsent)
	return err
}

var (
	mapTypeString = fmt.Sprintf("%T", Map{})
	setTypeString = fmt.Sprintf("%T", Set{})
)

func (s *Set) String() string {
	return strings.ReplaceAll(s.m.String(), mapTypeString, setTypeString)
}
