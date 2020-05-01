package cuckoohash

import (
	"crypto/md5"
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestBaseline1(t *testing.T) {
	s := newCuckooHashSet(true, false, 1, 1, 1)
	if !s.IsEmpty() {
		t.Fatalf("%v should be empty", s)
	}
	fmt.Printf("%v\n", s)
	b := []byte{0}
	if ok := s.Add(b); !ok {
		t.Fatalf("Add() shouldn't fail")
	}
	if !s.Contains(b) {
		t.Fatalf("Why %x not in %v", b, s)
	}
	if s.Count() != 1 {
		t.Fatalf("Bad count %v", s.Count())
	}
	if s.LoadFactor() != 1.0 {
		t.Fatalf("%v should full by now", s)
	}
	fmt.Printf("%v\n", s)

	if ok := s.Remove(b); !ok {
		t.Fatalf("Why %x not in %v previously", b, s)
	}
	if !s.IsEmpty() {
		t.Fatalf("%v should be empty", s)
	}

	if ok := s.Add(b); !ok {
		t.Fatalf("Add() shouldn't fail")
	}
	if s.Count() != 1 {
		t.Fatalf("Bad count %v", s.Count())
	}
	s.Clear()
	if !s.IsEmpty() {
		t.Fatalf("%v should be empty", s)
	}

	if ok := s.Add(b); !ok {
		t.Fatalf("Add() shouldn't fail")
	}
	b2 := []byte{1}
	if ok := s.Add(b2); ok {
		t.Fatalf("Add() should not success, since expandable is false")
	}
	if s.Contains(b2) {
		t.Fatalf("Why %v contains %x", s, b2)
	}
	if s.Count() != 1 {
		t.Fatalf("Bad count %v", s.Count())
	}
}

func TestPerformance(t *testing.T) {
	keysPerBucket := uint32(16)
	buckets := uint32(300_000)
	s := NewCuckooHashSet(md5.Size, keysPerBucket, 1)
	n := uint32(float64(keysPerBucket * buckets) * 0.66)
	arr := make([][]byte, n)

	t1 := time.Now()
	for i := range arr {
		u, err := uuid.NewRandom()
		if err != nil {
			t.Errorf("uuid.NewRandom() fail: %v", err)
		}
		sum := md5.Sum([]byte(u.String()))
		a := make([]byte, len(sum))
		for i, b := range sum {
			a[i] = b
		}
		arr[i] = a
	}
	fmt.Printf("Data populate time spent: %v\n", time.Since(t1))

	t1 = time.Now()
	for _, v := range arr {
		if !s.Add(v) {
			t.Fatalf("%x seems already in set", v)
		}
	}
	fmt.Printf("Add() without optimal buckets, time spent: %v\n", time.Since(t1))
	fmt.Printf("%v\n", s)

	s1 := NewCuckooHashSet(md5.Size, keysPerBucket, s.buckets)
	t1 = time.Now()
	for _, v := range arr {
		if !s1.Add(v) {
			t.Fatalf("%x seems already in set", v)
		}
	}
	fmt.Printf("Add() with optimal buckets, time spent: %v\n", time.Since(t1))
	fmt.Printf("%v\n", s1)

	t1 = time.Now()
	for _, v := range arr {
		s.Contains(v)
	}
	fmt.Printf("Contains() time spent: %v\n", time.Since(t1))

	t1 = time.Now()
	for _, v := range arr {
		if !s.Remove(v) {
			t.Fatalf("%x not in set", v)
		}
	}
	fmt.Printf("Remove() time spent: %v\n", time.Since(t1))
	fmt.Printf("%v\n", s)
	if !s.IsEmpty() {
		t.Fatalf("%v should be empty by now", s)
	}
}

