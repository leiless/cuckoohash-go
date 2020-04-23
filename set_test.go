package cuckoohash

import (
	"crypto/md5"
	"fmt"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestPerformance(t *testing.T) {
	keysPerBucket := uint32(16)
	buckets := uint32(300_000)
	s := NewCuckooHashSet(md5.Size, keysPerBucket, buckets)
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
	fmt.Printf("Add() time spent: %v\n", time.Since(t1))

	t1 = time.Now()
	for _, v := range arr {
		s.Contains(v)
	}
	fmt.Printf("All keys present in set, time spent: %v\n", time.Since(t1))

	fmt.Printf("%v\n", s)
}

