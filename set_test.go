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
	s := NewCuckooHashSet(md5.Size, keysPerBucket, 1)
	n := uint32(float64(keysPerBucket * buckets) * 0.66)
	arr := make([][]byte, n)
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
		//arr[i] = md5.Sum([]byte(u.String()))
		arr[i] = a
	}

	t1 := time.Now()
	for i := range arr {
		if !s.Add(arr[i]) {
			t.Fatalf("%x seems already in set", arr[i])
		}
	}
	fmt.Printf("Add() time spent: %v\n", time.Since(t1))

	fmt.Printf("%v\n", s)
}

