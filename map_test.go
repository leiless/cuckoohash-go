package cuckoohash

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	rand2 "math/rand"
	"testing"
	"time"
)

var (
	h1       = farm.Hash64WithSeed
	h2       = xxhash.Checksum64S
	dummyVal = []byte{0xa, 0xb, 0xc, 0xd, 0xe, 0xf}
)

func TestMap1(t *testing.T) {
	m, err := newMap(true, 1, 1, 1, h1, h2, true)
	assert.Nil(t, err)
	assert.True(t, m.IsEmpty())
	assert.Equal(t, 0.0, m.LoadFactor())
	assert.Nil(t, m.Get(nil))
	assert.False(t, m.ContainsKey(nil))
	assert.False(t, m.ContainsKey(nil))
	assert.Equal(t, m.Get(nil, dummyVal), dummyVal)
	assert.Equal(t, m.Get(nil, dummyVal), []byte{0xa, 0xb, 0xc, 0xd, 0xe, 0xf})
	t.Log(m)

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		assert.Nil(t, m.Get(k))
		assert.False(t, m.ContainsKey(k))
		assert.False(t, m.ContainsValue(k))
	}
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			k := []byte{byte(i), byte(j)}
			assert.Nil(t, m.Get(k))
			assert.False(t, m.ContainsKey(k))
			assert.False(t, m.ContainsValue(k))
		}
	}
	assert.False(t, m.ContainsKey(nil))
	assert.False(t, m.ContainsValue(nil))

	m.Clear()
	t.Log(m)

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		oldVal, err := m.Put(k, k, true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)

		v := m.Get(k)
		assert.Equal(t, k, v)

		assert.True(t, m.ContainsKey(k))
		assert.True(t, m.ContainsValue(k))
	}

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		oldVal, err := m.Put(k, k, true)
		assert.Nil(t, err)
		assert.Equal(t, k, oldVal)

		v := m.Get(k)
		assert.Equal(t, k, v)

		assert.True(t, m.ContainsKey(k))
		assert.True(t, m.ContainsValue(k))
	}

	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			k := []byte{byte(i), byte(j)}
			assert.Nil(t, m.Get(k))
			assert.False(t, m.ContainsKey(k))
			assert.False(t, m.ContainsValue(k))
		}
	}

	t.Log(m)
}

func TestMap2(t *testing.T) {
	m, err := newMap(true, 1, 1, 1, h1, h2, true)
	assert.Nil(t, err)

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		oldVal, err := m.Put(k, k, true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)

		v := m.Get(k)
		assert.Equal(t, k, v)
	}

	assert.Equal(t, m.Count(), uint64(256))
	t.Log(m)

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		oldVal, err := m.Del(k)
		assert.Nil(t, err)
		assert.Equal(t, k, oldVal)
	}

	assert.True(t, m.IsEmpty())
	t.Log(m)

	for i := 0; i < 256; i++ {
		k := []byte{byte(i)}
		oldVal, err := m.Del(k)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, oldVal)

		assert.Nil(t, m.Get(k))
		assert.False(t, m.ContainsKey(k))
		assert.False(t, m.ContainsValue(k))
	}
}

func genRandomBytes(size int) []byte {
	b := make([]byte, size)
	n, err := rand.Read(b)
	if err != nil {
		panic(err)
	} else if n != size {
		panic(fmt.Errorf("%w: %v vs %v", io.ErrShortWrite, n, size))
	}
	return b
}

func TestMap3(t *testing.T) {
	m, err := newMap(true, md5.Size, 1, 1, h1, h2, true)
	assert.Nil(t, err)

	n := 5000
	list := make([][]byte, n)
	for i := 0; i < n; i++ {
		list[i] = genRandomBytes(md5.Size)
		oldVal, err := m.Put(list[i], list[i], true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)

		assert.True(t, m.ContainsKey(list[i]))
		assert.True(t, m.ContainsValue(list[i]))
		assert.Equal(t, m.Get(list[i]), list[i])
	}

	assert.Equal(t, m.Count(), uint64(n))
	t.Log(m)

	for i := 0; i < n; i++ {
		k := genRandomBytes(md5.Size)
		assert.Nil(t, m.Get(k))
		assert.False(t, m.ContainsKey(k))
		assert.False(t, m.ContainsValue(k))
	}

	for i := 0; i < n; i += 2 {
		oldVal, err := m.Del(list[i])
		assert.Nil(t, err)
		assert.Equal(t, list[i], oldVal)
	}

	assert.Equal(t, m.Count(), uint64(n)/2)

	for i := 1; i < n; i += 2 {
		assert.NotNil(t, m.Get(list[i]))
		assert.True(t, m.ContainsKey(list[i]))
		assert.True(t, m.ContainsValue(list[i]))
	}

	for i := 1; i < n; i += 2 {
		oldVal, err := m.Del(list[i])
		assert.Nil(t, err)
		assert.Equal(t, list[i], oldVal)
	}

	assert.True(t, m.IsEmpty())

	for i := 0; i < n; i++ {
		assert.Nil(t, m.Get(list[i]))
		assert.False(t, m.ContainsKey(list[i]))
		assert.False(t, m.ContainsValue(list[i]))
	}
}

func TestMap4(t *testing.T) {
	m, err := newMap(true, md5.Size, 1, 1, h1, h2, true)
	assert.Nil(t, err)
	require.Greater(t, md5.Size, 1)

	n := 5000
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = genRandomBytes(md5.Size)
		vals[i] = genRandomBytes(md5.Size / 2)

		oldVal, err := m.Put(keys[i], vals[i], true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)

		assert.True(t, m.ContainsKey(keys[i]))
		assert.True(t, m.ContainsValue(vals[i]))
		assert.Equal(t, m.Get(keys[i]), vals[i])
	}

	for i := 0; i < n; i += 2 {
		oldVal, err := m.Del(keys[i])
		assert.Nil(t, err)
		assert.Equal(t, vals[i], oldVal)

		assert.Nil(t, m.Get(keys[i]))
	}

	assert.Equal(t, m.Count(), uint64(n)/2)

	for i := 1; i < n; i += 2 {
		assert.NotNil(t, m.Get(keys[i]))
		assert.True(t, m.ContainsKey(keys[i]))
		assert.True(t, m.ContainsValue(vals[i]))
	}

	for i := 1; i < n; i += 2 {
		oldVal, err := m.Del(keys[i])
		assert.Nil(t, err)
		assert.Equal(t, vals[i], oldVal)
	}

	assert.True(t, m.IsEmpty())

	for i := 0; i < n; i++ {
		assert.Nil(t, m.Get(keys[i]))
		assert.False(t, m.ContainsKey(keys[i]))
		assert.False(t, m.ContainsValue(vals[i]))
	}
}

func TestMap5(t *testing.T) {
	m, err := newMap(false, md5.Size, 16, 1, h1, h2, true)
	assert.Nil(t, err)

	n := 5_000_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = genRandomBytes(md5.Size)
	}

	for i := 0; i < n; i++ {
		oldVal, err := m.Put(keys[i], nil, true)
		if err != nil {
			panic(err)
		} else if oldVal != nil {
			panic(fmt.Sprintf("expected nil value, got %v", oldVal))
		}
	}

	m.debug = true
	m.sanityCheck()
}

func pickN(big []int, n int) []int {
	if n <= 0 {
		return nil
	}
	if n > len(big) {
		n = len(big)
	}
	cot := make([]int, n)
	arr := make([]int, n)
	k := (len(big) / n) * n
	r := rand2.NewSource(time.Now().UnixNano()).(rand2.Source64)
	for i, e := range big {
		var j int
		if i < k {
			j = i % n
		} else {
			j = int(r.Uint64()&0x7fff_ffff) % n
		}
		cot[j] += 1
		if int(r.Uint64())%cot[j] == 0 {
			arr[j] = e
		}
	}
	return arr
}

func intArrToMap(arr []int) map[int]struct{} {
	m := make(map[int]struct{})
	for _, e := range arr {
		m[e] = struct{}{}
	}
	return m
}

// Fuzzing test
func TestMap6(t *testing.T) {
	m, err := newMap(true, md5.Size, 3, 1, h1, h2, true)
	assert.Nil(t, err)

	n := 10000
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = genRandomBytes(md5.Size)
		vals[i] = genRandomBytes(md5.Size / 4)

		oldVal, err := m.Put(keys[i], vals[i], true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)
	}
	assert.Equal(t, m.Count(), uint64(n))
	t.Log(m)

	r := rand2.NewSource(time.Now().UnixNano()).(rand2.Source64)
	var p int
	for p == 0 {
		p = int(r.Uint64() % 5000)
	}
	indexes := make([]int, n)
	for i := range indexes {
		indexes[i] = i
	}
	keyIndexesToRemove := pickN(indexes, p)
	for _, idx := range keyIndexesToRemove {
		oldVal, err := m.Del(keys[idx])
		assert.Nil(t, err)
		assert.Equal(t, oldVal, vals[idx])
	}

	assert.Equal(t, int(m.Count()), len(keys)-len(keyIndexesToRemove))

	indexSet := intArrToMap(keyIndexesToRemove)
	for i := 0; i < n; i++ {
		if _, ok := indexSet[i]; ok {
			// Key-val removed
			assert.Nil(t, m.Get(keys[i]))
			assert.False(t, m.ContainsKey(keys[i]))
			assert.False(t, m.ContainsValue(vals[i]))
		} else {
			val := m.Get(keys[i])
			assert.Equal(t, val, vals[i])
			assert.True(t, m.ContainsKey(keys[i]))
			assert.True(t, m.ContainsValue(vals[i]))
		}
	}

	m.sanityCheck()
	t.Log(m)

	m.Clear()
	m.sanityCheck()
}

// In-expandable Map tests
func TestMap7(t *testing.T) {
	m, err := newMap(true, md5.Size, 2, 1, h1, h2, false)
	assert.Nil(t, err)

	b1 := genRandomBytes(md5.Size)
	oldVal, err := m.Put(b1, b1, true)
	assert.Nil(t, err)
	assert.Nil(t, oldVal)

	oldVal, err = m.Put(b1, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, oldVal, b1)

	b2 := genRandomBytes(md5.Size)
	oldVal, err = m.Put(b2, b2, true)
	assert.Nil(t, err)
	assert.Nil(t, oldVal)

	t.Log(m)
	assert.Equal(t, m.LoadFactor(), 1.0)

	oldVal, err = m.Put(b2, b1, true)
	assert.Nil(t, err)
	assert.Equal(t, oldVal, b2)

	b3 := genRandomBytes(md5.Size)
	oldVal, err = m.Put(b3, b3, true)
	assert.ErrorIs(t, err, ErrBucketIsFull)
	assert.Nil(t, oldVal)

	assert.Equal(t, m.Get(b1), b1)
	assert.True(t, m.ContainsKey(b1))
	assert.True(t, m.ContainsValue(b1))

	assert.Nil(t, m.Get(b3))
	assert.False(t, m.ContainsKey(b3))
	assert.False(t, m.ContainsValue(b3))

	assert.Nil(t, m.Get(nil))
	assert.False(t, m.ContainsKey(nil))
	assert.False(t, m.ContainsValue(nil))

	oldVal, err = m.Del(b1)
	assert.Nil(t, err)
	assert.Equal(t, oldVal, b1)

	assert.Equal(t, m.LoadFactor(), 0.5)

	oldVal, err = m.Put(b3, b3, true)
	assert.Nil(t, err)
	assert.Nil(t, oldVal)
	assert.Equal(t, m.LoadFactor(), 1.0)

	assert.Nil(t, m.Get(b1))
	assert.False(t, m.ContainsKey(b1))
	assert.False(t, m.ContainsValue(b1))

	assert.Equal(t, m.Get(b3), b3)
	assert.True(t, m.ContainsKey(b3))
	assert.True(t, m.ContainsValue(b3))

	t.Log(m)
}

func BenchmarkMap1(b *testing.B) {
	m, err := newMap(false, md5.Size, 16, 1, h1, h2, true)
	if err != nil {
		panic(err)
	}

	n := 5_000_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = genRandomBytes(md5.Size)
	}

	b.ResetTimer()
	for i := 0; i < n; i++ {
		oldVal, err := m.Put(keys[i], nil, true)
		if err != nil {
			panic(err)
		} else if oldVal != nil {
			panic(fmt.Sprintf("expected nil value, got %v", oldVal))
		}
	}
}

func BenchmarkMap2(b *testing.B) {
	// With preset bucket count, no expansion are needed
	m, err := newMap(false, md5.Size, 16, 524_288, h1, h2, true)
	if err != nil {
		panic(err)
	}

	n := 5_000_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = genRandomBytes(md5.Size)
	}

	b.ResetTimer()
	for i := 0; i < n; i++ {
		oldVal, err := m.Put(keys[i], nil, true)
		if err != nil {
			panic(err)
		} else if oldVal != nil {
			panic(fmt.Sprintf("expected nil value, got %v", oldVal))
		}
	}
}
