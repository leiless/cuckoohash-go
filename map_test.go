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
	"testing"
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
