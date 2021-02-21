package cuckoohash

import (
	"github.com/OneOfOne/xxhash"
	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	h1 = farm.Hash64WithSeed
	h2 = xxhash.Checksum64S
)

func TestNewMap(t *testing.T) {
	m, err := newMap(true, 1, 1, 1, h1, h2, true)
	assert.Nil(t, err)
	assert.True(t, m.IsEmpty())
	assert.Equal(t, 0.0, m.LoadFactor())
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

func TestNewMap2(t *testing.T) {
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
	}
}
