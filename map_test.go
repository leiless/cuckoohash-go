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
		b := []byte{byte(i)}
		assert.Nil(t, m.Get(b))
		assert.False(t, m.ContainsKey(b))
		assert.False(t, m.ContainsValue(b))
	}
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			b := []byte{byte(i), byte(j)}
			assert.Nil(t, m.Get(b))
			assert.False(t, m.ContainsKey(b))
			assert.False(t, m.ContainsValue(b))
		}
	}
	assert.False(t, m.ContainsKey(nil))
	assert.False(t, m.ContainsValue(nil))

	m.Clear()
	t.Log(m)

	for i := 0; i < 256; i++ {
		b := []byte{byte(i)}
		oldVal, err := m.Put(b, b, true)
		assert.Nil(t, err)
		assert.Nil(t, oldVal)
	}

	t.Log(m)
}
