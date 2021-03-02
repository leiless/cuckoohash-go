package cuckoohash

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet1(t *testing.T) {
	s, err := newSet(1, 1, 1, h1, h2, true, false)
	assert.Nil(t, err)
	assert.True(t, s.IsEmpty())
	t.Log(s)

	b1 := []byte{0}
	assert.True(t, s.Put(b1))
	assert.True(t, s.Contains(b1))
	assert.Equal(t, s.Count(), uint64(1))
	assert.Equal(t, s.LoadFactor(), 1.0)
	t.Log(s)

	assert.True(t, s.Del(b1))
	assert.True(t, s.IsEmpty())

	assert.True(t, s.Put(b1))
	assert.Equal(t, s.Count(), uint64(1))

	s.Clear()
	assert.True(t, s.IsEmpty())

	assert.True(t, s.Put(b1))
	assert.True(t, s.Put(b1))

	b2 := []byte{1}
	assert.False(t, s.Put(b2))

	assert.Equal(t, s.Count(), uint64(1))
}
