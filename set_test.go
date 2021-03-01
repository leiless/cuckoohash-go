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
	err = s.Put(b1, true)
	assert.Nil(t, err)
	assert.True(t, s.Contains(b1))
	assert.Equal(t, s.Count(), uint64(1))
	assert.Equal(t, s.LoadFactor(), 1.0)
	t.Log(s)

	err = s.Del(b1)
	assert.Nil(t, err)
	assert.True(t, s.IsEmpty())

	err = s.Put(b1, true)
	assert.Nil(t, err)
	assert.Equal(t, s.Count(), uint64(1))

	s.Clear()
	assert.True(t, s.IsEmpty())

	err = s.Put(b1, true)
	assert.Nil(t, err)
	err = s.Put(b1, true)
	assert.Nil(t, err)

	b2 := []byte{1}
	err = s.Put(b2, true)
	assert.ErrorIs(t, err, ErrBucketIsFull)

	assert.Equal(t, s.Count(), uint64(1))
}
