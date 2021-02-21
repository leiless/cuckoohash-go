package cuckoohash

import (
	"github.com/OneOfOne/xxhash"
	gofarm "github.com/dgryski/go-farm"
	"testing"
)

func TestNewMap(t *testing.T) {
	m, err := newMap(true, 1, 1, 1, gofarm.Hash64WithSeed, xxhash.Checksum64S, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(m)
}
