package cuckoohash

type Hasher interface {
	Hash64WithSeed(b []byte, seed uint64) uint64
}
