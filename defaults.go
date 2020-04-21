package cuckoohash

const (
	DefaultBytesPerKey = 1
	DefaultKeysPerBucket = 4
	DefaultBuckets = 1024
)

var (
	masks = [64]uint64{}
)

func init() {
	for i := range masks {
		masks[i] = (1 << i) - 1
	}
}

func nextPowerOfTwo(n uint32) uint32 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

