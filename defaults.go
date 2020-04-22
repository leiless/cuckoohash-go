package cuckoohash

const (
	DefaultBytesPerKey = 1
	DefaultKeysPerBucket = 8
	DefaultBuckets = 1
)

var (
	masks = [64]uint64{}
)

func init() {
	for i := range masks {
		masks[i] = (1 << i) - 1
	}
}

// If n already power of 2, return value will be n itself
// If n is 0 or 0xffffffff, zero is returned
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

