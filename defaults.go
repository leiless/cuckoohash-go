package cuckoohash

const (
	DefaultBytesPerKey   = 1
	DefaultKeysPerBucket = 8
	DefaultBuckets       = 1
)

var (
	masks = [64]uint64{}
)

func init() {
	for i := range masks {
		masks[i] = (uint64(1) << i) - 1
	}
}
