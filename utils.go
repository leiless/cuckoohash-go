package cuckoohash

import (
	"fmt"
	"strconv"
)

func debug(format string, a ...interface{}) {
	_, _ = fmt.Printf("[DBG] "+format+"\n", a...)
}

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
)

// Taken from https://github.com/cloudfoundry/bytefmt/blob/master/bytes.go with modification
func formatBytes(bytes uint64) string {
	unit := ""
	value := float64(bytes)

	switch {
	case bytes >= GIGABYTE:
		unit = "GiB"
		value = value / GIGABYTE
	case bytes >= MEGABYTE:
		unit = "MiB"
		value = value / MEGABYTE
	case bytes >= KILOBYTE:
		unit = "KiB"
		value = value / KILOBYTE
	case bytes >= BYTE:
		unit = "B"
	}

	result := strconv.FormatFloat(value, 'f', 1, 64)
	return result + unit
}

// Code taken from java.util.Arrays#hashCode()
// see: https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/Arrays.java#L4377
func simpleHash(a []byte) uint64 {
	if len(a) == 0 {
		return 0
	}

	h := uint64(1)
	for _, b := range a {
		h = uint64(31)*h + uint64(b)
	}
	return h
}

func byteSliceEquals(lhs, rhs []byte) bool {
	if len(lhs) == len(rhs) {
		for i := 0; i < len(lhs); i++ {
			if lhs[i] != rhs[i] {
				return false
			}
		}
		return true
	}
	return false
}
