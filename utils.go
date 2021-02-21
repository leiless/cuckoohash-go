package cuckoohash

import (
	"fmt"
	"strconv"
	"strings"
)

func debug(format string, a ...interface{}) {
	fmt.Printf("[DBG] "+format+"\n", a...)
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

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
)

// Taken from https://github.com/cloudfoundry/bytefmt/blob/master/bytes.go with modification
func formatBytes(bytes uint64) string {
	unit := ""
	value := float64(bytes)

	switch {
	case bytes >= TERABYTE:
		unit = "T"
		value = value / TERABYTE
	case bytes >= GIGABYTE:
		unit = "G"
		value = value / GIGABYTE
	case bytes >= MEGABYTE:
		unit = "M"
		value = value / MEGABYTE
	case bytes >= KILOBYTE:
		unit = "K"
		value = value / KILOBYTE
	case bytes >= BYTE:
		unit = "B"
	case bytes == 0:
		return "0B"
	}

	result := strconv.FormatFloat(value, 'f', 1, 64)
	result = strings.TrimSuffix(result, ".0")
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
