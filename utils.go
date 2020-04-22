package cuckoohash

import (
	"fmt"
	"strconv"
)

func debug(format string, a ...interface{}) {
	_, _ = fmt.Printf("[DBG] " + format + "\n", a...)
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

	result := strconv.FormatFloat(value, 'g', 2, 64)
	return result + unit
}

