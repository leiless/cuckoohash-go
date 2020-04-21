package cuckoohash

import "fmt"

func debug(format string, a ...interface{}) {
	_, _ = fmt.Printf("[DBG] " + format + "\n", a...)
}

