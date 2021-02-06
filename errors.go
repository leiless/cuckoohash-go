package cuckoohash

import "errors"

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrBucketIsFull    = errors.New("bucket is full")
)
