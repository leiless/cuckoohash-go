package cuckoohash

import "errors"

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrBucketIsFull    = errors.New("bucket is full")
	ErrKeyNotFound     = errors.New("key not found")
)
