package main

import (
	"sync/atomic"
	"time"
)

// Snowflake ID: 41-bit Unix millisecond timestamp in the high bits,
// 22-bit monotonic sequence in the low bits.
// Result is a positive int64, monotonically increasing with time.

var seq uint64

func NewSnowflakeID() int64 {
	s := atomic.AddUint64(&seq, 1) & 0x3FFFFF // 22 bits
	ms := uint64(time.Now().UnixMilli())
	return int64((ms << 22) | s)
}
