package streamid

import (
	"sync/atomic"
	"time"
)

var counter uint32 = uint32(time.Now().UnixNano())

// Next 返回一个非零且单调递增的 stream id。
func Next() uint32 {
	v := atomic.AddUint32(&counter, 1)
	if v == 0 {
		v = atomic.AddUint32(&counter, 1)
	}
	return v
}
