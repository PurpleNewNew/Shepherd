package streamid

import (
    "sync/atomic"
    "time"
)

var counter uint32 = uint32(time.Now().UnixNano())

// Next returns a non-zero monotonically increasing stream id.
func Next() uint32 {
    v := atomic.AddUint32(&counter, 1)
    if v == 0 {
        v = atomic.AddUint32(&counter, 1)
    }
    return v
}
