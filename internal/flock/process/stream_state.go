package process

import (
	"net"
	"time"
)

type streamState struct {
	options   string
	sessionID string
	kind      string
	meta      map[string]string
	// replyConn is the preferred uplink connection for this stream (when available).
	// If a stream was opened via a supplemental link, using that origin connection
	// for agent->admin frames keeps the flow alive even if the primary upstream is
	// sleeping/reconnecting.
	replyConn net.Conn
	// replyIsSupplemental marks that replyConn came from a supplemental edge (i.e. it
	// differs from the current upstream session conn at STREAM_OPEN time). In that
	// case we must not clear replyConn just because the upstream session reconnects;
	// the supplemental path is the whole point of the pin.
	replyIsSupplemental bool
	// lastAck tracks the last acknowledged outbound seq (agent -> admin).
	lastAck uint32
	// rxAck tracks the highest contiguous inbound seq we have applied (admin -> agent).
	// We buffer out-of-order frames in rxBuf and only advance rxAck when gaps are filled.
	rxAck    uint32
	rxBuf    map[uint32][]byte
	txSeq    uint32
	txWindow int
	pending  map[uint32]*pendingFrame
	closing  bool
	// ephemeral flags for certain kinds (e.g., ssh-tunnel)
	started bool
}

type pendingFrame struct {
	payload  []byte
	attempts int
	timer    *time.Timer
}
