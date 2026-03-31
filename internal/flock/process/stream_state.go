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
	// replyConn 是这个 stream 优先使用的上行连接（如果存在）。
	// 如果某个 stream 是通过 supplemental 链路打开的，那么 agent->admin 帧
	// 沿原始连接返回，就能在主上游处于睡眠或重连时继续维持这条流。
	replyConn net.Conn
	// replyIsSupplemental 表示 replyConn 来自一条 supplemental 边
	// （也就是它与 STREAM_OPEN 时的当前上游会话连接不同）。
	// 这种情况下，不能仅仅因为上游会话重连就清掉 replyConn；
	// 固定这条 supplemental 路径本身就是这里的目的。
	replyIsSupplemental bool
	// lastAck 记录最近一次已确认的出站序号（agent -> admin）。
	lastAck uint32
	// rxAck 记录我们已经连续应用到的最高入站序号（admin -> agent）。
	// 乱序帧会先缓存在 rxBuf 中，只有补齐空洞后才推进 rxAck。
	rxAck    uint32
	rxBuf    map[uint32][]byte
	txSeq    uint32
	txWindow int
	pending  map[uint32]*pendingFrame
	closing  bool
	// 某些 kind 使用的临时标记（例如 ssh-tunnel）
	started bool
}

type pendingFrame struct {
	payload  []byte
	attempts int
	timer    *time.Timer
}
