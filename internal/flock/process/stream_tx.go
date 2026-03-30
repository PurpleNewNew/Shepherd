package process

import (
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	streamDefaultWindow   = 32
	streamRetransTimeout  = 5 * time.Second
	streamMaxRetrans      = 6
	streamWindowSleepStep = 10 * time.Millisecond
)

func (agent *Agent) streamUplinkConn(streamID uint32) (net.Conn, string, bool) {
	if agent == nil {
		return nil, "", false
	}
	sess := agent.currentSession()
	if sess == nil {
		return nil, "", false
	}
	secret := sess.Secret()
	sessConn := sess.Conn()
	var conn net.Conn
	sameConn := func(a, b net.Conn) bool {
		if a == nil || b == nil {
			return false
		}
		if a == b {
			return true
		}
		if sc, ok := a.(*utils.SafeConn); ok && sc != nil && sc.Conn != nil {
			a = sc.Conn
		}
		if sc, ok := b.(*utils.SafeConn); ok && sc != nil && sc.Conn != nil {
			b = sc.Conn
		}
		return a == b
	}
	agent.streamMu.Lock()
	if agent.streams != nil {
		if st := agent.streams[streamID]; st != nil && st.replyConn != nil {
			// replyConn records the connection on which STREAM_OPEN arrived. If the upstream
			// session has since been replaced (reconnect/sleep), the saved conn becomes stale
			// and will wedge retransmission loops by repeatedly writing to a closed socket.
			// Prefer the current session connection in that case.
			if sessConn != nil && !st.replyIsSupplemental && !sameConn(st.replyConn, sessConn) {
				st.replyConn = nil
			} else {
				conn = st.replyConn
			}
		}
	}
	agent.streamMu.Unlock()
	if conn == nil {
		conn = sessConn
	}
	if conn == nil {
		return nil, secret, false
	}
	return conn, secret, true
}

func (agent *Agent) sendReliableStreamData(streamID uint32, payload []byte) {
	if agent == nil || len(payload) == 0 {
		return
	}
	for {
		agent.streamMu.Lock()
		if agent.streams == nil {
			agent.streamMu.Unlock()
			return
		}
		st := agent.streams[streamID]
		if st == nil || st.closing {
			agent.streamMu.Unlock()
			return
		}
		if st.txWindow <= 0 {
			st.txWindow = streamDefaultWindow
		}
		if st.pending == nil {
			st.pending = make(map[uint32]*pendingFrame)
		}
		if len(st.pending) < st.txWindow {
			seq := st.txSeq + 1
			st.txSeq = seq
			frame := &pendingFrame{payload: append([]byte(nil), payload...)}
			st.pending[seq] = frame
			ack := st.lastAck
			window := st.txWindow
			agent.streamMu.Unlock()
			agent.dispatchStreamFrame(streamID, seq, ack, window, frame)
			return
		}
		agent.streamMu.Unlock()
		time.Sleep(streamWindowSleepStep)
	}
}

func (agent *Agent) dispatchStreamFrame(streamID, seq uint32, ack uint32, window int, frame *pendingFrame) {
	if agent == nil || frame == nil {
		return
	}
	// Treat stream TX as activity to prevent the sleep manager from tearing down the
	// upstream session mid-transfer (which can wedge streams and dataplane downloads).
	agent.noteActivity()
	conn, secret, ok := agent.streamUplinkConn(streamID)
	if !ok || conn == nil {
		agent.scheduleStreamRetry(streamID, seq, frame)
		return
	}
	frame.attempts++
	msg := &protocol.StreamData{StreamID: streamID, Seq: seq, Ack: ack, Window: uint16(window)}
	if len(frame.payload) > 0 {
		msg.Payload = append([]byte(nil), frame.payload...)
	}
	up := protocol.NewUpMsg(conn, secret, agent.UUID)
	header := &protocol.Header{Sender: agent.UUID, Accepter: protocol.ADMIN_UUID, MessageType: uint16(protocol.STREAM_DATA), RouteLen: uint32(len([]byte(protocol.TEMP_ROUTE))), Route: protocol.TEMP_ROUTE}
	protocol.ConstructMessage(up, header, msg, false)
	up.SendMessage()
	agent.armStreamTimer(streamID, seq, frame)
}

func (agent *Agent) armStreamTimer(streamID, seq uint32, frame *pendingFrame) {
	if frame.timer != nil {
		frame.timer.Stop()
	}
	frame.timer = time.AfterFunc(streamRetransTimeout, func() {
		agent.onStreamFrameTimeout(streamID, seq)
	})
}

func (agent *Agent) scheduleStreamRetry(streamID, seq uint32, frame *pendingFrame) {
	if frame == nil {
		return
	}
	if frame.timer != nil {
		frame.timer.Stop()
	}
	frame.timer = time.AfterFunc(streamRetransTimeout, func() {
		agent.onStreamFrameTimeout(streamID, seq)
	})
}

func (agent *Agent) onStreamFrameTimeout(streamID, seq uint32) {
	agent.streamMu.Lock()
	if agent.streams == nil {
		agent.streamMu.Unlock()
		return
	}
	st := agent.streams[streamID]
	if st == nil {
		agent.streamMu.Unlock()
		return
	}
	frame := st.pending[seq]
	if frame == nil {
		agent.streamMu.Unlock()
		return
	}
	if frame.attempts >= streamMaxRetrans {
		delete(st.pending, seq)
		agent.streamMu.Unlock()
		agent.sendStreamClose(streamID, 1, "stream timeout")
		return
	}
	ack := st.lastAck
	window := st.txWindow
	agent.streamMu.Unlock()
	agent.dispatchStreamFrame(streamID, seq, ack, window, frame)
}

// sendStreamData wraps the reliable sender for non-SOCKS consumers.
func (agent *Agent) sendStreamData(streamID uint32, data []byte) {
	agent.sendReliableStreamData(streamID, data)
}

func (agent *Agent) emitStreamClose(streamID uint32, code uint16, reason string, pinnedReplyConn net.Conn) {
	if agent == nil {
		return
	}
	agent.noteActivity()
	sess := agent.currentSession()
	secret := ""
	if sess != nil {
		secret = sess.Secret()
	}
	if secret == "" && agent.mgr != nil {
		secret = agent.mgr.ActiveSecret()
	}
	if secret == "" {
		return
	}
	var sessConn net.Conn
	if sess != nil {
		sessConn = sess.Conn()
	}
	replyConn := pinnedReplyConn

	// STREAM_CLOSE is not ACKed by the stream layer. Under churn (sleep/reconnect/repair),
	// sending the close on both the per-stream replyConn (when available) and the current
	// upstream session connection significantly reduces dataplane upload hangs.
	sameConn := func(a, b net.Conn) bool {
		if a == nil || b == nil {
			return false
		}
		if a == b {
			return true
		}
		if sc, ok := a.(*utils.SafeConn); ok && sc != nil && sc.Conn != nil {
			a = sc.Conn
		}
		if sc, ok := b.(*utils.SafeConn); ok && sc != nil && sc.Conn != nil {
			b = sc.Conn
		}
		return a == b
	}
	send := func(conn net.Conn) {
		if conn == nil {
			return
		}
		up := protocol.NewUpMsg(conn, secret, agent.UUID)
		h := &protocol.Header{Sender: agent.UUID, Accepter: protocol.ADMIN_UUID, MessageType: uint16(protocol.STREAM_CLOSE), RouteLen: uint32(len([]byte(protocol.TEMP_ROUTE))), Route: protocol.TEMP_ROUTE}
		msg := &protocol.StreamClose{StreamID: streamID, Code: code, Reason: reason}
		protocol.ConstructMessage(up, h, msg, false)
		up.SendMessage()
	}

	if replyConn != nil {
		send(replyConn)
		if sessConn != nil && !sameConn(sessConn, replyConn) {
			send(sessConn)
		}
		return
	}
	send(sessConn)
}

// sendStreamClose emits a STREAM_CLOSE frame to admin.
func (agent *Agent) sendStreamClose(streamID uint32, code uint16, reason string) {
	// Snapshot replyConn at call time so duplicates still follow the same uplink even if the
	// stream state entry is removed before the timers fire (common for inbound file-put/proxy).
	var pinnedReplyConn net.Conn
	agent.streamMu.Lock()
	if agent.streams != nil {
		if st := agent.streams[streamID]; st != nil {
			pinnedReplyConn = st.replyConn
		}
	}
	agent.streamMu.Unlock()

	// Best-effort reliability: close frames are not ACKed today.
	// Under reconnect/sleep churn, sending a couple of duplicates significantly reduces hangs.
	agent.emitStreamClose(streamID, code, reason, pinnedReplyConn)
	time.AfterFunc(2*time.Second, func() { agent.emitStreamClose(streamID, code, reason, pinnedReplyConn) })
	time.AfterFunc(6*time.Second, func() { agent.emitStreamClose(streamID, code, reason, pinnedReplyConn) })
	agent.cleanupStreamPending(streamID)
}

func (agent *Agent) handleStreamAck(ack *protocol.StreamAck) {
	if agent == nil || ack == nil {
		return
	}
	agent.streamMu.Lock()
	if agent.streams == nil {
		agent.streamMu.Unlock()
		return
	}
	st := agent.streams[ack.StreamID]
	if st == nil {
		agent.streamMu.Unlock()
		return
	}
	if ack.Ack > st.lastAck {
		st.lastAck = ack.Ack
	}
	for seq, frame := range st.pending {
		if seq <= ack.Ack {
			if frame.timer != nil {
				frame.timer.Stop()
			}
			delete(st.pending, seq)
		}
	}
	agent.streamMu.Unlock()
}

func (agent *Agent) cleanupStreamPending(streamID uint32) {
	agent.streamMu.Lock()
	if agent.streams == nil {
		agent.streamMu.Unlock()
		return
	}
	st := agent.streams[streamID]
	if st == nil {
		agent.streamMu.Unlock()
		return
	}
	st.closing = true
	for seq, frame := range st.pending {
		if frame.timer != nil {
			frame.timer.Stop()
		}
		delete(st.pending, seq)
	}
	agent.streamMu.Unlock()
}
