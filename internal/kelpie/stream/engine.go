package stream

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/share/streamopts"
	"codeberg.org/agnoie/shepherd/pkg/streamid"
	"codeberg.org/agnoie/shepherd/protocol"
)

// Config defines DTN-Stream 行为参数。
type Config struct {
	ChunkBytes    int
	WindowFrames  int
	MinWindow     int
	InitialWindow int
	RetransLimit  int
	BaseRTO       time.Duration
	MinRTO        time.Duration
	MaxRTO        time.Duration
	IdleTimeout   time.Duration
	MaxStream     int
}

func DefaultConfig() Config {
	return Config{
		ChunkBytes:    768,
		WindowFrames:  32,
		MinWindow:     4,
		InitialWindow: 8,
		RetransLimit:  8,
		BaseRTO:       3 * time.Second,
		MinRTO:        1 * time.Second,
		MaxRTO:        30 * time.Second,
		IdleTimeout:   30 * time.Second,
		MaxStream:     64,
	}
}

type Engine struct {
	cfg     Config
	clock   func() time.Time
	mux     sync.Mutex
	streams map[uint32]*streamSession
	sendFn  func(target string, payload []byte) error
	logFn   func(format string, args ...interface{})
}

type Options struct {
	Target string
	Meta   map[string]string
}

func New(cfg Config, send func(target string, payload []byte) error, logf func(format string, args ...interface{})) *Engine {
	def := DefaultConfig()
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = def.ChunkBytes
	}
	if cfg.WindowFrames <= 0 {
		cfg.WindowFrames = def.WindowFrames
	}
	if cfg.MinWindow <= 0 || cfg.MinWindow > cfg.WindowFrames {
		cfg.MinWindow = def.MinWindow
		if cfg.MinWindow > cfg.WindowFrames {
			cfg.MinWindow = cfg.WindowFrames
		}
	}
	if cfg.InitialWindow <= 0 {
		cfg.InitialWindow = def.InitialWindow
	}
	if cfg.InitialWindow > cfg.WindowFrames {
		cfg.InitialWindow = cfg.WindowFrames
	}
	if cfg.InitialWindow < cfg.MinWindow {
		cfg.InitialWindow = cfg.MinWindow
	}
	if cfg.RetransLimit <= 0 {
		cfg.RetransLimit = def.RetransLimit
	}
	if cfg.MinRTO <= 0 {
		cfg.MinRTO = def.MinRTO
	}
	if cfg.MaxRTO <= 0 || cfg.MaxRTO < cfg.MinRTO {
		cfg.MaxRTO = def.MaxRTO
	}
	if cfg.BaseRTO <= 0 {
		cfg.BaseRTO = def.BaseRTO
	}
	if cfg.BaseRTO < cfg.MinRTO {
		cfg.BaseRTO = cfg.MinRTO
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = def.IdleTimeout
	}
	if cfg.MaxStream <= 0 {
		cfg.MaxStream = def.MaxStream
	}
	eng := &Engine{
		cfg:     cfg,
		clock:   time.Now,
		streams: make(map[uint32]*streamSession),
		sendFn:  send,
		logFn:   logf,
	}
	return eng
}

func (e *Engine) Now() time.Time {
	if e.clock != nil {
		return e.clock()
	}
	return time.Now()
}

// Open 创建一个新的 Stream，用于在 DTN 上发送可靠流。
func (e *Engine) Open(ctx context.Context, target string, opt Options) (*Stream, error) {
	if e == nil {
		return nil, fmt.Errorf("engine nil")
	}
	if strings.TrimSpace(target) == "" {
		return nil, fmt.Errorf("empty target")
	}
	sess, err := e.newSession(target, opt)
	if err != nil {
		return nil, err
	}
	sendR, sendW := io.Pipe()
	recvR, recvW := io.Pipe()
	stream := &Stream{
		engine:  e,
		session: sess,
		sendR:   sendR,
		sendW:   sendW,
		recvR:   recvR,
		recvW:   recvW,
		ctx:     ctx,
	}
	sess.bindStream(stream)
	go stream.runSender()
	return stream, nil
}

// Stream 在本地表现为 io.ReadWriteCloser。
type Stream struct {
	engine  *Engine
	session *streamSession
	sendR   *io.PipeReader
	sendW   *io.PipeWriter
	recvR   *io.PipeReader
	recvW   *io.PipeWriter
	ctx     context.Context
}

func (s *Stream) ID() uint32 {
	if s == nil || s.session == nil {
		return 0
	}
	return s.session.streamID
}

func (s *Stream) Read(p []byte) (int, error) {
	if s == nil || s.recvR == nil {
		return 0, io.EOF
	}
	return s.recvR.Read(p)
}

func (s *Stream) Write(p []byte) (int, error) {
	if s == nil || s.sendW == nil {
		return 0, io.ErrClosedPipe
	}
	return s.sendW.Write(p)
}

// CloseWrite signals end-of-input to the remote side while keeping the receive side open.
// This is useful for protocols that need a remote "finalize + close" acknowledgement
// (e.g. file-put validation) before the caller considers the operation complete.
func (s *Stream) CloseWrite() error {
	if s == nil || s.sendW == nil {
		return nil
	}
	return s.sendW.Close()
}

func (s *Stream) Close() error {
	if s == nil {
		return nil
	}
	if s.sendW != nil {
		s.sendW.Close()
	}
	if s.sendR != nil {
		s.sendR.Close()
	}
	if s.recvW != nil {
		s.recvW.Close()
	}
	if s.recvR != nil {
		s.recvR.Close()
	}
	if s.session != nil {
		s.session.Close()
		if s.engine != nil {
			s.engine.removeSession(s.session.streamID)
		}
	}
	return nil
}

func (s *Stream) runSender() {
	if s == nil || s.session == nil {
		return
	}
	buffer := make([]byte, s.engine.cfg.ChunkBytes)
	for {
		n, err := s.sendR.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			s.session.Enqueue(chunk)
		}
		if err != nil {
			if err != io.EOF && err != io.ErrClosedPipe {
				s.session.SetError(err)
			}
			s.session.MarkEOF()
			return
		}
	}
}

// streamSession 表示单个 DTN-Stream 会话。
type streamSession struct {
	engine   *Engine
	target   string
	options  Options
	streamID uint32
	outbound bool

	mu sync.Mutex

	seq           uint32
	ack           uint32
	rxAck         uint32
	rxBuf         map[uint32][]byte
	inflight      map[uint32]*frame
	pending       [][]byte
	closed        bool
	closing       bool
	closeCode     uint16
	closeReason   string
	stream        *Stream
	recvClosed    bool
	window        int
	minWindow     int
	maxWindow     int
	ssthresh      int
	aiCounter     int
	srtt          time.Duration
	rttvar        time.Duration
	rto           time.Duration
	rttSamples    int
	lastActivity  time.Time
	throughputEMA float64
	lastAckUpdate time.Time
	lastAckSeq    uint32

	sendCond *sync.Cond
}

func (e *Engine) newSession(target string, opt Options) (*streamSession, error) {
	sess := &streamSession{
		engine:   e,
		target:   target,
		options:  opt,
		streamID: streamid.Next(),
		inflight: make(map[uint32]*frame),
		outbound: true,
	}
	sess.minWindow = e.cfg.MinWindow
	sess.maxWindow = e.cfg.WindowFrames
	sess.window = e.cfg.InitialWindow
	sess.ssthresh = e.cfg.WindowFrames
	sess.rto = e.cfg.BaseRTO
	sess.lastActivity = e.Now()
	sess.sendCond = sync.NewCond(&sess.mu)
	e.mux.Lock()
	if e.cfg.MaxStream > 0 && len(e.streams) >= e.cfg.MaxStream {
		e.mux.Unlock()
		return nil, fmt.Errorf("stream limit reached")
	}
	e.streams[sess.streamID] = sess
	e.mux.Unlock()
	go sess.run()
	return sess, nil
}

func (s *streamSession) bindStream(stream *Stream) {
	s.mu.Lock()
	s.stream = stream
	s.mu.Unlock()
}

func (s *streamSession) windowSize() int {
	if s == nil || s.engine == nil {
		return 0
	}
	if s.window <= 0 {
		return s.engine.cfg.WindowFrames
	}
	return s.window
}

func (s *streamSession) touchLocked(now time.Time) {
	if s == nil {
		return
	}
	if now.IsZero() && s.engine != nil {
		now = s.engine.Now()
	}
	s.lastActivity = now
}

func (s *streamSession) currentRTOLocked() time.Duration {
	if s == nil || s.engine == nil {
		return 0
	}
	rto := s.rto
	if rto <= 0 {
		rto = s.engine.cfg.BaseRTO
	}
	if rto < s.engine.cfg.MinRTO {
		rto = s.engine.cfg.MinRTO
	}
	if s.engine.cfg.MaxRTO > 0 && rto > s.engine.cfg.MaxRTO {
		rto = s.engine.cfg.MaxRTO
	}
	return rto
}

func (s *streamSession) observeRTTLocked(sample time.Duration) {
	if sample <= 0 {
		return
	}
	if s.rttSamples == 0 {
		s.srtt = sample
		s.rttvar = sample / 2
		s.rto = s.srtt + 4*s.rttvar
		s.rttSamples = 1
		return
	}
	delta := sample - s.srtt
	if delta < 0 {
		delta = -delta
	}
	s.rttvar = (3*s.rttvar + delta) / 4
	s.srtt = ((7 * s.srtt) + sample) / 8
	s.rto = s.srtt + 4*s.rttvar
	s.rttSamples++
}

func (s *streamSession) adjustWindowOnAckLocked(acked int) {
	if acked <= 0 {
		return
	}
	window := s.windowSize()
	if window <= 0 {
		return
	}
	s.aiCounter += acked
	// 慢启动：窗口低于阈值时快速增大；否则线性增大
	if s.window < s.ssthresh {
		for i := 0; i < acked; i++ {
			if s.window < s.maxWindow {
				s.window++
			}
		}
	} else {
		for s.aiCounter >= window {
			if s.window < s.maxWindow {
				s.window++
			} else {
				s.aiCounter = 0
				break
			}
			s.aiCounter -= window
			window = s.windowSize()
		}
	}
	s.sendCond.Signal()
}

func (s *streamSession) reduceWindowOnLossLocked() {
	window := s.windowSize()
	if window <= 1 {
		return
	}
	window = window / 2
	if window < s.minWindow {
		window = s.minWindow
	}
	if window < 1 {
		window = 1
	}
	// 更新慢启动阈值为当前窗口（乘法减小），下次从阈值以下重新慢启动
	s.ssthresh = window
	s.window = window
	s.aiCounter = 0
	s.sendCond.Signal()
}

func (s *streamSession) updateThroughputLocked(seq uint32, now time.Time) {
	if seq == 0 || s.engine == nil {
		return
	}
	if s.lastAckUpdate.IsZero() {
		s.lastAckUpdate = now
		s.lastAckSeq = seq
		return
	}
	if seq <= s.lastAckSeq {
		return
	}
	delta := seq - s.lastAckSeq
	dt := now.Sub(s.lastAckUpdate).Seconds()
	if dt <= 0 {
		return
	}
	bytes := float64(delta) * float64(s.engine.cfg.ChunkBytes)
	rate := bytes / dt
	if rate <= 0 {
		return
	}
	if s.throughputEMA == 0 {
		s.throughputEMA = rate
	} else {
		s.throughputEMA = 0.8*s.throughputEMA + 0.2*rate
	}
	s.lastAckSeq = seq
	s.lastAckUpdate = now
}

func (s *streamSession) adjustWindowForCapacityLocked() {
	target := s.capacityWindowLocked()
	if target <= 0 {
		return
	}
	if target > s.window {
		s.window++
	} else if target < s.window {
		s.window--
	}
}

func (s *streamSession) capacityWindowLocked() int {
	if s.engine == nil {
		return s.window
	}
	srtt := s.srtt
	if srtt <= 0 {
		srtt = s.engine.cfg.BaseRTO
	}
	if srtt <= 0 {
		return s.window
	}
	bandwidth := s.throughputEMA
	if bandwidth <= 0 {
		return s.window
	}
	bdpBytes := bandwidth * srtt.Seconds()
	chunk := float64(s.engine.cfg.ChunkBytes)
	if chunk <= 0 {
		chunk = 1
	}
	target := int(math.Ceil(bdpBytes / chunk))
	if target < s.minWindow {
		target = s.minWindow
	}
	if target > s.maxWindow {
		target = s.maxWindow
	}
	return target
}

func (s *streamSession) removeAckedLocked(seq uint32) []*frame {
	if s == nil {
		return nil
	}
	removed := make([]*frame, 0)
	for id, frm := range s.inflight {
		if id <= seq {
			removed = append(removed, frm)
			delete(s.inflight, id)
		}
	}
	return removed
}

func (s *streamSession) Enqueue(chunk []byte) {
	s.mu.Lock()
	s.pending = append(s.pending, chunk)
	s.sendCond.Signal()
	s.mu.Unlock()
}

func (s *streamSession) MarkEOF() {
	s.mu.Lock()
	s.closing = true
	s.sendCond.Signal()
	s.mu.Unlock()
}

func (s *streamSession) SetError(err error) {
	s.mu.Lock()
	s.closeCode = 1
	s.closeReason = err.Error()
	s.closing = true
	s.sendCond.Signal()
	s.mu.Unlock()
}

func (s *streamSession) Close() {
	s.mu.Lock()
	s.closed = true
	s.sendCond.Broadcast()
	code := s.closeCode
	reason := strings.TrimSpace(s.closeReason)
	s.mu.Unlock()
	if s.stream != nil && s.stream.recvW != nil && !s.recvClosed {
		if code != 0 && reason != "" {
			s.stream.recvW.CloseWithError(errors.New(reason))
		} else {
			s.stream.recvW.Close()
		}
		s.recvClosed = true
	}
}

func (s *streamSession) Abort(reason string) {
	if s == nil {
		return
	}
	if reason == "" {
		reason = "stream closed by admin"
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	for _, frm := range s.inflight {
		if frm != nil && frm.timer != nil {
			frm.timer.Stop()
		}
	}
	s.inflight = make(map[uint32]*frame)
	s.pending = nil
	s.closeCode = 1
	s.closeReason = reason
	s.closed = true
	s.closing = true
	s.sendCond.Broadcast()
	s.mu.Unlock()
	s.sendClose()
	if s.engine != nil {
		s.engine.removeSession(s.streamID)
	}
	if s.stream != nil && s.stream.recvW != nil && !s.recvClosed {
		reason := strings.TrimSpace(reason)
		if reason == "" {
			reason = "stream aborted"
		}
		s.stream.recvW.CloseWithError(errors.New(reason))
		s.recvClosed = true
	}
}

func (s *streamSession) Ack(seq uint32) {
	if s == nil || s.engine == nil {
		return
	}
	s.mu.Lock()
	if seq <= s.ack {
		s.mu.Unlock()
		return
	}
	prevAck := s.ack
	s.ack = seq
	removed := s.removeAckedLocked(seq)
	acked := int(seq - prevAck)
	s.adjustWindowOnAckLocked(acked)
	now := s.engine.Now()
	s.updateThroughputLocked(seq, now)
	for _, frm := range removed {
		if frm == nil {
			continue
		}
		if frm.timer != nil {
			frm.timer.Stop()
			frm.timer = nil
		}
		if !frm.sentAt.IsZero() {
			s.observeRTTLocked(now.Sub(frm.sentAt))
		}
	}
	s.touchLocked(now)
	s.adjustWindowForCapacityLocked()
	s.sendCond.Signal()
	s.mu.Unlock()
}

func (s *streamSession) deliverRemote(data []byte) {
	if len(data) == 0 || s.stream == nil || s.stream.recvW == nil {
		return
	}
	if _, err := s.stream.recvW.Write(data); err != nil {
		s.Close()
		return
	}
	s.mu.Lock()
	if s.engine != nil {
		s.touchLocked(s.engine.Now())
	}
	s.mu.Unlock()
}

func (s *streamSession) sendAck(seq uint32) {
	ack := &protocol.StreamAck{StreamID: s.streamID, Ack: seq}
	raw, _ := protocol.EncodePayload(ack)
	s.mu.Lock()
	if s.engine != nil {
		s.touchLocked(s.engine.Now())
	}
	s.mu.Unlock()
	s.engine.sendProto(s.target, protocol.STREAM_ACK, raw)
}

func (s *streamSession) run() {
	if s.outbound {
		s.enqueueOpen()
	}
	for {
		s.mu.Lock()
		for len(s.pending) == 0 && len(s.inflight) == 0 && !s.closing && !s.closed {
			s.sendCond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			break
		}
		if len(s.pending) > 0 && len(s.inflight) < s.windowSize() {
			data := s.pending[0]
			s.pending = s.pending[1:]
			seq := s.nextSeq()
			frm := &frame{seq: seq, payload: data, attempts: 0}
			s.inflight[seq] = frm
			go s.sendData(frm)
		} else if s.closing && len(s.inflight) == 0 {
			s.mu.Unlock()
			s.sendClose()
			break
		} else {
			s.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.mu.Unlock()
	}
}

// Accept creates an inbound stream that was opened by the remote peer.
// The caller should use the returned stream for bi-directional I/O.
func (e *Engine) Accept(id uint32, opt Options) *Stream {
	if e == nil {
		return nil
	}
	sess := &streamSession{
		engine:   e,
		target:   opt.Target,
		options:  opt,
		streamID: id,
		inflight: make(map[uint32]*frame),
		outbound: false,
	}
	sess.minWindow = e.cfg.MinWindow
	sess.maxWindow = e.cfg.WindowFrames
	sess.window = e.cfg.InitialWindow
	sess.rto = e.cfg.BaseRTO
	sess.lastActivity = e.Now()
	sess.sendCond = sync.NewCond(&sess.mu)
	e.mux.Lock()
	if e.cfg.MaxStream > 0 && len(e.streams) >= e.cfg.MaxStream {
		e.mux.Unlock()
		e.rejectStream(id, opt.Target, "stream limit reached")
		return nil
	}
	e.streams[id] = sess
	e.mux.Unlock()
	sendR, sendW := io.Pipe()
	recvR, recvW := io.Pipe()
	stream := &Stream{engine: e, session: sess, sendR: sendR, sendW: sendW, recvR: recvR, recvW: recvW}
	sess.bindStream(stream)
	go stream.runSender()
	return stream
}

func (s *streamSession) nextSeq() uint32 {
	s.seq++
	return s.seq
}

func (s *streamSession) enqueueOpen() {
	open := &protocol.StreamOpen{StreamID: s.streamID, Options: encodeOptions(s.options.Meta)}
	openBytes, _ := protocol.EncodePayload(open)
	s.engine.sendProto(s.target, protocol.STREAM_OPEN, openBytes)
}

func (s *streamSession) sendData(frm *frame) {
	if s == nil || s.engine == nil || frm == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	frm.attempts++
	now := s.engine.Now()
	frm.sentAt = now
	s.touchLocked(now)
	ack := s.ack
	window := s.windowSize()
	attempt := frm.attempts
	rto := s.currentRTOLocked()
	if frm.timer != nil {
		frm.timer.Stop()
	}
	frm.timer = time.AfterFunc(rto, func() {
		s.onTimeout(frm.seq, attempt)
	})
	s.mu.Unlock()
	data := &protocol.StreamData{StreamID: s.streamID, Seq: frm.seq, Ack: ack, Window: uint16(window)}
	if len(frm.payload) > 0 {
		data.Payload = append([]byte(nil), frm.payload...)
	}
	raw, _ := protocol.EncodePayload(data)
	s.engine.sendProto(s.target, protocol.STREAM_DATA, raw)
}

func (s *streamSession) onTimeout(seq uint32, attempt int) {
	if s == nil || s.engine == nil {
		return
	}
	s.mu.Lock()
	frm := s.inflight[seq]
	if frm == nil || s.closed || frm.attempts != attempt {
		s.mu.Unlock()
		return
	}
	if frm.attempts >= s.engine.cfg.RetransLimit {
		// A reliable stream cannot make progress once any in-flight frame exceeds
		// the retransmission budget. Abort immediately so callers (e.g. dataplane)
		// observe the failure promptly instead of draining the remaining pending
		// buffer for minutes under DTN churn/offline targets.
		s.mu.Unlock()
		s.Abort(fmt.Sprintf("stream timeout seq=%d", seq))
		return
	}
	s.reduceWindowOnLossLocked()
	s.mu.Unlock()
	go s.sendData(frm)
}

func (s *streamSession) sendClose() {
	if s == nil || s.engine == nil {
		return
	}
	closeMsg := &protocol.StreamClose{StreamID: s.streamID, Code: s.closeCode, Reason: s.closeReason}
	raw, _ := protocol.EncodePayload(closeMsg)
	s.engine.sendProto(s.target, protocol.STREAM_CLOSE, raw)

	// STREAM_CLOSE is not explicitly ACKed at the stream layer. Under DTN churn (repair, sleep),
	// a single delayed/misdirected close can strand the session and wedge callers (e.g. dataplane
	// uploads waiting for remote finalization). Re-send a few duplicates while the session is
	// still active to increase the chance at least one close reaches the peer after routes heal.
	//
	// We guard by pointer equality so a recycled stream_id can't receive a stale close.
	id := s.streamID
	for _, delay := range []time.Duration{2 * time.Second, 8 * time.Second, 20 * time.Second} {
		time.AfterFunc(delay, func() {
			if s.engine == nil {
				return
			}
			if s.engine.getSession(id) != s {
				return
			}
			s.engine.sendProto(s.target, protocol.STREAM_CLOSE, raw)
		})
	}
}

func (e *Engine) sendProto(target string, msgType uint16, payload []byte) {
	env := fmt.Sprintf("proto:%04x:%s", msgType, hex.EncodeToString(payload))
	if e.sendFn != nil {
		_ = e.sendFn(target, []byte(env))
	}
}

func (e *Engine) rejectStream(id uint32, target, reason string) {
	if e == nil {
		return
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return
	}
	if reason == "" {
		reason = "stream rejected"
	}
	closeMsg := &protocol.StreamClose{StreamID: id, Code: 1, Reason: reason}
	raw, _ := protocol.EncodePayload(closeMsg)
	e.sendProto(target, protocol.STREAM_CLOSE, raw)
	if e.logFn != nil {
		e.logFn("stream %d rejected: %s", id, reason)
	}
}

func (e *Engine) Close(streamID uint32, reason string) bool {
	if e == nil {
		return false
	}
	if reason == "" {
		reason = "stream closed by admin"
	}
	if sess := e.getSession(streamID); sess != nil {
		sess.Abort(reason)
		return true
	}
	return false
}

func encodeOptions(meta map[string]string) string {
	return streamopts.Encode(meta)
}

func (e *Engine) HandleAck(ack *protocol.StreamAck) {
	if ack == nil {
		return
	}
	if sess := e.getSession(ack.StreamID); sess != nil {
		sess.Ack(ack.Ack)
	}
}

func (e *Engine) HandleData(data *protocol.StreamData) {
	if data == nil {
		return
	}
	if sess := e.getSession(data.StreamID); sess != nil {
		// Buffer out-of-order frames; only ACK the "contiguous applied" rxAck.
		// This prevents data loss/corruption when DTN retransmissions or routing churn reorders frames.
		var (
			ackVal   uint32
			deliver  [][]byte
			overflow bool
		)
		sess.mu.Lock()
		if sess.rxBuf == nil {
			sess.rxBuf = make(map[uint32][]byte)
		}
		seq := data.Seq
		switch {
		case seq == 0:
			// ignore
		case seq <= sess.rxAck:
			// duplicate / old frame; ACK current rxAck
		default:
			if len(data.Payload) > 0 {
				if _, ok := sess.rxBuf[seq]; !ok {
					sess.rxBuf[seq] = append([]byte(nil), data.Payload...)
				}
			}
			for {
				next := sess.rxAck + 1
				payload, ok := sess.rxBuf[next]
				if !ok {
					break
				}
				delete(sess.rxBuf, next)
				sess.rxAck = next
				if len(payload) > 0 {
					deliver = append(deliver, payload)
				}
			}
			if len(sess.rxBuf) > 256 {
				overflow = true
			}
		}
		ackVal = sess.rxAck
		sess.mu.Unlock()

		if overflow {
			sess.Abort("stream rx buffer overflow")
			return
		}
		for _, chunk := range deliver {
			if len(chunk) == 0 {
				continue
			}
			sess.deliverRemote(chunk)
		}
		sess.sendAck(ackVal)
	}
}

func (e *Engine) HandleClose(msg *protocol.StreamClose) {
	if msg == nil {
		return
	}
	if sess := e.getSession(msg.StreamID); sess != nil {
		if msg.Code == 0 {
			// Normal remote closure: propagate EOF to readers.
			sess.mu.Lock()
			sess.closeCode = 0
			sess.closeReason = msg.Reason
			sess.mu.Unlock()
			sess.Close()
			e.removeSession(msg.StreamID)
			return
		}
		reason := strings.TrimSpace(msg.Reason)
		if reason == "" {
			reason = "remote closed"
		}
		sess.SetError(fmt.Errorf("remote closed: %s", reason))
		sess.Close()
		e.removeSession(msg.StreamID)
	}
}

func (e *Engine) getSession(id uint32) *streamSession {
	e.mux.Lock()
	s := e.streams[id]
	e.mux.Unlock()
	return s
}

func (e *Engine) removeSession(id uint32) {
	e.mux.Lock()
	delete(e.streams, id)
	e.mux.Unlock()
}

func (e *Engine) SessionMeta(id uint32) map[string]string {
	e.mux.Lock()
	sess := e.streams[id]
	e.mux.Unlock()
	if sess == nil || sess.options.Meta == nil {
		return nil
	}
	meta := make(map[string]string, len(sess.options.Meta))
	for k, v := range sess.options.Meta {
		meta[k] = v
	}
	return meta
}

type frame struct {
	seq      uint32
	payload  []byte
	attempts int
	sentAt   time.Time
	timer    *time.Timer
}

type SessionDiag struct {
	ID           uint32
	Target       string
	Kind         string
	Outbound     bool
	Pending      int
	Inflight     int
	Window       int
	Ack          uint32
	Seq          uint32
	RTO          time.Duration
	LastActivity time.Time
	SessionID    string
	Metadata     map[string]string
}

func (e *Engine) Diagnostics() []SessionDiag {
	if e == nil {
		return nil
	}
	e.mux.Lock()
	sessions := make([]*streamSession, 0, len(e.streams))
	for _, sess := range e.streams {
		sessions = append(sessions, sess)
	}
	e.mux.Unlock()
	diags := make([]SessionDiag, 0, len(sessions))
	for _, sess := range sessions {
		if sess == nil {
			continue
		}
		diags = append(diags, sess.snapshot())
	}
	sort.Slice(diags, func(i, j int) bool {
		return diags[i].ID < diags[j].ID
	})
	return diags
}

func (s *streamSession) snapshot() SessionDiag {
	if s == nil {
		return SessionDiag{}
	}
	s.mu.Lock()
	sessionID := ""
	if s.options.Meta != nil {
		sessionID = s.options.Meta["session"]
	}
	meta := make(map[string]string)
	if s.options.Meta != nil {
		for k, v := range s.options.Meta {
			meta[k] = v
		}
	}
	diag := SessionDiag{
		ID:           s.streamID,
		Target:       s.target,
		Kind:         s.kind(),
		Outbound:     s.outbound,
		Pending:      len(s.pending),
		Inflight:     len(s.inflight),
		Window:       s.windowSize(),
		Ack:          s.ack,
		Seq:          s.seq,
		RTO:          s.currentRTOLocked(),
		LastActivity: s.lastActivity,
		SessionID:    sessionID,
		Metadata:     meta,
	}
	s.mu.Unlock()
	return diag
}

func (s *streamSession) kind() string {
	if s == nil || s.options.Meta == nil {
		return "unknown"
	}
	if kind, ok := s.options.Meta["kind"]; ok && kind != "" {
		return kind
	}
	return "unknown"
}
