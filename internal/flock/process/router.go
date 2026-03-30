package process

import (
	"bytes"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/share/streamopts"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func (agent *Agent) setupRouter() {
	if agent.router == nil {
		agent.router = bus.NewRouter()
	}
	if agent.mgr == nil {
		agent.routerReady = false
		return
	}
	agent.router = bus.NewRouter()
	// V2: 会话创建与数据通道均走 STREAM_*，移除 SHELLWINSZ 路由
	// V2: SSH 走 DTN-Stream，移除 legacy SSHREQ/SSHCOMMAND 下行入口
	// V2: SSHTunnel 通过 DTN-Stream，不再接收 legacy SSHTUNNELREQ
	// V2: 文件传输走 DTN-Stream，不再接收 legacy file 消息
	// V2: SOCKS 通过 DTN-Stream，不再接收 legacy SOCKS 消息
	// V2: forward/backward 通过 DTN-Stream，不再接收 legacy 消息
	agent.registerMailboxHandlers(
		"listen",
		agent.mgr.ListenManager.Enqueue,
		protocol.CHILDUUIDRES,
		protocol.LISTENREQ,
		protocol.LISTENSTOP,
	)
	agent.registerMailboxHandlers(
		"offline",
		agent.mgr.OfflineManager.Enqueue,
		protocol.UPSTREAMOFFLINE,
		protocol.UPSTREAMREONLINE,
	)

	agent.router.Register(protocol.MYMEMO, agent.memoUpdateHandler())
	agent.router.Register(uint16(protocol.GOSSIP_REQUEST), agent.gossipRequestHandler())
	agent.router.Register(uint16(protocol.GOSSIP_RESPONSE), agent.gossipResponseHandler())
	agent.router.Register(uint16(protocol.GOSSIP_UPDATE), agent.gossipUpdateHandler())
	agent.router.Register(uint16(protocol.SUPPLINKREQ), agent.supplinkRequestHandler())
	agent.router.Register(uint16(protocol.SUPPLINKTEARDOWN), agent.supplinkTeardownHandler())
	agent.router.Register(uint16(protocol.SUPPFAILOVER), agent.supplinkFailoverHandler())
	agent.router.Register(uint16(protocol.RESCUE_REQUEST), agent.rescueRequestHandler())
	agent.router.Register(protocol.SHUTDOWN, agent.shutdownHandler())
	agent.router.Register(protocol.HEARTBEAT, agent.ignoreHandler())
	// DTN data plane (experimental)
	agent.router.Register(uint16(protocol.DTN_DATA), agent.dtnDataHandler())
	// STREAM_* (V2) placeholders
	agent.router.Register(uint16(protocol.STREAM_OPEN), agent.streamOpenHandler())
	agent.router.Register(uint16(protocol.STREAM_DATA), agent.streamDataHandler())
	agent.router.Register(uint16(protocol.STREAM_ACK), agent.streamAckHandler())
	agent.router.Register(uint16(protocol.STREAM_CLOSE), agent.streamCloseHandler())
	agent.router.Register(uint16(protocol.SLEEP_UPDATE), agent.sleepUpdateHandler())
	agent.router.Register(uint16(protocol.CONNECTSTART), agent.connectStartHandler())
	agent.routerReady = true
}
func (agent *Agent) registerMailboxHandlers(name string, enqueue func(context.Context, interface{}) (bool, error), messageTypes ...uint16) {
	if agent.router == nil || enqueue == nil {
		return
	}
	handler := agent.mailboxHandler(name, enqueue)
	for _, msgType := range messageTypes {
		agent.router.Register(msgType, handler)
	}
}

func (agent *Agent) mailboxHandler(name string, enqueue func(context.Context, interface{}) (bool, error)) bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		if header == nil {
			return bus.ErrNoHandler
		}
		dropped, err := enqueue(ctx, payload)
		if err != nil && !errors.Is(err, utils.ErrMailboxClosed) {
			return err
		}
		if dropped {
			logger.Warnf("%s queue dropped oldest message (type=%d)", name, header.MessageType)
		}
		return nil
	}
}

func (agent *Agent) ensureRouter() {
	if agent.router == nil {
		agent.router = bus.NewRouter()
	}
	if !agent.routerReady && agent.mgr != nil {
		agent.setupRouter()
	}
}

func (agent *Agent) memoUpdateHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		memo, ok := payload.(*protocol.MyMemo)
		if !ok {
			return fmt.Errorf("expected *protocol.MyMemo, got %T", payload)
		}
		agent.Memo = memo.Memo
		// Memo convergence should not depend solely on the async trigger channel:
		// under heavy gossip churn a coalesced trigger can delay/skip the newest
		// memo snapshot. Emit once immediately to guarantee propagation.
		agent.emitGossipUpdate()
		// Keep the periodic/async path as a fallback when the immediate emission
		// cannot proceed (e.g. transient session/connection unavailable).
		agent.triggerGossipUpdate()
		return nil
	}
}

func (agent *Agent) dtnDataHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		data, ok := payload.(*protocol.DTNData)
		if !ok {
			return fmt.Errorf("expected *protocol.DTNData, got %T", payload)
		}
		// Treat DTN delivery as activity so the sleep manager doesn't drop the upstream
		// connection while we're applying the payload and sending the ACK.
		agent.noteActivity()
		clone := *data
		if data.Payload != nil {
			clone.Payload = append([]byte(nil), data.Payload...)
		}
		agent.recordDTNMessage(&clone)

		// Trace helper: "log:<message>" DTN payloads need to be observable at Kelpie.
		// Instead of relying on a separate RuntimeLog message (which can be lost during
		// sleep/failover churn), echo the log payload via DTN_ACK.Error on success so it
		// inherits ACK retry/carry-forward behavior.
		var dtnLogEcho string
		msgText := strings.TrimSpace(string(clone.Payload))
		lower := strings.ToLower(msgText)
		if strings.HasPrefix(lower, "log:") {
			dtnLogEcho = strings.TrimSpace(msgText[len("log:"):])
		}

		origin := originFromContext(ctx, header)
		originConn := originConnFromContext(ctx)
		sess := agent.currentSession()
		if sess == nil {
			return nil
		}
		// Prefer replying via the origin link (e.g. supplemental) so responses can flow
		// even when the primary upstream session is sleeping/closed.
		var adminConn net.Conn
		if originConn != nil {
			adminConn = originConn
		} else if conn, ok, _ := agent.nextHopConn(origin, true); ok && conn != nil {
			adminConn = conn
		} else {
			adminConn = sess.Conn()
		}

		// Optional: apply minimal business actions
		applyErr := agent.applyDTNPayload(&clone, adminConn)
		if applyErr != nil {
			// keep stored for diagnostics; ACK will carry error
		}

		// Immediately ACK upstream (OK on stored and applied, ERR if inbox full or apply error)
		ackHeader := &protocol.Header{
			Sender:      agent.UUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: uint16(protocol.DTN_ACK),
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		ack := &protocol.DTNAck{BundleIDLen: uint16(len(data.BundleID)), BundleID: data.BundleID, OK: 1}
		if applyErr != nil {
			ack.OK = 0
			ack.Error = applyErr.Error()
			ack.ErrorLen = uint16(len(ack.Error))
		} else if dtnLogEcho != "" {
			ack.Error = dtnLogEcho
			ack.ErrorLen = uint16(len(ack.Error))
		}
		if adminConn != nil && sess != nil {
			if err := agent.sendUpCarryItemOnConn(adminConn, sess.Secret(), agent.UUID, ackHeader, ack, false); err == nil {
				return nil
			}
		}
		// If the upstream link is flapping (sleep/kill/reconnect), buffer the ACK and retry.
		agent.maybeEnqueueUpCarryLocal(ackHeader, ack)
		return nil
	}
}

func (agent *Agent) sleepUpdateHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		update, ok := payload.(*protocol.SleepUpdate)
		if !ok {
			return fmt.Errorf("expected *protocol.SleepUpdate, got %T", payload)
		}
		err := agent.applySleepUpdate(update)
		// Sleep updates often precede follow-up control traffic (supplemental planning, DTN flush, rescue).
		// If lastActivity is stale, the sleep manager can immediately drop the upstream connection and
		// strand those messages. Hold the upstream open for (at least) one work window.
		cfg := agent.loadSleepConfig()
		grace := time.Duration(cfg.workSeconds) * time.Second
		if grace <= 0 {
			grace = 2 * time.Second
		}
		agent.holdAwakeFor(grace)
		agent.sendSleepUpdateAck(err)
		return err
	}
}

func (agent *Agent) sendSleepUpdateAck(applyErr error) {
	if agent == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil || sess.Conn() == nil {
		return
	}
	up := protocol.NewUpMsg(sess.Conn(), sess.Secret(), agent.UUID)
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.SLEEP_UPDATE_ACK),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	ack := &protocol.SleepUpdateAck{}
	if applyErr == nil {
		ack.OK = 1
	} else {
		ack.OK = 0
		ack.Error = applyErr.Error()
	}
	protocol.ConstructMessage(up, header, ack, false)
	up.SendMessage()
}

// applyDTNPayload interprets a minimal DTN payload contract:
// - "memo:<text>" updates agent memo and triggers gossip update.
// - "log:<message>" sends a RuntimeLog upstream (INFO).
// Returns error if unsupported or delivery failed (for log path).
func (agent *Agent) applyDTNPayload(data *protocol.DTNData, adminConn net.Conn) error {
	if agent == nil || data == nil {
		return fmt.Errorf("invalid payload")
	}
	msg := strings.TrimSpace(string(data.Payload))
	lower := strings.ToLower(msg)
	switch {
	case strings.HasPrefix(lower, "memo:"):
		content := strings.TrimSpace(msg[len("memo:"):])
		agent.Memo = content
		logger.Infof("[diag/dtn_memo_apply] self=%s bundle=%s memo=%q", agent.UUID, data.BundleID, content)
		// DTN memo updates must become visible to Kelpie immediately.
		agent.emitGossipUpdate()
		agent.pushMemoSnapshotToAdmin(adminConn)
		// Preserve eventual convergence when immediate uplink fails.
		agent.triggerGossipUpdate()
		return nil
	case strings.HasPrefix(lower, "log:"):
		// "log:<message>" payloads are echoed via DTN_ACK.Error (see dtnDataHandler),
		// so we don't need to emit a separate RuntimeLog here.
		return nil
	case strings.HasPrefix(lower, "stream:"):
		// stream:<subcommand>[:kv]
		// Prototype: stream:ping[:size=<n>][:seq=<n>]
		// Respond upstream with a RuntimeLog so admin can see the pong.
		content := strings.TrimSpace(msg[len("stream:"):])
		sub := content
		if i := strings.IndexByte(content, ':'); i >= 0 {
			sub = strings.ToLower(strings.TrimSpace(content[:i]))
		} else {
			sub = strings.ToLower(strings.TrimSpace(content))
		}
		if sub != "ping" {
			return fmt.Errorf("unsupported stream subcommand: %s", sub)
		}
		// parse params (very lenient)
		size := 0
		seq := 0
		fields := strings.Split(content, ":")
		for _, f := range fields[1:] {
			kv := strings.SplitN(strings.TrimSpace(f), "=", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(kv[0]))
			val := strings.TrimSpace(kv[1])
			switch key {
			case "size":
				fmt.Sscanf(val, "%d", &size)
			case "seq":
				fmt.Sscanf(val, "%d", &seq)
			}
		}
		// reply upstream as RuntimeLog (INFO) for prototype visibility
		sess := agent.currentSession()
		header := &protocol.Header{
			Sender:      agent.UUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: uint16(protocol.RUNTIMELOG),
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		msgText := fmt.Sprintf("STREAM_PONG seq=%d size=%d", seq, size)
		rlog := &protocol.RuntimeLog{
			UUIDLen:     uint16(len(agent.UUID)),
			UUID:        agent.UUID,
			SeverityLen: uint16(len("INFO")),
			Severity:    "INFO",
			CodeLen:     uint16(len("DTN")),
			Code:        "DTN",
			MessageLen:  uint64(len(msgText)),
			Message:     msgText,
			Retryable:   0,
			CauseLen:    0,
			Cause:       "",
		}
		if sess != nil && adminConn != nil {
			if err := agent.sendUpCarryItemOnConn(adminConn, sess.Secret(), agent.UUID, header, rlog, false); err == nil {
				return nil
			}
		}
		agent.maybeEnqueueUpCarryLocal(header, rlog)
		return nil
	default:
		// proto:<type-hex>:<payload-hex>
		if strings.HasPrefix(lower, "proto:") {
			rest := msg[len("proto:"):]
			parts := strings.SplitN(rest, ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid proto envelope")
			}
			var mt uint16
			if _, err := fmt.Sscanf(parts[0], "%x", &mt); err != nil {
				return fmt.Errorf("invalid message type")
			}
			raw, err := hex.DecodeString(strings.TrimSpace(parts[1]))
			if err != nil {
				return fmt.Errorf("invalid payload hex")
			}
			payload, err := protocol.DecodePayload(uint16(mt), raw)
			if err != nil {
				return fmt.Errorf("decode payload: %w", err)
			}
			// Preserve the origin connection for inner "proto:*" payloads.
			//
			// Stream/dataplane frames are delivered over DTN as "proto:<type>:<payload>".
			// When they traverse a supplemental edge, replies must go back over that same
			// link; otherwise a sleeping/disconnected upstream session can wedge the flow.
			//
			// dispatchLocalMessage() propagates originConn into handler context, which the
			// stream layer uses to pin replyConn for reliable uplink.
			header := &protocol.Header{Sender: protocol.ADMIN_UUID, Accepter: agent.UUID, MessageType: uint16(mt), RouteLen: 0, Route: ""}
			agent.dispatchLocalMessage(header, payload, protocol.ADMIN_UUID, adminConn)
			return nil
		}
		return fmt.Errorf("unsupported payload")
	}
}

func (agent *Agent) pushMemoSnapshotToAdmin(adminConn net.Conn) {
	if agent == nil || adminConn == nil {
		return
	}
	sess := agent.currentSession()
	if sess == nil {
		return
	}
	info := agent.buildNodeInfo()
	nodeData, err := json.Marshal(info)
	if err != nil {
		logger.Warnf("marshal memo snapshot failed: %v", err)
		return
	}
	update := &protocol.GossipUpdate{
		TTL:           1,
		NodeDataLen:   uint64(len(nodeData)),
		NodeData:      nodeData,
		SenderUUIDLen: uint16(len(info.UUID)),
		SenderUUID:    info.UUID,
		Timestamp:     time.Now().Unix(),
	}
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.GOSSIP_UPDATE),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	if err := agent.sendUpCarryItemOnConn(adminConn, sess.Secret(), agent.UUID, header, update, false); err != nil {
		agent.maybeEnqueueUpCarryLocal(header, update)
	}
}

func (agent *Agent) gossipRequestHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.GossipRequest)
		if !ok {
			return fmt.Errorf("expected *protocol.GossipRequest, got %T", payload)
		}
		agent.handleGossipRequest(req)
		return nil
	}
}

func (agent *Agent) gossipResponseHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		resp, ok := payload.(*protocol.GossipResponse)
		if !ok {
			return fmt.Errorf("expected *protocol.GossipResponse, got %T", payload)
		}
		agent.handleGossipResponse(resp)
		return nil
	}
}

func (agent *Agent) gossipUpdateHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		origin := originFromContext(ctx, header)
		agent.handleGossipPayload(payload, origin)
		return nil
	}
}

func (agent *Agent) supplinkRequestHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.SuppLinkRequest)
		if !ok {
			return fmt.Errorf("expected *protocol.SuppLinkRequest, got %T", payload)
		}
		HandleSuppLinkRequest(agent.mgr, req)
		return nil
	}
}

func (agent *Agent) supplinkTeardownHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		td, ok := payload.(*protocol.SuppLinkTeardown)
		if !ok {
			return fmt.Errorf("expected *protocol.SuppLinkTeardown, got %T", payload)
		}
		HandleSuppLinkTeardown(agent.mgr, td)
		return nil
	}
}

func (agent *Agent) supplinkFailoverHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		cmd, ok := payload.(*protocol.SuppFailoverCommand)
		if !ok {
			return fmt.Errorf("expected *protocol.SuppFailoverCommand, got %T", payload)
		}
		agent.handleSuppFailoverCommand(cmd)
		return nil
	}
}

func (agent *Agent) shutdownHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		agent.Stop()
		return nil
	}
}

func (agent *Agent) ignoreHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		return nil
	}
}
func (agent *Agent) streamOpenHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		open, ok := payload.(*protocol.StreamOpen)
		if !ok || open == nil {
			return fmt.Errorf("expected *protocol.StreamOpen, got %T", payload)
		}
		opts := parseStreamOptions(open.Options)
		sessionID := strings.TrimSpace(opts["session"])
		kind := strings.ToLower(strings.TrimSpace(opts["kind"]))
		originConn := originConnFromContext(ctx)
		var sessConn net.Conn
		if sess := agent.currentSession(); sess != nil {
			sessConn = sess.Conn()
		}
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
		if agent.streams == nil {
			agent.streams = make(map[uint32]*streamState)
		}
		// STREAM_DATA can arrive before STREAM_OPEN under DTN reordering.
		// Preserve any existing rx buffer / ack state so we don't drop already-received frames.
		st := agent.streams[open.StreamID]
		if st == nil {
			st = &streamState{}
			agent.streams[open.StreamID] = st
		}
		st.options = open.Options
		st.sessionID = sessionID
		st.kind = kind
		st.meta = opts
		st.replyConn = originConn
		st.replyIsSupplemental = originConn != nil && sessConn != nil && !sameConn(originConn, sessConn)
		agent.streamMu.Unlock()
		if kind == "" {
			agent.rejectStreamOpen(open.StreamID, "missing stream kind")
			return nil
		}
		switch kind {
		case "shell":
			if sessionID == "" {
				agent.rejectStreamOpen(open.StreamID, "shell session missing")
				break
			}
			if agent.mgr == nil || agent.mgr.ShellManager == nil {
				agent.rejectStreamOpen(open.StreamID, "shell manager unavailable")
				break
			}
			agent.mgr.ShellManager.SetStreamForSession(sessionID, open.StreamID)
			mode := uint16(protocol.ShellModePipe)
			if m, ok := opts["mode"]; ok {
				if mv, err := strconv.Atoi(m); err == nil && (mv == int(protocol.ShellModePipe) || mv == int(protocol.ShellModePTY)) {
					mode = uint16(mv)
				}
			}
			_, _ = agent.mgr.ShellManager.Enqueue(ctx, &ShellReqWithStream{SessionID: sessionID, Mode: mode, Resume: false})
		case "ssh":
			if agent.mgr == nil || agent.mgr.SSHManager == nil {
				agent.rejectStreamOpen(open.StreamID, "ssh manager unavailable")
				break
			}
			req := &SSHReqMsg{}
			switch strings.TrimSpace(opts["method"]) {
			case "2":
				req.Method = 2
			default:
				req.Method = 1
			}
			addr := strings.TrimSpace(opts["addr"])
			if addr == "" {
				agent.rejectStreamOpen(open.StreamID, "ssh addr missing")
				break
			}
			req.Addr = addr
			if u := strings.TrimSpace(opts["username"]); u != "" {
				req.Username = u
			}
			if p := opts["password"]; p != "" && req.Method == 1 {
				req.Password = p
			}
			_, _ = agent.mgr.SSHManager.Enqueue(ctx, &SSHReqWithStream{Req: req, StreamID: open.StreamID})
		case "ssh-tunnel":
			method := strings.TrimSpace(opts["method"])
			if method == "" {
				method = "1"
			}
			if method == "1" {
				go agent.startSSHTunnel(open.StreamID, opts, nil)
			}
		case "file-put", "file-get":
			agent.fileOnOpen(open.StreamID, opts)
		case "proxy":
			agent.proxyOnOpen(open.StreamID, opts)
		case "socks":
			agent.socksOnOpen(open.StreamID, opts)
		default:
			agent.rejectStreamOpen(open.StreamID, fmt.Sprintf("unsupported stream kind: %s", kind))
		}
		agent.noteActivity()
		return nil
	}
}

func (agent *Agent) rejectStreamOpen(streamID uint32, reason string) {
	if reason == "" {
		reason = "stream rejected"
	}
	agent.sendStreamClose(streamID, 1, reason)
}

func (agent *Agent) streamDataHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		data, ok := payload.(*protocol.StreamData)
		if !ok || data == nil {
			return fmt.Errorf("expected *protocol.StreamData, got %T", payload)
		}
		agent.streamMu.Lock()
		var (
			sessionID string
			kind      string
			ackVal    uint32
			deliver   [][]byte
			overflow  bool
		)
		if agent.streams != nil {
			st := agent.streams[data.StreamID]
			if st == nil {
				st = &streamState{}
				agent.streams[data.StreamID] = st
			}
			sessionID = st.sessionID
			kind = st.kind
			if st.rxBuf == nil {
				st.rxBuf = make(map[uint32][]byte)
			}
			seq := data.Seq
			switch {
			case seq == 0:
				// ignore
			case seq <= st.rxAck:
				// duplicate / old frame; ack current rxAck
			default:
				// Buffer out-of-order frames; only advance rxAck when gaps are filled.
				if len(data.Payload) > 0 {
					if _, ok := st.rxBuf[seq]; !ok {
						st.rxBuf[seq] = append([]byte(nil), data.Payload...)
					}
				}
				if kind != "" {
					// Only ACK frames that we can actually apply. If STREAM_DATA arrives before
					// STREAM_OPEN (kind unknown), advancing rxAck would permanently drop data
					// because the sender stops retransmitting once ACKed.
					for {
						next := st.rxAck + 1
						payload, ok := st.rxBuf[next]
						if !ok {
							break
						}
						delete(st.rxBuf, next)
						st.rxAck = next
						if len(payload) > 0 {
							deliver = append(deliver, payload)
						}
					}
				}
				if len(st.rxBuf) > 256 {
					overflow = true
				}
			}
			ackVal = st.rxAck
		}
		agent.streamMu.Unlock()
		agent.noteActivity()
		if overflow {
			agent.sendStreamClose(data.StreamID, 1, "stream rx buffer overflow")
			return nil
		}
		if len(deliver) > 0 && agent.mgr != nil {
			for _, chunk := range deliver {
				if len(chunk) == 0 {
					continue
				}
				switch kind {
				case "ssh":
					if agent.mgr.SSHManager != nil {
						cmd := &protocol.SSHCommand{CommandLen: uint64(len(chunk)), Command: string(chunk)}
						if _, err := agent.mgr.SSHManager.Enqueue(ctx, cmd); err != nil {
							logger.Warnf("stream ssh enqueue failed: %v", err)
						}
					}
				case "shell":
					if sessionID != "" && agent.mgr.ShellManager != nil {
						if chunk[0] == 0x00 {
							ctrl := bytes.TrimSpace(chunk[1:])
							if bytes.HasPrefix(ctrl, []byte("WIN ")) {
								fields := strings.Fields(string(ctrl[4:]))
								if len(fields) >= 2 {
									if r, err1 := strconv.Atoi(fields[0]); err1 == nil {
										if c, err2 := strconv.Atoi(fields[1]); err2 == nil {
											HandleShellResize(agent.mgr, sessionID, uint16(r), uint16(c))
										}
									}
								}
							}
						} else {
							cmd := &ShellCommandMsg{SessionID: sessionID, Command: string(chunk)}
							if _, err := agent.mgr.ShellManager.Enqueue(ctx, cmd); err != nil {
								logger.Warnf("stream shell enqueue failed: %v", err)
							}
						}
					}
				case "ssh-tunnel":
					// 证书模式：收到证书后启动（只触发一次）。
					agent.streamMu.Lock()
					st := agent.streams[data.StreamID]
					if st != nil && !st.started {
						st.started = true
						meta := st.meta
						agent.streamMu.Unlock()
						go agent.startSSHTunnel(data.StreamID, meta, chunk)
					} else {
						agent.streamMu.Unlock()
					}
				case "file-put":
					agent.fileOnData(data.StreamID, chunk)
				case "proxy":
					agent.proxyOnData(data.StreamID, chunk)
				case "socks":
					agent.socksOnData(data.StreamID, chunk)
				default:
					if kind != "" {
						logger.Warnf("stream %d received data for unsupported kind=%s", data.StreamID, kind)
					}
				}
			}
		}
		// 简单回 ACK（累积确认，按“连续已应用”的 rxAck 回传）
		if sess := agent.currentSession(); sess != nil {
			conn := originConnFromContext(ctx)
			if conn == nil {
				conn = sess.Conn()
			}
			if conn != nil {
				up := protocol.NewUpMsg(conn, sess.Secret(), agent.UUID)
				h := &protocol.Header{Sender: agent.UUID, Accepter: protocol.ADMIN_UUID, MessageType: uint16(protocol.STREAM_ACK), RouteLen: uint32(len([]byte(protocol.TEMP_ROUTE))), Route: protocol.TEMP_ROUTE}
				ack := &protocol.StreamAck{StreamID: data.StreamID, Ack: ackVal, Credit: 0}
				protocol.ConstructMessage(up, h, ack, false)
				up.SendMessage()
			}
		}
		return nil
	}
}

func (agent *Agent) streamAckHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		ack, ok := payload.(*protocol.StreamAck)
		if !ok || ack == nil {
			return fmt.Errorf("expected *protocol.StreamAck, got %T", payload)
		}
		agent.handleStreamAck(ack)
		return nil
	}
}

func (agent *Agent) streamCloseHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		closeMsg, ok := payload.(*protocol.StreamClose)
		if !ok || closeMsg == nil {
			return fmt.Errorf("expected *protocol.StreamClose, got %T", payload)
		}
		var kind string
		agent.streamMu.Lock()
		if agent.streams != nil {
			if st := agent.streams[closeMsg.StreamID]; st != nil {
				kind = st.kind
			}
		}
		agent.streamMu.Unlock()
		// Do not delete agent.streams[streamID] before running kind-specific close handlers.
		//
		// file-put/proxy need the per-stream replyConn (pinned to the origin link, often a
		// supplemental edge) to send their STREAM_CLOSE back upstream reliably. Deleting
		// the stream state first loses replyConn and can wedge dataplane uploads waiting for
		// remote finalization under churn (sleep/repair/failover).
		agent.cleanupStreamPending(closeMsg.StreamID)
		switch kind {
		case "file-put", "file-get":
			agent.fileOnClose(closeMsg.StreamID)
		case "proxy":
			agent.proxyOnClose(closeMsg.StreamID)
		case "socks":
			agent.socksOnClose(closeMsg.StreamID)
		}
		agent.streamMu.Lock()
		if agent.streams != nil {
			delete(agent.streams, closeMsg.StreamID)
		}
		agent.streamMu.Unlock()
		agent.noteActivity()
		return nil
	}
}

func (agent *Agent) connectStartHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.ConnectStart)
		if !ok {
			return fmt.Errorf("expected *protocol.ConnectStart, got %T", payload)
		}
		addr := strings.TrimSpace(req.Addr)
		if addr == "" {
			WarnRuntime(agent.mgr, "AGENT_CONNECT_START", false, nil, "connect request missing addr")
			agent.sendConnectDone(false)
			return nil
		}
		agent.startActiveConnect(addr)
		return nil
	}
}

func parseStreamOptions(opt string) map[string]string {
	return streamopts.Parse(opt)
}
