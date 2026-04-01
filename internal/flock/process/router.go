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
	agent.router.Register(uint16(protocol.DTN_DATA), agent.dtnDataHandler())
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
		// Memo 收敛不应只依赖异步触发通道：
		// 在高频 gossip 抖动下，合并后的触发可能延迟或跳过最新的
		// memo 快照。这里立即发送一次以确保传播。
		agent.emitGossipUpdate()
		// 当立即发送无法进行时（例如会话或连接暂时不可用），
		// 仍保留周期性/异步路径作为兜底。
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
		// 将 DTN 投递视为活跃行为，避免睡眠管理器在我们处理载荷并发送 ACK 时
		// 提前断开上游连接。
		agent.noteActivity()
		clone := *data
		if data.Payload != nil {
			clone.Payload = append([]byte(nil), data.Payload...)
		}
		agent.recordDTNMessage(&clone)

		// Trace 辅助：Kelpie 需要能观察到 "log:<message>" 形式的 DTN 载荷。
		// 与其依赖单独的 RuntimeLog 消息（它在 sleep/failover 抖动中可能丢失），
		// 更适合在成功时通过 DTN_ACK.Error 回显日志内容，从而继承 ACK 的
		// 重试与 carry-forward 行为。
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
		// 优先沿原始链路（例如 supplemental 边）回复，这样即使主上游会话
		// 正在睡眠或已经关闭，响应仍然可以返回。
		var adminConn net.Conn
		if originConn != nil {
			adminConn = originConn
		} else if conn, ok, _ := agent.nextHopConn(origin, true); ok && conn != nil {
			adminConn = conn
		} else {
			adminConn = sess.Conn()
		}

		// 可选：执行最小化的业务处理。
		applyErr := agent.applyDTNPayload(&clone, adminConn)
		if applyErr != nil {
			// 保留已存储的内容用于诊断；错误信息会由 ACK 携带。
		}

		// 立即向上游返回 ACK：存储并处理成功时为 OK，收件箱已满或处理失败时为 ERR。
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
		// 如果上游链路正在抖动（sleep/kill/reconnect），先缓存 ACK，稍后重试。
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
		// sleep 更新后通常会紧跟后续控制流量（supplemental 规划、DTN flush、rescue）。
		// 如果 lastActivity 过旧，睡眠管理器可能立刻断开上游连接，导致这些消息被卡住。
		// 因此至少保持一个 work 窗口的唤醒时间。
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

// applyDTNPayload 解析最小化的 DTN 载荷约定：
// - "memo:<text>"：更新 agent memo，并触发 gossip 更新。
// - "log:<message>"：向上游发送一条 RuntimeLog（INFO）。
// 若载荷不受支持，或在日志路径中投递失败，则返回错误。
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
		// DTN memo 更新需要立刻对 Kelpie 可见。
		agent.emitGossipUpdate()
		agent.pushMemoSnapshotToAdmin(adminConn)
		// 若立即上行失败，仍要保留最终收敛能力。
		agent.triggerGossipUpdate()
		return nil
	case strings.HasPrefix(lower, "log:"):
		// "log:<message>" 载荷会通过 DTN_ACK.Error 回显（见 dtnDataHandler），
		// 因此这里不需要再额外发送 RuntimeLog。
		return nil
	case strings.HasPrefix(lower, "stream:"):
		// stream 子命令格式：<subcommand>[:kv]
		// 原型格式：stream:ping[:size=<n>][:seq=<n>]
		// 通过 RuntimeLog 向上游回复，便于 admin 看到 pong。
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
		// 解析参数（尽量宽松）。
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
		// 以 RuntimeLog（INFO）的形式回复上游，便于原型调试时观察。
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
		// proto 封装格式：<type-hex>:<payload-hex>
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
			// 为内层 "proto:*" 载荷保留原始连接。
			//
			// Stream/dataplane 帧会以 "proto:<type>:<payload>" 的形式经 DTN 传递。
			// 当它们穿过 supplemental 边时，回复也必须沿同一条链路返回；
			// 否则一旦上游会话睡眠或断开，整个流就可能被卡住。
			//
			// dispatchLocalMessage() 会把 originConn 传入 handler 上下文，
			// stream 层再利用它固定 replyConn，以保证上行可靠。
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
		// 在 DTN 重排序下，STREAM_DATA 可能先于 STREAM_OPEN 到达。
		// 这里保留已有的 rx 缓冲区与 ack 状态，避免丢掉已收到的帧。
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
		case "file-put", "file-get", "file-list":
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
				// 忽略。
			case seq <= st.rxAck:
				// 重复帧或旧帧；回 ACK 当前 rxAck。
			default:
				// 缓存乱序帧；只有空洞补齐后才推进 rxAck。
				if len(data.Payload) > 0 {
					if _, ok := st.rxBuf[seq]; !ok {
						st.rxBuf[seq] = append([]byte(nil), data.Payload...)
					}
				}
				if kind != "" {
					// 仅对真正可应用的帧回 ACK。如果 STREAM_DATA 早于 STREAM_OPEN 到达，
					// 此时 kind 未知，贸然推进 rxAck 会导致数据被永久丢弃，
					// 因为发送端一旦收到 ACK 就会停止重传。
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
		// 在执行按 kind 区分的 close handler 之前，不要先删除 agent.streams[streamID]。
		//
		// file-put/proxy 需要每个 stream 自己的 replyConn（固定到原始链路，通常是
		// supplemental 边）才能可靠地把 STREAM_CLOSE 发回上游。若先删掉 stream 状态，
		// 就会丢失 replyConn，并可能让 dataplane 上传在 sleep/repair/failover 抖动下
		// 卡在等待远端收尾的阶段。
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
