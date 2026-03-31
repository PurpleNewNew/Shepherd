package process

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/bus"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (agent *Agent) rescueRequestHandler() bus.Handler {
	return func(ctx context.Context, header *protocol.Header, payload interface{}) error {
		req, ok := payload.(*protocol.RescueRequest)
		if !ok {
			return fmt.Errorf("expected *protocol.RescueRequest, got %T", payload)
		}
		agent.handleRescueRequest(req)
		return nil
	}
}

func (agent *Agent) handleRescueRequest(req *protocol.RescueRequest) {
	if agent == nil || req == nil {
		return
	}
	childUUID, err := agent.performRescue(req)
	if err != nil {
		agent.sendRescueResponse(req, 0, err.Error(), "", "")
		return
	}
	parentUUID := agent.UUID
	agent.sendRescueResponse(req, 1, "", parentUUID, childUUID)
}

func (agent *Agent) sendRescueResponse(req *protocol.RescueRequest, status uint16, message, parentUUID, childUUID string) {
	if agent == nil || req == nil {
		return
	}
	msg, sess, ok := agent.newUpMsg()
	if !ok {
		return
	}

	sender := agent.UUID
	if sess != nil && sess.UUID() != "" {
		sender = sess.UUID()
	}

	resp := &protocol.RescueResponse{
		TargetUUIDLen:  req.TargetUUIDLen,
		TargetUUID:     req.TargetUUID,
		RescuerUUIDLen: uint16(len(agent.UUID)),
		RescuerUUID:    agent.UUID,
		Status:         status,
		MessageLen:     uint16(len(message)),
		Message:        message,
		ParentUUIDLen:  uint16(len(parentUUID)),
		ParentUUID:     parentUUID,
		ChildUUIDLen:   uint16(len(childUUID)),
		ChildUUID:      childUUID,
	}

	header := &protocol.Header{
		Sender:      sender,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: uint16(protocol.RESCUE_RESPONSE),
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	protocol.ConstructMessage(msg, header, resp, false)
	msg.SendMessage()
}

func (agent *Agent) performRescue(req *protocol.RescueRequest) (string, error) {
	if agent == nil || agent.mgr == nil {
		return "", errors.New("rescue: agent manager unavailable")
	}
	if req.DialAddr == "" || req.DialPort == 0 {
		return "", errors.New("rescue: invalid dial target")
	}

	addr := net.JoinHostPort(req.DialAddr, strconv.Itoa(int(req.DialPort)))
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return "", fmt.Errorf("rescue: dial %s failed: %w", addr, err)
	}
	connClosed := true
	defer func() {
		if connClosed {
			_ = conn.Close()
		}
	}()

	token := req.Secret
	if token == "" && agent.store != nil {
		token = agent.store.PreAuthToken()
	}
	if token == "" {
		return "", errors.New("rescue: missing pre-auth token")
	}

	if req.Flags&protocol.RescueFlagRequireTLS != 0 {
		tlsConfig, err := transport.NewClientTLSConfig("", token)
		if err != nil {
			return "", fmt.Errorf("rescue: tls config: %w", err)
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
	}

	param := new(protocol.NegParam)
	param.Conn = conn
	proto := protocol.NewDownProto(param)
	if err := proto.CNegotiate(); err != nil {
		return "", fmt.Errorf("rescue: negotiate failed: %w", err)
	}
	conn = param.Conn

	if err := share.ActivePreAuth(conn, token); err != nil {
		return "", fmt.Errorf("rescue: pre-auth failed: %w", err)
	}

	sess := agent.currentSession()
	sessionSecret := ""
	if sess != nil {
		sessionSecret = sess.Secret()
	}
	if sessionSecret == "" && agent.mgr != nil {
		sessionSecret = agent.mgr.ActiveSecret()
	}
	baseSecret := ""
	if agent.options != nil {
		baseSecret = agent.options.BaseSecret()
	}
	handshakeSecret := baseSecret
	if handshakeSecret == "" {
		handshakeSecret = sessionSecret
	}
	if sessionSecret == "" && baseSecret != "" {
		// After a successful HI exchange, the connection switches to the derived
		// session secret (same rule as normal admin/agent handshake).
		tlsEnabled := req.Flags&protocol.RescueFlagRequireTLS != 0
		sessionSecret = share.DeriveSessionSecret(baseSecret, tlsEnabled)
	}

	version := protocol.CurrentProtocolVersion
	flags := protocol.DefaultProtocolFlags
	if sess != nil {
		if v := sess.ProtocolVersion(); v != 0 {
			version = v
			flags = sess.ProtocolFlags()
		}
	}
	if req.Transport != "http" {
		flags &^= protocol.FlagSupportChunked
	}

	hiHeader := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len("Shhh...")),
		Greeting:     "Shhh...",
		UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
		UUID:         protocol.ADMIN_UUID,
		IsAdmin:      1,
		IsReconnect:  0,
		ProtoVersion: version,
		ProtoFlags:   flags,
	}

	var downMsg protocol.Message
	if req.Transport != "" {
		downMsg = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, req.Transport)
	} else {
		downMsg = protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID)
	}
	protocol.SetMessageMeta(downMsg, version, flags)
	protocol.ConstructMessage(downMsg, hiHeader, hiMess, false)
	downMsg.SendMessage()

	var recvMsg protocol.Message
	if req.Transport != "" {
		recvMsg = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, req.Transport)
	} else {
		recvMsg = protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID)
	}

	fHeader, fMessage, err := protocol.DestructMessage(recvMsg)
	if err != nil {
		return "", fmt.Errorf("rescue: handshake response: %w", err)
	}

	if fHeader.MessageType != protocol.HI {
		return "", errors.New("rescue: unexpected handshake response")
	}
	mmess, ok := fMessage.(*protocol.HIMess)
	if !ok || !handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) || mmess.IsAdmin != 0 {
		return "", errors.New("rescue: invalid handshake greeting")
	}

	negotiation := protocol.Negotiate(version, flags, mmess.ProtoVersion, mmess.ProtoFlags)
	if !negotiation.IsV1() {
		return "", fmt.Errorf("rescue: unsupported protocol version %d", negotiation.Version)
	}

	parentUUID := agent.UUID
	if sess != nil && sess.UUID() != "" {
		parentUUID = sess.UUID()
	}

	childUUID := mmess.UUID
	_ = childUUID // mark used before potential reassignment
	if mmess.IsReconnect == 0 {
		requestID := utils.GenerateUUID()
		waitCh := agent.mgr.ListenManager.RegisterChildWaiter(requestID)
		defer agent.mgr.ListenManager.CancelChildWaiter(requestID)

		msg, sessUp, ok := agent.newUpMsg()
		if !ok {
			return "", errors.New("rescue: upstream unavailable")
		}
		if sessUp != nil {
			protocol.SetMessageMeta(msg, sessUp.ProtocolVersion(), sessUp.ProtocolFlags())
		}
		childIP := conn.RemoteAddr().String()
		reqHeader := &protocol.Header{
			Sender:      parentUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.CHILDUUIDREQ,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		reqMess := &protocol.ChildUUIDReq{
			ParentUUIDLen: uint16(len(parentUUID)),
			ParentUUID:    parentUUID,
			IPLen:         uint16(len(childIP)),
			IP:            childIP,
			RequestIDLen:  uint16(len(requestID)),
			RequestID:     requestID,
		}
		protocol.ConstructMessage(msg, reqHeader, reqMess, false)
		msg.SendMessage()

		select {
		case childUUID = <-waitCh:
		case <-time.After(5 * time.Second):
			return "", errors.New("rescue: child uuid timeout")
		}

		uuidHeader := &protocol.Header{
			Sender:      protocol.ADMIN_UUID,
			Accepter:    protocol.TEMP_UUID,
			MessageType: protocol.UUID,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		uuidMess := &protocol.UUIDMess{
			UUIDLen:      uint16(len(childUUID)),
			UUID:         childUUID,
			ProtoVersion: negotiation.Version,
			ProtoFlags:   negotiation.Flags,
		}
		uuidMsg := downMsg
		if sessionSecret != "" && sessionSecret != handshakeSecret {
			if req.Transport != "" {
				uuidMsg = protocol.NewDownMsgWithTransport(conn, sessionSecret, protocol.ADMIN_UUID, req.Transport)
			} else {
				uuidMsg = protocol.NewDownMsg(conn, sessionSecret, protocol.ADMIN_UUID)
			}
		}
		protocol.SetMessageMeta(uuidMsg, negotiation.Version, negotiation.Flags)
		protocol.ConstructMessage(uuidMsg, uuidHeader, uuidMess, false)
		uuidMsg.SendMessage()
	} else {
		// 通知 Admin 此节点重新上线
		msg, sessUp, ok := agent.newUpMsg()
		if ok {
			if sessUp != nil {
				protocol.SetMessageMeta(msg, sessUp.ProtocolVersion(), sessUp.ProtocolFlags())
			}
			reheader := &protocol.Header{
				Sender:      parentUUID,
				Accepter:    protocol.ADMIN_UUID,
				MessageType: protocol.NODEREONLINE,
				RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
				Route:       protocol.TEMP_ROUTE,
			}
			reMess := &protocol.NodeReonline{
				ParentUUIDLen: uint16(len(parentUUID)),
				ParentUUID:    parentUUID,
				UUIDLen:       uint16(len(mmess.UUID)),
				UUID:          mmess.UUID,
				IPLen:         uint16(len(conn.RemoteAddr().String())),
				IP:            conn.RemoteAddr().String(),
			}
			protocol.ConstructMessage(msg, reheader, reMess, false)
			msg.SendMessage()
		}
		childUUID = mmess.UUID
	}

	agent.mgr.ChildrenManager.AddChild(childUUID, conn)
	agent.mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})

	// Proactively "touch" the child with a direct (no-route) message from the rescuer's UUID.
	// This lets the child update its ParentUUID immediately, so subsequent sleep-triggered
	// reconnects can prefer the new parent rather than a stale options.Connect target.
	//
	// Without this, a rescued short-connection node may keep reconnecting to its old parent
	// after waking up, while Kelpie already routes DTN bundles via the new parent, causing
	// avoidable ACK timeouts and flakes in trace replay.
	secret := sessionSecret
	if secret == "" {
		secret = handshakeSecret
	}
	if secret != "" {
		hbHeader := &protocol.Header{
			Sender:      parentUUID,
			Accepter:    childUUID,
			MessageType: protocol.HEARTBEAT,
			RouteLen:    0,
			Route:       "",
		}
		hb := &protocol.HeartbeatMsg{Ping: 1}
		var hbMsg protocol.Message
		if req.Transport != "" {
			hbMsg = protocol.NewDownMsgWithTransport(conn, secret, parentUUID, req.Transport)
		} else {
			hbMsg = protocol.NewDownMsg(conn, secret, parentUUID)
		}
		protocol.SetMessageMeta(hbMsg, negotiation.Version, negotiation.Flags)
		protocol.ConstructMessage(hbMsg, hbHeader, hb, false)
		hbMsg.SendMessage()
	}

	connClosed = false
	return childUUID, nil
}
