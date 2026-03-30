package process

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"
)

func (agent *Agent) startActiveConnect(addr string) {
	if agent == nil {
		return
	}
	target := strings.TrimSpace(addr)
	if target == "" {
		agent.sendConnectDone(false)
		return
	}
	go func(dst string) {
		if _, err := agent.performActiveConnect(dst); err != nil {
			WarnRuntime(agent.mgr, "AGENT_CONNECT_START", true, err, "connect to %s failed", dst)
			agent.sendConnectDone(false)
			return
		}
		agent.sendConnectDone(true)
	}(target)
}

func (agent *Agent) performActiveConnect(addr string) (string, error) {
	if agent == nil || agent.mgr == nil {
		return "", errors.New("connect: agent manager unavailable")
	}
	sess := agent.currentSession()
	if sess == nil {
		return "", errors.New("connect: upstream session unavailable")
	}
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return "", err
	}
	connClosed := true
	defer func() {
		if connClosed {
			_ = conn.Close()
		}
	}()

	token := agent.preAuthToken()
	if token == "" {
		token = agent.mgr.PreAuthToken()
	}

	if agent.mgr.TLSEnabled() {
		tlsConfig, err := transport.NewClientTLSConfig("", token)
		if err != nil {
			return "", err
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
	}

	param := new(protocol.NegParam)
	param.Conn = conn
	proto := protocol.NewDownProto(param)
	if proto == nil {
		return "", errors.New("connect: downstream protocol unavailable")
	}
	if err := proto.CNegotiate(); err != nil {
		return "", fmt.Errorf("connect: negotiate failed: %w", err)
	}
	conn = param.Conn

	if token == "" {
		token = agent.mgr.PreAuthToken()
	}
	if err := share.ActivePreAuth(conn, token); err != nil {
		return "", fmt.Errorf("connect: pre-auth failed: %w", err)
	}

	secret := sess.Secret()
	if secret == "" {
		secret = agent.mgr.ActiveSecret()
	}

	version := protocol.CurrentProtocolVersion
	flags := protocol.DefaultProtocolFlags
	if v := sess.ProtocolVersion(); v != 0 {
		version = v
		flags = sess.ProtocolFlags()
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

	downMsg := protocol.NewDownMsg(conn, secret, protocol.ADMIN_UUID)
	protocol.SetMessageMeta(downMsg, version, flags)
	protocol.ConstructMessage(downMsg, hiHeader, hiMess, false)
	downMsg.SendMessage()

	recvMsg := protocol.NewDownMsg(conn, secret, protocol.ADMIN_UUID)
	fHeader, fMessage, err := protocol.DestructMessage(recvMsg)
	if err != nil {
		return "", fmt.Errorf("connect: handshake response failed: %w", err)
	}
	if fHeader.MessageType != protocol.HI {
		return "", errors.New("connect: unexpected handshake response")
	}
	mmess, ok := fMessage.(*protocol.HIMess)
	if !ok || mmess.Greeting != "Keep slient" || mmess.IsAdmin != 0 {
		return "", errors.New("connect: invalid handshake greeting")
	}

	parentUUID := agent.UUID
	if sess.UUID() != "" {
		parentUUID = sess.UUID()
	}

	var childUUID string
	if mmess.IsReconnect == 0 {
		childIP := conn.RemoteAddr().String()
		requestID := utils.GenerateUUID()
		waitCh := agent.mgr.ListenManager.RegisterChildWaiter(requestID)
		defer agent.mgr.ListenManager.CancelChildWaiter(requestID)

		reqHeader := &protocol.Header{
			Sender:      parentUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.CHILDUUIDREQ,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		reqPayload := &protocol.ChildUUIDReq{
			ParentUUIDLen: uint16(len(parentUUID)),
			ParentUUID:    parentUUID,
			IPLen:         uint16(len(childIP)),
			IP:            childIP,
			RequestIDLen:  uint16(len(requestID)),
			RequestID:     requestID,
		}
		if msg, sessUp, ok := agent.newUpMsg(); ok {
			protocol.SetMessageMeta(msg, sessUp.ProtocolVersion(), sessUp.ProtocolFlags())
			protocol.ConstructMessage(msg, reqHeader, reqPayload, false)
			msg.SendMessage()
		}

		childUUID = <-waitCh
		if childUUID == "" {
			return "", errors.New("connect: child uuid unavailable")
		}

		neg := protocol.Negotiate(version, flags, mmess.ProtoVersion, mmess.ProtoFlags)
		if !neg.IsV1() {
			return "", fmt.Errorf("connect: unsupported protocol version %d", neg.Version)
		}
		uuidHeader := &protocol.Header{
			Sender:      protocol.ADMIN_UUID,
			Accepter:    protocol.TEMP_UUID,
			MessageType: protocol.UUID,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}
		uuidPayload := &protocol.UUIDMess{
			UUIDLen:      uint16(len(childUUID)),
			UUID:         childUUID,
			ProtoVersion: neg.Version,
			ProtoFlags:   neg.Flags,
		}
		protocol.SetMessageMeta(downMsg, neg.Version, neg.Flags)
		protocol.ConstructMessage(downMsg, uuidHeader, uuidPayload, false)
		downMsg.SendMessage()
	} else {
		childUUID = mmess.UUID
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
			UUIDLen:       uint16(len(childUUID)),
			UUID:          childUUID,
			IPLen:         uint16(len(conn.RemoteAddr().String())),
			IP:            conn.RemoteAddr().String(),
		}
		if msg, sessUp, ok := agent.newUpMsg(); ok {
			protocol.SetMessageMeta(msg, sessUp.ProtocolVersion(), sessUp.ProtocolFlags())
			protocol.ConstructMessage(msg, reheader, reMess, false)
			msg.SendMessage()
		}
	}

	if childUUID == "" {
		return "", errors.New("connect: missing child uuid")
	}
	agent.mgr.ChildrenManager.AddChild(childUUID, conn)
	agent.mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})
	connClosed = false
	return childUUID, nil
}

func (agent *Agent) sendConnectDone(ok bool) {
	if agent == nil {
		return
	}
	msg, sess, ready := agent.newUpMsg()
	if !ready {
		return
	}
	header := &protocol.Header{
		Sender:      agent.UUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.CONNECTDONE,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	ack := &protocol.ConnectDone{}
	if ok {
		ack.OK = 1
	}
	protocol.SetMessageMeta(msg, sess.ProtocolVersion(), sess.ProtocolFlags())
	protocol.ConstructMessage(msg, header, ack, false)
	msg.SendMessage()
}
