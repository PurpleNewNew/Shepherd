package process

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"

	reuseport "github.com/libp2p/go-reuseport"
)

const (
	NORMAL = iota
	IPTABLES
	SOREUSE
)

type Listen struct {
	method     int
	addr       string
	ctx        context.Context
	cancel     context.CancelFunc
	listenerID string
}

func newListen(ctx context.Context, method int, addr, listenerID string) *Listen {
	if ctx == nil {
		ctx = context.Background()
	}
	childCtx, cancel := context.WithCancel(ctx)
	return &Listen{method: method, addr: addr, ctx: childCtx, cancel: cancel, listenerID: strings.TrimSpace(listenerID)}
}

func (listen *Listen) context() context.Context {
	if listen == nil || listen.ctx == nil {
		return context.Background()
	}
	return listen.ctx
}

func (listen *Listen) cancelContext() {
	if listen == nil || listen.cancel == nil {
		return
	}
	listen.cancel()
	listen.cancel = nil
}

func (listen *Listen) registerActive(mgr *manager.Manager) func() {
	if listen == nil {
		return func() {}
	}
	if listen.listenerID == "" || listen.cancel == nil || mgr == nil || mgr.ListenManager == nil {
		return func() { listen.cancelContext() }
	}
	mgr.ListenManager.RegisterActive(listen.listenerID, listen.cancel)
	return func() {
		listen.cancelContext()
		mgr.ListenManager.ClearActive(listen.listenerID)
	}
}

func (listen *Listen) start(mgr *manager.Manager, options *initial.Options) {
	if listen.method == IPTABLES {
		if options.ReusePort == "" {
			sendListenAck(mgr, false, listen.listenerID)
			listen.cancelContext()
			return
		}
	} else if listen.method == SOREUSE {
		if options.ReuseHost == "" {
			sendListenAck(mgr, false, listen.listenerID)
			listen.cancelContext()
			return
		}
	}

	switch listen.method {
	case NORMAL:
		go listen.normalListen(mgr, options)
	case IPTABLES:
		go listen.iptablesListen(mgr, options)
	case SOREUSE:
		go listen.soReuseListen(mgr, options)
	}

}

func (listen *Listen) normalListen(mgr *manager.Manager, options *initial.Options) {
	sess := activeSession(mgr)
	sUMessage, sess, ok := newAgentUpMsg(sess)
	if !ok {
		return
	}
	senderUUID := ""
	secret := ""
	if sess != nil {
		senderUUID = sess.UUID()
		secret = sess.Secret()
	}
	if senderUUID == "" && mgr != nil {
		senderUUID = mgr.ActiveUUID()
	}
	if secret == "" && mgr != nil {
		secret = mgr.ActiveSecret()
	}
	baseSecret := ""
	if options != nil {
		baseSecret = options.BaseSecret()
	}
	if baseSecret == "" {
		baseSecret = secret
	}
	handshakeSecret := baseSecret
	sessionSecret := secret
	if sessionSecret == "" && baseSecret != "" {
		sessionSecret = handshake.SessionSecret(baseSecret, mgr != nil && mgr.TLSEnabled())
	}

	listener, err := net.Listen("tcp", listen.addr)
	if err != nil {
		sendListenAck(mgr, false, listen.listenerID)
		listen.cancelContext()
		return
	}

	defer listener.Close()
	ctx := listen.context()
	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)

	cleanup := listen.registerActive(mgr)
	defer cleanup()
	sendListenAck(mgr, true, listen.listenerID)

	setCurrentListenAddr(listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			WarnRuntime(mgr, "AGENT_LISTEN_ACCEPT", true, err, "failed to accept connection on %s", listener.Addr())
			continue
		}

		if mgr != nil && mgr.TLSEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(mgr.PreAuthToken(), options.Domain)
			if err != nil {
				WarnRuntime(mgr, "AGENT_LISTEN_TLS_CONFIG", false, err, "failed to prepare TLS for listener %s", listener.Addr())
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewDownProto(param)
		_ = proto.SNegotiate()
		conn = param.Conn

		if err := share.PassivePreAuth(conn, mgr.PreAuthToken()); err != nil {
			WarnRuntime(mgr, "AGENT_CHILD_PREAUTH", true, err, "child pre-auth failed from %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		rMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)
		if err != nil {
			if handshakeDebugEnabled() {
				optSecret := ""
				if options != nil {
					optSecret = options.Secret
				}
				logger.Infof("handshake-debug child_handshake_read_failed local=%s listen=%s remote=%s tls=%t hs(len=%d fp=%s) base(len=%d fp=%s) sess(len=%d fp=%s) opt(len=%d fp=%s)",
					strings.TrimSpace(senderUUID),
					strings.TrimSpace(listener.Addr().String()),
					strings.TrimSpace(conn.RemoteAddr().String()),
					mgr != nil && mgr.TLSEnabled(),
					len(handshakeSecret),
					secretFingerprint(handshakeSecret),
					len(baseSecret),
					secretFingerprint(baseSecret),
					len(sessionSecret),
					secretFingerprint(sessionSecret),
					len(optSecret),
					secretFingerprint(optSecret),
				)
			}
			WarnRuntime(mgr, "AGENT_CHILD_HANDSHAKE", true, err, "child handshake read failed from %s", conn.RemoteAddr())
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)

			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 0 {
				var childUUID string

				localVersion := protocol.CurrentProtocolVersion
				localFlags := protocol.DefaultProtocolFlags
				if sess != nil {
					if v := sess.ProtocolVersion(); v != 0 {
						localVersion = v
					}
					localFlags = sess.ProtocolFlags()
				}
				negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
				if !negotiation.IsV1() {
					conn.Close()
					WarnRuntime(mgr, "AGENT_LISTEN_PROTOCOL", false, nil, "child handshake uses unsupported protocol version %d", negotiation.Version)
					continue
				}

				sLMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
				protocol.SetMessageMeta(sLMessage, negotiation.Version, negotiation.Flags)

				greet := handshake.RandomGreeting(handshake.RoleAdmin)
				hiMess := &protocol.HIMess{
					GreetingLen:  uint16(len(greet)),
					Greeting:     greet,
					UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
					UUID:         protocol.ADMIN_UUID,
					IsAdmin:      1,
					IsReconnect:  0,
					ProtoVersion: negotiation.Version,
					ProtoFlags:   negotiation.Flags,
				}

				hiHeader := &protocol.Header{
					Sender:      protocol.ADMIN_UUID,
					Accepter:    protocol.TEMP_UUID,
					MessageType: protocol.HI,
					RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
					Route:       protocol.TEMP_ROUTE,
				}

				protocol.ConstructMessage(sLMessage, hiHeader, hiMess, false)
				sLMessage.SendMessage()

				parentUUID := senderUUID
				if sess != nil && sess.UUID() != "" {
					parentUUID = sess.UUID()
				}

				if mmess.IsReconnect == 0 {
					childIP := conn.RemoteAddr().String()
					requestID := utils.GenerateUUID()
					waitCh := mgr.ListenManager.RegisterChildWaiter(requestID)

					cUUIDReqHeader := &protocol.Header{
						Sender:      parentUUID,
						Accepter:    protocol.ADMIN_UUID,
						MessageType: protocol.CHILDUUIDREQ,
						RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
						Route:       protocol.TEMP_ROUTE,
					}

					cUUIDMess := &protocol.ChildUUIDReq{
						ParentUUIDLen: uint16(len(parentUUID)),
						ParentUUID:    parentUUID,
						IPLen:         uint16(len(childIP)),
						IP:            childIP,
						RequestIDLen:  uint16(len(requestID)),
						RequestID:     requestID,
					}

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, cUUIDReqHeader, cUUIDMess, false)
					sUMessage.SendMessage()

					childUUID = <-waitCh

					uuidHeader := &protocol.Header{
						Sender:      protocol.ADMIN_UUID, // 使用管理员标识
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

					uuidMessage := protocol.NewDownMsg(conn, sessionSecret, protocol.ADMIN_UUID)
					protocol.SetMessageMeta(uuidMessage, negotiation.Version, negotiation.Flags)
					protocol.ConstructMessage(uuidMessage, uuidHeader, uuidMess, false)
					uuidMessage.SendMessage()
				} else {
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

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, reheader, reMess, false)
					sUMessage.SendMessage()

					childUUID = mmess.UUID
				}

				mgr.ChildrenManager.AddChild(childUUID, conn)
				mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})

				// 保持 listener 存活，这样子节点才能重连到同一个地址和端口。
				continue
			}
		}

		WarnRuntime(mgr, "AGENT_CHILD_HANDSHAKE", false, nil, "child handshake rejected from %s (type=%d)", conn.RemoteAddr(), fHeader.MessageType)
		conn.Close()
	}
}

func (listen *Listen) iptablesListen(mgr *manager.Manager, options *initial.Options) {
	sess := activeSession(mgr)
	sUMessage, sess, ok := newAgentUpMsg(sess)
	if !ok {
		return
	}
	senderUUID := ""
	secret := ""
	if sess != nil {
		senderUUID = sess.UUID()
		secret = sess.Secret()
	}
	if senderUUID == "" && mgr != nil {
		senderUUID = mgr.ActiveUUID()
	}
	if secret == "" && mgr != nil {
		secret = mgr.ActiveSecret()
	}
	baseSecret := ""
	if options != nil {
		baseSecret = options.BaseSecret()
	}
	if baseSecret == "" {
		baseSecret = secret
	}
	handshakeSecret := baseSecret
	sessionSecret := secret
	if sessionSecret == "" && baseSecret != "" {
		sessionSecret = handshake.SessionSecret(baseSecret, mgr != nil && mgr.TLSEnabled())
	}

	// 再尝试写入复用规则
	initial.SetPortReuseRules(options.Listen, options.ReusePort)

	listenAddr := fmt.Sprintf("0.0.0.0:%s", options.Listen)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		sendListenAck(mgr, false, listen.listenerID)
		listen.cancelContext()
		return
	}

	defer listener.Close()
	ctx := listen.context()
	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)

	cleanup := listen.registerActive(mgr)
	defer cleanup()
	sendListenAck(mgr, true, listen.listenerID)
	setCurrentListenAddr(listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			WarnRuntime(mgr, "AGENT_LISTEN_IPTABLES_ACCEPT", true, err, "failed to accept connection on %s", listener.Addr())
			continue
		}

		if mgr != nil && mgr.TLSEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(mgr.PreAuthToken(), options.Domain)
			if err != nil {
				WarnRuntime(mgr, "AGENT_LISTEN_TLS_CONFIG", false, err, "failed to prepare TLS for listener %s", listener.Addr())
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewDownProto(param)
		_ = proto.SNegotiate()
		conn = param.Conn

		if err := share.PassivePreAuth(conn, mgr.PreAuthToken()); err != nil {
			conn.Close()
			continue
		}

		rMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)

			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 0 {
				var childUUID string

				localVersion := protocol.CurrentProtocolVersion
				localFlags := protocol.DefaultProtocolFlags
				if sess != nil {
					if v := sess.ProtocolVersion(); v != 0 {
						localVersion = v
					}
					localFlags = sess.ProtocolFlags()
				}
				negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
				if !negotiation.IsV1() {
					conn.Close()
					WarnRuntime(mgr, "AGENT_LISTEN_PROTOCOL", false, nil, "child handshake uses unsupported protocol version %d", negotiation.Version)
					continue
				}

				sLMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
				protocol.SetMessageMeta(sLMessage, negotiation.Version, negotiation.Flags)

				greet := handshake.RandomGreeting(handshake.RoleAdmin)
				hiMess := &protocol.HIMess{
					GreetingLen:  uint16(len(greet)),
					Greeting:     greet,
					UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
					UUID:         protocol.ADMIN_UUID,
					IsAdmin:      1,
					IsReconnect:  0,
					ProtoVersion: negotiation.Version,
					ProtoFlags:   negotiation.Flags,
				}

				hiHeader := &protocol.Header{
					Sender:      protocol.ADMIN_UUID,
					Accepter:    protocol.TEMP_UUID,
					MessageType: protocol.HI,
					RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
					Route:       protocol.TEMP_ROUTE,
				}

				protocol.ConstructMessage(sLMessage, hiHeader, hiMess, false)
				sLMessage.SendMessage()

				parentUUID := senderUUID
				if sess != nil && sess.UUID() != "" {
					parentUUID = sess.UUID()
				}

				if mmess.IsReconnect == 0 {
					childIP := conn.RemoteAddr().String()
					requestID := utils.GenerateUUID()
					waitCh := mgr.ListenManager.RegisterChildWaiter(requestID)

					cUUIDReqHeader := &protocol.Header{
						Sender:      parentUUID,
						Accepter:    protocol.ADMIN_UUID,
						MessageType: protocol.CHILDUUIDREQ,
						RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
						Route:       protocol.TEMP_ROUTE,
					}

					cUUIDMess := &protocol.ChildUUIDReq{
						ParentUUIDLen: uint16(len(parentUUID)),
						ParentUUID:    parentUUID,
						IPLen:         uint16(len(childIP)),
						IP:            childIP,
						RequestIDLen:  uint16(len(requestID)),
						RequestID:     requestID,
					}

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, cUUIDReqHeader, cUUIDMess, false)
					sUMessage.SendMessage()

					childUUID = <-waitCh
					mgr.ListenManager.CancelChildWaiter(requestID)

					uuidHeader := &protocol.Header{
						Sender:      protocol.ADMIN_UUID, // 使用管理员标识
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

					uuidMessage := protocol.NewDownMsg(conn, sessionSecret, protocol.ADMIN_UUID)
					protocol.SetMessageMeta(uuidMessage, negotiation.Version, negotiation.Flags)
					protocol.ConstructMessage(uuidMessage, uuidHeader, uuidMess, false)
					uuidMessage.SendMessage()
				} else {
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

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, reheader, reMess, false)
					sUMessage.SendMessage()

					childUUID = mmess.UUID
				}

				mgr.ChildrenManager.AddChild(childUUID, conn)
				mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})

				// 保持 listener 存活，这样子节点才能重连到同一个地址和端口。
				continue
			}
		}

		conn.Close()
	}
}

func (listen *Listen) soReuseListen(mgr *manager.Manager, options *initial.Options) {
	sess := activeSession(mgr)
	sUMessage, sess, ok := newAgentUpMsg(sess)
	if !ok {
		return
	}
	senderUUID := ""
	secret := ""
	if sess != nil {
		senderUUID = sess.UUID()
		secret = sess.Secret()
	}
	if senderUUID == "" && mgr != nil {
		senderUUID = mgr.ActiveUUID()
	}
	if secret == "" && mgr != nil {
		secret = mgr.ActiveSecret()
	}
	baseSecret := ""
	if options != nil {
		baseSecret = options.BaseSecret()
	}
	if baseSecret == "" {
		baseSecret = secret
	}
	handshakeSecret := baseSecret
	sessionSecret := secret
	if sessionSecret == "" && baseSecret != "" {
		sessionSecret = handshake.SessionSecret(baseSecret, mgr != nil && mgr.TLSEnabled())
	}

	listenAddr := fmt.Sprintf("%s:%s", options.ReuseHost, options.ReusePort)
	listener, err := reuseport.Listen("tcp", listenAddr)
	if err != nil {
		sendListenAck(mgr, false, listen.listenerID)
		listen.cancelContext()
		return
	}

	defer listener.Close()
	ctx := listen.context()
	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)

	cleanup := listen.registerActive(mgr)
	defer cleanup()
	sendListenAck(mgr, true, listen.listenerID)
	setCurrentListenAddr(listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			WarnRuntime(mgr, "AGENT_LISTEN_SOREUSE_ACCEPT", true, err, "failed to accept connection on %s", listener.Addr())
			continue
		}

		if mgr != nil && mgr.TLSEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(mgr.PreAuthToken(), options.Domain)
			if err != nil {
				WarnRuntime(mgr, "AGENT_LISTEN_TLS_CONFIG", false, err, "failed to prepare TLS for listener %s", listener.Addr())
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewDownProto(param)
		_ = proto.SNegotiate()
		conn = param.Conn

		ok, err := initial.PassivePreAuthOrProxy(conn, mgr.PreAuthToken(), options.ReusePort, 2*time.Second)
		if err != nil {
			conn.Close()
			continue
		}
		if !ok {
			// 非 Shepherd 流量已经被代理转发。
			continue
		}

		rMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)

			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 0 {
				var childUUID string

				localVersion := protocol.CurrentProtocolVersion
				localFlags := protocol.DefaultProtocolFlags
				if sess != nil {
					if v := sess.ProtocolVersion(); v != 0 {
						localVersion = v
					}
					localFlags = sess.ProtocolFlags()
				}
				negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
				if !negotiation.IsV1() {
					conn.Close()
					WarnRuntime(mgr, "AGENT_LISTEN_PROTOCOL", false, nil, "child handshake uses unsupported protocol version %d", negotiation.Version)
					continue
				}

				sLMessage := protocol.NewDownMsg(conn, handshakeSecret, protocol.ADMIN_UUID) // 使用管理员标识
				protocol.SetMessageMeta(sLMessage, negotiation.Version, negotiation.Flags)

				greet := handshake.RandomGreeting(handshake.RoleAdmin)
				hiMess := &protocol.HIMess{
					GreetingLen:  uint16(len(greet)),
					Greeting:     greet,
					UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
					UUID:         protocol.ADMIN_UUID,
					IsAdmin:      1,
					IsReconnect:  0,
					ProtoVersion: negotiation.Version,
					ProtoFlags:   negotiation.Flags,
				}

				hiHeader := &protocol.Header{
					Sender:      protocol.ADMIN_UUID,
					Accepter:    protocol.TEMP_UUID,
					MessageType: protocol.HI,
					RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
					Route:       protocol.TEMP_ROUTE,
				}

				protocol.ConstructMessage(sLMessage, hiHeader, hiMess, false)
				sLMessage.SendMessage()

				parentUUID := senderUUID
				if sess != nil && sess.UUID() != "" {
					parentUUID = sess.UUID()
				}

				if mmess.IsReconnect == 0 {
					childIP := conn.RemoteAddr().String()
					requestID := utils.GenerateUUID()
					waitCh := mgr.ListenManager.RegisterChildWaiter(requestID)

					cUUIDReqHeader := &protocol.Header{
						Sender:      parentUUID,
						Accepter:    protocol.ADMIN_UUID,
						MessageType: protocol.CHILDUUIDREQ,
						RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
						Route:       protocol.TEMP_ROUTE,
					}

					cUUIDMess := &protocol.ChildUUIDReq{
						ParentUUIDLen: uint16(len(parentUUID)),
						ParentUUID:    parentUUID,
						IPLen:         uint16(len(childIP)),
						IP:            childIP,
						RequestIDLen:  uint16(len(requestID)),
						RequestID:     requestID,
					}

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, cUUIDReqHeader, cUUIDMess, false)
					sUMessage.SendMessage()

					childUUID = <-waitCh
					mgr.ListenManager.CancelChildWaiter(requestID)

					uuidHeader := &protocol.Header{
						Sender:      protocol.ADMIN_UUID, // 使用管理员标识
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

					uuidMessage := protocol.NewDownMsg(conn, sessionSecret, protocol.ADMIN_UUID)
					protocol.SetMessageMeta(uuidMessage, negotiation.Version, negotiation.Flags)
					protocol.ConstructMessage(uuidMessage, uuidHeader, uuidMess, false)
					uuidMessage.SendMessage()
				} else {
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

					if sess != nil {
						protocol.SetMessageMeta(sUMessage, sess.ProtocolVersion(), sess.ProtocolFlags())
					}
					protocol.ConstructMessage(sUMessage, reheader, reMess, false)
					sUMessage.SendMessage()

					childUUID = mmess.UUID
				}

				mgr.ChildrenManager.AddChild(childUUID, conn)
				mgr.ChildrenManager.NotifyChild(&manager.ChildInfo{UUID: childUUID, Conn: conn})

				// 保持 listener 存活，这样子节点才能重连到同一个地址和端口。
				continue
			}
		}

		conn.Close()
	}
}

func DispatchListenMess(ctx context.Context, mgr *manager.Manager, options *initial.Options) {
	for {
		var message interface{}
		select {
		case <-ctx.Done():
			return
		case message = <-mgr.ListenManager.ListenMessChan:
		}

		switch mess := message.(type) {
		case *protocol.ListenReq:
			listen := newListen(ctx, int(mess.Method), mess.Addr, mess.ListenerID)
			go listen.start(mgr, options)
		case *protocol.ListenStop:
			ok := false
			if mgr != nil && mgr.ListenManager != nil {
				ok = mgr.ListenManager.StopActive(strings.TrimSpace(mess.ListenerID))
			}
			sendListenAck(mgr, ok, mess.ListenerID)
		case *protocol.ChildUUIDRes:
			mgr.ListenManager.DeliverChildUUID(mess.RequestID, mess.UUID)
		}
	}
}

func sendListenAck(mgr *manager.Manager, accepted bool, listenerID string) {
	listenerID = strings.TrimSpace(listenerID)
	if mgr == nil || listenerID == "" {
		return
	}
	message, sess, ok := newAgentUpMsgForManager(mgr)
	if !ok {
		return
	}
	senderUUID := ""
	if sess != nil {
		senderUUID = sess.UUID()
	}
	if senderUUID == "" {
		senderUUID = mgr.ActiveUUID()
	}
	header := &protocol.Header{
		Sender:      senderUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.LISTENRES,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}
	ack := &protocol.ListenRes{ListenerID: listenerID}
	if accepted {
		ack.OK = 1
	}
	protocol.ConstructMessage(message, header, ack, false)
	message.SendMessage()
}
