package initial

import (
	"crypto/tls"
	"net"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/kelpie/printer"
	"codeberg.org/agnoie/shepherd/internal/kelpie/supp"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/pkg/utils/runtimeerr"
	"codeberg.org/agnoie/shepherd/protocol"
)

func dispatchUUID(conn net.Conn, secret, transport string, nego protocol.Negotiation) string {
	var sMessage protocol.Message

	uuid := utils.GenerateUUID()
	uuidMess := &protocol.UUIDMess{
		UUIDLen:      uint16(len(uuid)),
		UUID:         uuid,
		ProtoVersion: nego.Version,
		ProtoFlags:   nego.Flags,
	}

	header := &protocol.Header{
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.UUID,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	sMessage = protocol.NewDownMsgWithTransport(conn, secret, protocol.ADMIN_UUID, transport)
	protocol.SetMessageMeta(sMessage, nego.Version, nego.Flags)

	protocol.ConstructMessage(sMessage, header, uuidMess, false)
	sMessage.SendMessage()

	return uuid
}

func NormalActive(userOptions *Options, topo *topology.Topology, proxy share.Proxy) (net.Conn, *protocol.Negotiation, error) {
	var sMessage, rMessage protocol.Message

	baseSecret := userOptions.BaseSecret()
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(userOptions.Downstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, userOptions.TlsEnable)

	// NormalActive 是握手中的“客户端”一侧；即使我们是 Kelpie，
	// 对端仍然期望收到“客户端问候”（RoleAgent）。
	greet := handshake.RandomGreeting(handshake.RoleAgent)
	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len(greet)),
		Greeting:     greet,
		UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
		UUID:         protocol.ADMIN_UUID,
		IsAdmin:      1,
		IsReconnect:  0,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}

	header := &protocol.Header{
		Version:     localVersion,
		Flags:       localFlags,
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	var (
		conn net.Conn
		err  error
	)

	dialer := &net.Dialer{Timeout: defaults.HandshakeDialTimeout}
	if proxy == nil {
		conn, err = dialer.Dial("tcp", userOptions.Connect)
	} else {
		conn, err = proxy.Dial()
	}

	if err != nil {
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_DIAL_FAILED", runtimeerr.SeverityError, true, "connect %s failed", userOptions.Connect)
	}

	if userOptions.TlsEnable {
		var tlsConfig *tls.Config
		tlsConfig, err = transport.NewClientTLSConfig(userOptions.Domain, userOptions.PreAuthToken)
		if err != nil {
			conn.Close()
			return nil, nil, runtimeerr.Wrap(err, "ADMIN_TLS_CONFIG", runtimeerr.SeverityError, false, "prepare tls client")
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
	}

	param := new(protocol.NegParam)
	param.Conn = conn
	param.Domain = userOptions.Domain
	proto := protocol.NewDownProto(param)
	if err := proto.CNegotiate(); err != nil {
		conn.Close()
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_NEGOTIATE", runtimeerr.SeverityError, true, "downstream negotiate failed")
	}
	conn = param.Conn

	if err := share.ActivePreAuth(conn, userOptions.PreAuthToken); err != nil {
		conn.Close()
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_PREAUTH", runtimeerr.SeverityError, true, "pre-auth failed")
	}

	sMessage = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, userOptions.Downstream)
	protocol.SetMessageMeta(sMessage, localVersion, localFlags)

	if err := conn.SetWriteDeadline(time.Now().Add(defaults.HandshakeReadTimeout)); err != nil {
		conn.Close()
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_HANDSHAKE", runtimeerr.SeverityError, false, "set write deadline")
	}
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	sMessage.SendMessage()
	_ = conn.SetWriteDeadline(time.Time{})

	rMessage = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, userOptions.Downstream)
	if err := conn.SetReadDeadline(time.Now().Add(defaults.HandshakeReadTimeout)); err != nil {
		conn.Close()
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_HANDSHAKE", runtimeerr.SeverityError, false, "set read deadline")
	}
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	_ = conn.SetReadDeadline(time.Time{})

	if err != nil {
		addr := ""
		if conn.RemoteAddr() != nil {
			addr = conn.RemoteAddr().String()
		}
		conn.Close()
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_HANDSHAKE", runtimeerr.SeverityError, true, "fail to connect node %s", addr)
	}

	negotiation := protocol.Negotiate(localVersion, localFlags, 0, 0)
	if fHeader.MessageType == protocol.HI {
		mmess := fMessage.(*protocol.HIMess)
		negotiation = protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
		// NormalActive 是握手中的“客户端”一侧；对端会返回
		// “服务端问候”（RoleAdmin）。
		if handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) && mmess.IsAdmin == 0 {
			if !negotiation.IsV1() {
				conn.Close()
				return nil, nil, runtimeerr.New("ADMIN_PROTOCOL_VERSION", runtimeerr.SeverityError, false, "peer protocol version %d unsupported", mmess.ProtoVersion)
			}
			if strings.ToLower(userOptions.Downstream) == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
				conn.Close()
				return nil, nil, runtimeerr.New("ADMIN_HTTP_CHUNKED", runtimeerr.SeverityError, false, "peer does not support HTTP chunked transfer")
			}
			userOptions.Secret = handshake.SessionSecret(baseSecret, userOptions.TlsEnable)
			if mmess.IsReconnect == 0 {
				childUUID := dispatchUUID(conn, userOptions.Secret, userOptions.Downstream, negotiation)
				node := topology.NewNode(childUUID, conn.RemoteAddr().String())
				task := &topology.TopoTask{
					Mode:       topology.ADDNODE,
					Target:     node,
					ParentUUID: protocol.TEMP_UUID,
					IsFirst:    true,
				}
				if _, err := topo.Execute(task); err != nil {
					return nil, nil, err
				}
				if _, err := topo.Execute(&topology.TopoTask{
					Mode:         topology.ADDEDGE,
					UUID:         protocol.ADMIN_UUID,
					NeighborUUID: childUUID,
					EdgeType:     topology.TreeEdge,
				}); err != nil {
					return nil, nil, err
				}
				if _, err := topo.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
					return nil, nil, err
				}

				printer.Success("[*] Connect to node %s successfully! Node id is 0\r\n", conn.RemoteAddr().String())
				supp.PublishNodeAdded(childUUID)
				return conn, &negotiation, nil
			}

			node := topology.NewNode(mmess.UUID, conn.RemoteAddr().String())
			task := &topology.TopoTask{
				Mode:       topology.REONLINENODE,
				Target:     node,
				ParentUUID: protocol.TEMP_UUID,
				IsFirst:    true,
			}
			if _, err := topo.Execute(task); err != nil {
				return nil, nil, err
			}
			if _, err := topo.Execute(&topology.TopoTask{
				Mode:         topology.ADDEDGE,
				UUID:         protocol.ADMIN_UUID,
				NeighborUUID: mmess.UUID,
				EdgeType:     topology.TreeEdge,
			}); err != nil {
				return nil, nil, err
			}
			if _, err := topo.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
				return nil, nil, err
			}

			printer.Success("[*] Reconnected to node %s\r\n", conn.RemoteAddr().String())
			supp.PublishNodeAdded(mmess.UUID)
			return conn, &negotiation, nil
		}
	}

	conn.Close()
	return nil, nil, runtimeerr.New("ADMIN_ILLEGAL_NODE", runtimeerr.SeverityWarn, false, "target node seems illegal")
}

func NormalPassive(userOptions *Options, topo *topology.Topology) (net.Conn, *protocol.Negotiation, error) {
	listenAddr, _, err := utils.CheckIPPort(userOptions.Listen)
	if err != nil {
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_LISTEN_ADDR", runtimeerr.SeverityError, false, "invalid listen address %s", userOptions.Listen)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, runtimeerr.Wrap(err, "ADMIN_LISTEN_FAILED", runtimeerr.SeverityError, true, "listen on %s failed", listenAddr)
	}

	defer func() {
		listener.Close() // 不要忘记关闭监听器
	}()

	var sMessage, rMessage protocol.Message

	baseSecret := userOptions.BaseSecret()
	localVersion := protocol.CurrentProtocolVersion
	localFlags := protocol.DefaultProtocolFlags
	if strings.ToLower(userOptions.Downstream) != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}
	handshakeSecret := handshake.HandshakeSecret(baseSecret, userOptions.TlsEnable)

	// 打个招呼!
	greetAdmin := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen:  uint16(len(greetAdmin)),
		Greeting:     greetAdmin,
		UUIDLen:      uint16(len(protocol.ADMIN_UUID)),
		UUID:         protocol.ADMIN_UUID,
		IsAdmin:      1,
		IsReconnect:  0,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}

	header := &protocol.Header{
		Version:     localVersion,
		Flags:       localFlags,
		Sender:      protocol.ADMIN_UUID,
		Accepter:    protocol.TEMP_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			printer.Fail("[*] Error occurred: %s\r\n", err.Error())
			continue
		}

		if userOptions.TlsEnable {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(userOptions.PreAuthToken, userOptions.Domain)
			if err != nil {
				printer.Fail("[*] Error occured: %s", err.Error())
				conn.Close()
				time.Sleep(time.Second)
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewDownProto(param)
		if err := proto.SNegotiate(); err != nil {
			printer.Fail("[*] HTTP negotiate failed: %v\r\n", err)
			conn.Close()
			continue
		}
		conn = param.Conn

		if err := share.PassivePreAuth(conn, userOptions.PreAuthToken); err != nil {
			printer.Fail("[*] Error occurred: %s\r\n", err.Error())
			conn.Close()
			continue
		}

		rMessage = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, userOptions.Downstream)
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			printer.Fail("[*] Fail to set connection from %s, Error: %s\r\n", conn.RemoteAddr().String(), err.Error())
			conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.HI {
			mmess := fMessage.(*protocol.HIMess)
			if handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) && mmess.IsAdmin == 0 {
				negotiation := protocol.Negotiate(localVersion, localFlags, mmess.ProtoVersion, mmess.ProtoFlags)
				if !negotiation.IsV1() {
					conn.Close()
					printer.Fail("[*] Incoming node uses unsupported protocol version %d\r\n", mmess.ProtoVersion)
					time.Sleep(time.Second)
					continue
				}
				if strings.ToLower(userOptions.Downstream) == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
					conn.Close()
					printer.Fail("[*] Incoming node does not support HTTP chunked transfer\r\n")
					time.Sleep(time.Second)
					continue
				}

				sMessage = protocol.NewDownMsgWithTransport(conn, handshakeSecret, protocol.ADMIN_UUID, userOptions.Downstream)
				protocol.SetMessageMeta(sMessage, negotiation.Version, negotiation.Flags)
				header.Version = negotiation.Version
				header.Flags = negotiation.Flags
				responseHI := *hiTemplate
				responseHI.ProtoVersion = negotiation.Version
				responseHI.ProtoFlags = negotiation.Flags
				protocol.ConstructMessage(sMessage, header, &responseHI, false)
				sMessage.SendMessage()
				userOptions.Secret = handshake.SessionSecret(baseSecret, userOptions.TlsEnable)

				if mmess.IsReconnect == 0 {
					childUUID := dispatchUUID(conn, userOptions.Secret, userOptions.Downstream, negotiation)
					node := topology.NewNode(childUUID, conn.RemoteAddr().String())
					task := &topology.TopoTask{
						Mode:       topology.ADDNODE,
						Target:     node,
						ParentUUID: protocol.TEMP_UUID,
						IsFirst:    true,
					}
					if _, err := topo.Execute(task); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node")
					}
					if _, err := topo.Execute(&topology.TopoTask{
						Mode:         topology.ADDEDGE,
						UUID:         protocol.ADMIN_UUID,
						NeighborUUID: childUUID,
						EdgeType:     topology.TreeEdge,
					}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node edge: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node edge")
					}
					if _, err := topo.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to calculate routes: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to calculate routes")
					}

					printer.Success("[*] Connection from node %s is set up successfully! Node id is 0\r\n", conn.RemoteAddr().String())
					supp.PublishNodeAdded(childUUID)
				} else {
					node := topology.NewNode(mmess.UUID, conn.RemoteAddr().String())
					task := &topology.TopoTask{
						Mode:       topology.REONLINENODE,
						Target:     node,
						ParentUUID: protocol.TEMP_UUID,
						IsFirst:    true,
					}
					if _, err := topo.Execute(task); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node")
					}
					if _, err := topo.Execute(&topology.TopoTask{
						Mode:         topology.ADDEDGE,
						UUID:         protocol.ADMIN_UUID,
						NeighborUUID: mmess.UUID,
						EdgeType:     topology.TreeEdge,
					}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to register node edge: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to register node edge")
					}
					if _, err := topo.Execute(&topology.TopoTask{Mode: topology.CALCULATE}); err != nil {
						conn.Close()
						printer.Fail("[*] Failed to calculate routes: %v\r\n", err)
						return nil, nil, runtimeerr.Wrap(err, "ADMIN_REGISTER_NODE", runtimeerr.SeverityError, true, "failed to calculate routes")
					}

					printer.Success("[*] Connection from node %s is re-established!\r\n", conn.RemoteAddr().String())
					supp.PublishNodeAdded(mmess.UUID)
				}

				return conn, &negotiation, nil
			}
		}

		conn.Close()
		printer.Fail("[*] Incoming connection seems illegal!")
		time.Sleep(time.Second)
	}
}
