package process

import (
	"fmt"
	"math/rand"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/protocol"
)

const (
	repairMinPort = 40000
	repairMaxPort = 60000
)

func (agent *Agent) shouldStartRepairListener() bool {
	if agent == nil || agent.options == nil {
		return false
	}
	// 如果显式配置了 repair 端口，那么即使在主动模式下也要启动 repair listener。
	// 这能让 supplemental 自愈流程反向拨入那些平时只主动发起外连的节点。
	if agent.options.RepairPort != 0 {
		return true
	}
	switch agent.options.Mode {
	case initial.NORMAL_PASSIVE, initial.SO_REUSE_PASSIVE, initial.IPTABLES_REUSE_PASSIVE:
		return true
	default:
		return false
	}
}

func (agent *Agent) startRepairListener() {
	if !agent.shouldStartRepairListener() {
		return
	}

	listener, bindAddr, port, err := agent.bindRepairListener()
	if err != nil {
		logger.Warnf("repair listener unavailable: %v", err)
		return
	}

	agent.repairListener = listener
	agent.repairBind = bindAddr
	agent.repairPort = port
	if agent.options != nil {
		agent.options.RepairPort = port
	}

	go agent.runRepairAcceptLoop(listener)
}

func (agent *Agent) bindRepairListener() (net.Listener, string, int, error) {
	if agent == nil || agent.options == nil {
		return nil, "", 0, fmt.Errorf("repair listener requires agent options")
	}
	bindIP := agent.options.RepairBind
	if bindIP == "" {
		bindIP = "0.0.0.0"
	}

	requestedPort := agent.options.RepairPort
	attempts := 0
	for {
		port := requestedPort
		if port == 0 {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			port = repairMinPort + rng.Intn(repairMaxPort-repairMinPort)
		}
		addr := fmt.Sprintf("%s:%d", bindIP, port)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			if requestedPort != 0 {
				return nil, "", 0, err
			}
			attempts++
			if attempts >= 8 {
				return nil, "", 0, err
			}
			continue
		}
		actualPort := 0
		if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok && tcpAddr != nil {
			actualPort = tcpAddr.Port
		}
		if actualPort == 0 {
			actualPort = port
		}
		return listener, bindIP, actualPort, nil
	}
}

func (agent *Agent) runRepairAcceptLoop(listener net.Listener) {
	if agent == nil || listener == nil {
		return
	}
	ctx := agent.context()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			logger.Warnf("repair listener accept failed: %v", err)
			return
		}
		go agent.handleRepairConn(conn)
	}
}

func (agent *Agent) handleRepairConn(conn net.Conn) {
	if agent == nil || conn == nil {
		if conn != nil {
			_ = conn.Close()
		}
		return
	}

	if agent.repairConnHook != nil {
		agent.repairConnHook(conn)
		return
	}

	options := agent.options
	preAuth := ""
	if agent.store != nil {
		preAuth = agent.preAuthToken()
	}
	if options == nil || preAuth == "" {
		_ = conn.Close()
		return
	}

	if agent.tlsEnabled() {
		tlsConfig, err := transport.NewServerTLSConfig(preAuth, options.Domain)
		if err != nil {
			logger.Warnf("repair TLS config failed: %v", err)
			_ = conn.Close()
			return
		}
		conn = transport.WrapTLSServerConn(conn, tlsConfig)
	}

	param := new(protocol.NegParam)
	param.Conn = conn
	param.Domain = options.Domain
	proto := protocol.NewUpProto(param)
	if err := proto.SNegotiate(); err != nil {
		logger.Warnf("repair transport handshake failed: %v", err)
		_ = conn.Close()
		return
	}
	conn = param.Conn

	if err := share.PassivePreAuth(conn, preAuth); err != nil {
		logger.Warnf("repair pre-auth failed: %v", err)
		_ = conn.Close()
		return
	}

	localFlags := agent.protocolFlags()
	if agent.upstreamTransport() != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}

	activeUUID := agent.activeUUID()
	if activeUUID == "" {
		activeUUID = agent.UUID
	}

	greet := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen: uint16(len(greet)),
		Greeting:    greet,
		UUIDLen:     uint16(len(activeUUID)),
		UUID:        activeUUID,
		IsAdmin:     0,
		IsReconnect: 1,
		ProtoFlags:  localFlags,
	}

	header := &protocol.Header{
		Flags:       localFlags,
		Sender:      activeUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	finalConn, err := agent.finalizePassiveHandshake(conn, options, hiTemplate, header, activeUUID)
	if err != nil {
		logger.Warnf("repair handshake failed: %v", err)
		_ = conn.Close()
		return
	}
	conn = finalConn

	agent.watchConn(agent.context(), conn)
	agent.setActiveConnection(conn)
	// 将入站的 rescue/repair 连接视为一个关键区。否则一旦 lastActivity 过旧，
	// 睡眠管理器可能立刻关闭刚建立好的上游会话。
	// 这对短连接节点（sleep/work）尤其重要，因为 rescue 在拓扑修复期间
	// 本身就是一条控制面生命线。
	cfg := agent.loadSleepConfig()
	grace := time.Duration(cfg.workSeconds) * time.Second
	// 即便 work 窗口非常短，也要预留足够时间接收后续控制面流量
	// （如 gossip、路由更新），并刷出一批 DTN 消息。
	if grace < 10*time.Second {
		grace = 10 * time.Second
	}
	agent.holdAwakeFor(grace)
	agent.finalizeUpstreamRecovery(agent.context())
	agent.sendMyInfo()
}
