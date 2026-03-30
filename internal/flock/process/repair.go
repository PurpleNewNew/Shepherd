package process

import (
	"fmt"
	"math/rand"
	"net"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/pkg/share"
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
	// If a repair port is explicitly configured, start the repair listener even
	// in active modes. This is required for supplemental self-heal to be able to
	// dial back into nodes that normally initiate outbound sessions.
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
		logger.Warnf("repair negotiation failed: %v", err)
		_ = conn.Close()
		return
	}
	conn = param.Conn

	if err := share.PassivePreAuth(conn, preAuth); err != nil {
		logger.Warnf("repair pre-auth failed: %v", err)
		_ = conn.Close()
		return
	}

	localVersion, localFlags := agent.protocolMeta()
	if agent.upstreamTransport() != "http" {
		localFlags &^= protocol.FlagSupportChunked
	}

	activeUUID := agent.activeUUID()
	if activeUUID == "" {
		activeUUID = agent.UUID
	}

	hiTemplate := &protocol.HIMess{
		GreetingLen:  uint16(len("Keep slient")),
		Greeting:     "Keep slient",
		UUIDLen:      uint16(len(activeUUID)),
		UUID:         activeUUID,
		IsAdmin:      0,
		IsReconnect:  1,
		ProtoVersion: localVersion,
		ProtoFlags:   localFlags,
	}

	header := &protocol.Header{
		Version:     localVersion,
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
	// Treat an inbound rescue/repair connection as a critical section, otherwise the sleep
	// manager can immediately close the freshly-established upstream session if lastActivity
	// is stale. This is especially important for short-connection nodes (sleep/work) where
	// rescue is used as a control-plane lifeline during topology repair.
	cfg := agent.loadSleepConfig()
	grace := time.Duration(cfg.workSeconds) * time.Second
	// For very small work windows, we still need enough time to receive follow-up
	// control-plane traffic (gossip/route updates) and flush DTN bursts.
	if grace < 10*time.Second {
		grace = 10 * time.Second
	}
	agent.holdAwakeFor(grace)
	agent.finalizeUpstreamRecovery(agent.context())
	agent.sendMyInfo()
}
