package process

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/internal/flock/initial"
	"codeberg.org/agnoie/shepherd/internal/flock/manager"
	"codeberg.org/agnoie/shepherd/pkg/config/defaults"
	"codeberg.org/agnoie/shepherd/pkg/share"
	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
	reconn "codeberg.org/agnoie/shepherd/pkg/share/reconnect"
	"codeberg.org/agnoie/shepherd/pkg/share/transport"
	"codeberg.org/agnoie/shepherd/pkg/utils"
	"codeberg.org/agnoie/shepherd/protocol"

	reuseport "github.com/libp2p/go-reuseport"
)

const (
	manualReconnectAttempts = 5
	manualReconnectDelay    = 5 * time.Second
)

func (agent *Agent) protocolMeta() (uint16, uint16) {
	if agent != nil {
		if sess := agent.currentSession(); sess != nil {
			version := sess.ProtocolVersion()
			flags := sess.ProtocolFlags()
			if version == 0 {
				version = protocol.CurrentProtocolVersion
				if flags == 0 {
					flags = protocol.DefaultProtocolFlags
				}
			}
			return version, flags
		}
	}
	return protocol.CurrentProtocolVersion, protocol.DefaultProtocolFlags
}

func reconnectStrategy(options *initial.Options) reconn.Strategy {
	strategy := reconn.DefaultStrategy
	if options != nil && options.Reconnect > 0 {
		base := time.Duration(options.Reconnect) * time.Second
		if base > 0 {
			strategy.BaseDelay = base
			if base*8 > strategy.MaxDelay {
				strategy.MaxDelay = base * 8
			}
		}
	}
	return strategy
}

func (agent *Agent) reconHandshakeSecret(options *initial.Options) string {
	if options == nil {
		return ""
	}
	// Handshake (HI exchange) must use the original/base secret.
	// Options.Secret may be overwritten with the derived session secret after the first link,
	// which would break reconnect handshakes against listeners that decrypt HI with BaseSecret.
	if base := options.BaseSecret(); base != "" {
		return base
	}
	return options.Secret
}

func applyNegotiationResult(options *initial.Options, negotiation protocol.Negotiation) error {
	if options == nil {
		return nil
	}
	effectiveUp := strings.ToLower(strings.TrimSpace(options.Upstream))
	if effectiveUp == "" {
		effectiveUp = protocol.DefaultTransports().Upstream()
	}
	if effectiveUp == "http" && negotiation.Flags&protocol.FlagSupportChunked == 0 {
		return fmt.Errorf("upstream does not support HTTP chunked transfer")
	}
	return nil
}

func (agent *Agent) finalizePassiveHandshake(conn net.Conn, options *initial.Options, hiTemplate *protocol.HIMess, header *protocol.Header, activeUUID string) (net.Conn, error) {
	if conn == nil || options == nil || hiTemplate == nil || header == nil {
		return nil, fmt.Errorf("invalid handshake parameters")
	}

	handshakeSecret := agent.reconHandshakeSecret(options)
	transport := options.Upstream
	if strings.TrimSpace(transport) == "" {
		transport = agent.upstreamTransport()
	}
	rMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	if err != nil {
		return nil, err
	}

	if fHeader.MessageType != protocol.HI {
		return nil, fmt.Errorf("unexpected handshake response type %d", fHeader.MessageType)
	}

	mmess, ok := fMessage.(*protocol.HIMess)
	if !ok || !handshake.ValidGreeting(handshake.RoleAgent, mmess.Greeting) || mmess.IsAdmin != 1 {
		return nil, fmt.Errorf("invalid handshake greeting")
	}

	negotiation := protocol.Negotiate(hiTemplate.ProtoVersion, hiTemplate.ProtoFlags, mmess.ProtoVersion, mmess.ProtoFlags)
	if !negotiation.IsV1() {
		return nil, fmt.Errorf("incoming connection uses unsupported protocol version %d", mmess.ProtoVersion)
	}

	if err := applyNegotiationResult(options, negotiation); err != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_NEGOTIATION", true, err, "apply negotiation result failed")
		return nil, err
	}

	if agent.store != nil {
		agent.store.UpdateProtocol(activeUUID, negotiation.Version, negotiation.Flags)
	}

	resp := *hiTemplate
	resp.ProtoVersion = negotiation.Version
	resp.ProtoFlags = negotiation.Flags

	sMessage := protocol.NewUpMsgWithTransport(conn, agent.reconHandshakeSecret(options), protocol.TEMP_UUID, transport)
	protocol.SetMessageMeta(sMessage, negotiation.Version, negotiation.Flags)
	header.Version = negotiation.Version
	header.Flags = negotiation.Flags
	protocol.ConstructMessage(sMessage, header, &resp, false)
	sMessage.SendMessage()
	return conn, nil
}

func upstreamOffline(agent *Agent) {
	mgr := agent.mgr
	options := agent.options
	if mgr == nil || options == nil {
		return
	}
	// Capture the upstream connection pointer that triggered offline handling. If a
	// repair/rescue connection arrives in parallel, it will replace the active conn;
	// we must not keep blocking on sleep/reconnect against stale options.Connect.
	oldConn := func() net.Conn {
		if sess := agent.currentSession(); sess != nil {
			return sess.Conn()
		}
		return nil
	}()
	// Important: when the upstream is gone, keep the session/store from holding on to a
	// closed net.Conn pointer. Otherwise, subsystems that pick "current session conn"
	// (streams/dataplane ACKs, etc) can wedge by repeatedly writing to a dead socket,
	// even after a supplemental link has delivered requests to us.
	//
	// We only clear when the session still points to oldConn to avoid racing with a
	// concurrently-established repair/rescue connection.
	if oldConn != nil {
		if sess := agent.currentSession(); sess != nil && sess.Conn() == oldConn {
			if agent.store != nil {
				agent.store.UpdateActiveConn(nil)
			}
			agent.updateSessionConn(nil)
		}
	}
	// If our view of ParentUUID has changed (e.g. rescue/reparent), prefer reconnecting to the
	// parent's advertised gossip/listen endpoint instead of a stale static options.Connect.
	//
	// This matters for short-connection sleep nodes: they intentionally close the upstream session
	// during sleep and rely on reconnect. If Connect still points at the old parent, the node can
	// get stuck "online in topology" but unreachable in practice, leading to DTN ACK timeouts.
	if connect := agent.preferredUpstreamConnect(options); connect != "" && strings.TrimSpace(connect) != strings.TrimSpace(options.Connect) {
		clone := *options
		clone.Connect = connect
		options = &clone
	}
	ctx := agent.context()
	// If sleeping scheduled, wait until wake time
	agent.sleepMu.Lock()
	until := agent.sleepingUntil
	agent.sleepMu.Unlock()
	if !until.IsZero() {
		now := time.Now()
		if until.After(now) {
			timer := time.NewTimer(until.Sub(now))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			case <-agent.connChanged:
				timer.Stop()
				// A repair/rescue connection may have replaced the upstream session while
				// we were waiting for wake time; let the read loop pick up the new conn.
				if sess := agent.currentSession(); sess != nil && sess.Conn() != nil && sess.Conn() != oldConn {
					agent.finalizeUpstreamRecovery(ctx)
					return
				}
			}
		}
		// clear sleeping
		agent.sleepMu.Lock()
		agent.sleepingUntil = time.Time{}
		agent.sleepMu.Unlock()
	}
	if agent.tryAdoptFailoverParent(ctx) {
		agent.finalizeUpstreamRecovery(ctx)
		return
	}

	broadcastOfflineMess(ctx, mgr)

	var (
		newConn net.Conn
		err     error
	)
	switch options.Mode {
	case initial.NORMAL_PASSIVE:
		newConn, err = agent.normalPassiveReconn(agent.context(), options)
	case initial.IPTABLES_REUSE_PASSIVE:
		newConn, err = agent.ipTableReusePassiveReconn(agent.context(), options)
	case initial.SO_REUSE_PASSIVE:
		newConn, err = agent.soReusePassiveReconn(agent.context(), options)
	case initial.NORMAL_RECONNECT_ACTIVE:
		newConn = agent.normalReconnActiveReconn(agent.context(), options, nil)
	case initial.SOCKS5_PROXY_RECONNECT_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		newConn = agent.normalReconnActiveReconn(agent.context(), options, proxy)
	case initial.HTTP_PROXY_RECONNECT_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		newConn = agent.normalReconnActiveReconn(agent.context(), options, proxy)
	case initial.NORMAL_ACTIVE:
		newConn, err = agent.manualActiveReconnect(agent.context(), options, nil, manualReconnectAttempts, manualReconnectDelay)
	case initial.SOCKS5_PROXY_ACTIVE:
		proxy := share.NewSocks5Proxy(options.Connect, options.Socks5Proxy, options.Socks5ProxyU, options.Socks5ProxyP)
		newConn, err = agent.manualActiveReconnect(agent.context(), options, proxy, manualReconnectAttempts, manualReconnectDelay)
	case initial.HTTP_PROXY_ACTIVE:
		proxy := share.NewHTTPProxy(options.Connect, options.HttpProxy)
		newConn, err = agent.manualActiveReconnect(agent.context(), options, proxy, manualReconnectAttempts, manualReconnectDelay)
	default:
		err = fmt.Errorf("unsupported reconnect mode %d", options.Mode)
	}

	if err != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_RESTORE", true, err, "failed to restore upstream connection")
		return
	}
	if newConn == nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_RESTORE", true, nil, "upstream connection could not be restored")
		return
	}

	agent.setActiveConnection(newConn)

	agent.finalizeUpstreamRecovery(ctx)
}

func (agent *Agent) finalizeUpstreamRecovery(ctx context.Context) {
	if agent == nil || agent.mgr == nil {
		return
	}
	tellAdminReonline(agent.mgr)
	broadcastReonlineMess(ctx, agent.mgr)
	// Opportunistically pull DTN after reconnection
	agent.requestDTNPull(8)
}

// preferredUpstreamConnect returns an alternate connect address (host:port) based on the current
// ParentUUID and the last known gossip NodeInfo for that parent.
//
// Returns empty string when no override is available.
func (agent *Agent) preferredUpstreamConnect(options *initial.Options) string {
	if agent == nil || options == nil {
		return ""
	}
	// Only meaningful for active reconnect modes that already have a connect target.
	if strings.TrimSpace(options.Connect) == "" {
		return ""
	}
	parent := agent.ParentUUID()
	if parent == "" || parent == protocol.ADMIN_UUID {
		return ""
	}

	agent.knownMu.RLock()
	info := agent.knownNodes[parent]
	agent.knownMu.RUnlock()
	if info == nil || info.Port <= 0 {
		return ""
	}
	ip := strings.TrimSpace(info.IP)
	if ip == "" {
		return ""
	}
	return net.JoinHostPort(ip, strconv.Itoa(info.Port))
}

func (agent *Agent) normalPassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	listenAddr, _, err := utils.CheckIPPort(options.Listen)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	closeListener := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closeListener:
		}
	}()
	defer func() {
		close(closeListener)
		listener.Close()
	}()

	activeUUID := agent.activeUUID()

	version, flags := agent.protocolMeta()
	greet := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen:  uint16(len(greet)),
		Greeting:     greet,
		UUIDLen:      uint16(len(activeUUID)),
		UUID:         activeUUID,
		IsAdmin:      0,
		IsReconnect:  1,
		ProtoVersion: version,
		ProtoFlags:   flags,
	}

	var lastErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				WarnRuntime(agent.mgr, "AGENT_PASSIVE_ACCEPT", true, err, "temporary passive accept error")
				continue
			}
			lastErr = err
			break
		}

		if agent.tlsEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(agent.preAuthToken(), agent.options.Domain)
			if err != nil {
				WarnRuntime(agent.mgr, "AGENT_TLS_CONFIG", false, err, "failed to prepare TLS server config")
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			continue
		}
		conn = param.Conn

		token := agent.preAuthToken()
		if err := share.PassivePreAuth(conn, token); err != nil {
			conn.Close()
			continue
		}

		header := &protocol.Header{
			Version:     version,
			Flags:       flags,
			Sender:      activeUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.HI,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		conn, err = agent.finalizePassiveHandshake(conn, options, hiTemplate, header, activeUUID)
		if err != nil {
			conn.Close()
			continue
		}

		return conn, nil
	}

	return nil, lastErr
}

func (agent *Agent) ipTableReusePassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	return agent.normalPassiveReconn(ctx, options)
}

func (agent *Agent) soReusePassiveReconn(ctx context.Context, options *initial.Options) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	listenAddr := fmt.Sprintf("%s:%s", options.ReuseHost, options.ReusePort)

	listener, err := reuseport.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	closeListener := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closeListener:
		}
	}()
	defer func() {
		close(closeListener)
		listener.Close()
	}()

	activeUUID := agent.activeUUID()

	version, flags := agent.protocolMeta()
	greet := handshake.RandomGreeting(handshake.RoleAdmin)
	hiTemplate := &protocol.HIMess{
		GreetingLen:  uint16(len(greet)),
		Greeting:     greet,
		UUIDLen:      uint16(len(activeUUID)),
		UUID:         activeUUID,
		IsAdmin:      0,
		IsReconnect:  1,
		ProtoVersion: version,
		ProtoFlags:   flags,
	}

	var reuseLastErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				WarnRuntime(agent.mgr, "AGENT_PASSIVE_SOREUSE_ACCEPT", true, err, "temporary soReuse accept error")
				continue
			}
			reuseLastErr = err
			break
		}

		if agent.tlsEnabled() {
			var tlsConfig *tls.Config
			tlsConfig, err = transport.NewServerTLSConfig(agent.preAuthToken(), agent.options.Domain)
			if err != nil {
				WarnRuntime(agent.mgr, "AGENT_TLS_CONFIG", false, err, "failed to prepare TLS server config")
				conn.Close()
				continue
			}
			conn = transport.WrapTLSServerConn(conn, tlsConfig)
		}

		param := new(protocol.NegParam)
		param.Conn = conn
		proto := protocol.NewUpProto(param)
		if err := proto.SNegotiate(); err != nil {
			conn.Close()
			continue
		}
		conn = param.Conn

		ok, err := initial.PassivePreAuthOrProxy(conn, agent.preAuthToken(), options.ReusePort, 2*time.Second)
		if err != nil {
			conn.Close()
			continue
		}
		if !ok {
			// Non-Shepherd traffic was proxied.
			continue
		}

		header := &protocol.Header{
			Version:     version,
			Flags:       flags,
			Sender:      activeUUID,
			Accepter:    protocol.ADMIN_UUID,
			MessageType: protocol.HI,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		conn, err = agent.finalizePassiveHandshake(conn, options, hiTemplate, header, activeUUID)
		if err != nil {
			conn.Close()
			continue
		}

		return conn, nil
	}

	return nil, reuseLastErr
}

func (agent *Agent) manualActiveReconnect(ctx context.Context, options *initial.Options, proxy share.Proxy, attempts int, delay time.Duration) (net.Conn, error) {
	if options == nil {
		return nil, fmt.Errorf("nil options")
	}
	if attempts <= 0 {
		attempts = 1
	}
	if delay <= 0 {
		delay = manualReconnectDelay
	}

	strategy := reconnectStrategy(options)
	strategy.MaxAttempts = attempts
	strategy.BaseDelay = delay
	strategy.MaxDelay = delay * 8
	sched := reconn.NewScheduler(strategy)

	var lastErr error
	attemptsCh := sched.Attempts(ctx)
	failoverCh := agent.failoverConnChan
	for {
		select {
		case <-ctx.Done():
			goto done
		case candidate := <-failoverCh:
			if agent.adoptFailoverCandidate(candidate) {
				return agent.currentSession().Conn(), nil
			}
		case attempt, ok := <-attemptsCh:
			if !ok {
				goto done
			}
			conn, err := agent.reconnectOnce(ctx, options, proxy)
			if err == nil {
				return conn, nil
			}
			lastErr = err
			WarnRuntime(agent.mgr, "AGENT_RECONNECT_ACTIVE_ATTEMPT", true, err, "active reconnect attempt %d failed", attempt.Index)
		}
	}
done:
	if lastErr == nil {
		lastErr = fmt.Errorf("reconnect attempts cancelled")
	}
	return nil, lastErr
}

func (agent *Agent) reconnectOnce(ctx context.Context, options *initial.Options, proxy share.Proxy) (net.Conn, error) {
	if options == nil {
		return nil, fmt.Errorf("nil options")
	}
	sess := agent.currentSession()
	if sess == nil || sess.UUID() == "" {
		return nil, fmt.Errorf("missing global component state")
	}

	var (
		conn net.Conn
		err  error
	)

	if proxy == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		d := &net.Dialer{}
		conn, err = d.DialContext(ctx, "tcp", options.Connect)
	} else {
		// Proxy dial is not context-aware; keep it but ensure subsequent reads have deadlines
		conn, err = proxy.Dial()
	}
	if err != nil {
		return nil, err
	}

	if agent.tlsEnabled() {
		var tlsConfig *tls.Config
		tlsConfig, err = transport.NewClientTLSConfig(options.Domain, options.PreAuthToken)
		if err != nil {
			conn.Close()
			return nil, err
		}
		conn = transport.WrapTLSClientConn(conn, tlsConfig)
	}

	param := &protocol.NegParam{
		Conn:   conn,
		Domain: options.Domain,
	}
	proto := protocol.NewUpProto(param)
	if err := proto.CNegotiate(); err != nil {
		conn.Close()
		return nil, err
	}
	conn = param.Conn

	token := agent.preAuthToken()
	if err := share.ActivePreAuth(conn, token); err != nil {
		conn.Close()
		return nil, err
	}

	activeUUID := sess.UUID()
	version, flags := agent.protocolMeta()
	hiMess := &protocol.HIMess{
		GreetingLen:  uint16(len("Shhh...")),
		Greeting:     "Shhh...",
		UUIDLen:      uint16(len(activeUUID)),
		UUID:         activeUUID,
		IsAdmin:      0,
		IsReconnect:  1,
		ProtoVersion: version,
		ProtoFlags:   flags,
	}
	header := &protocol.Header{
		Sender:      activeUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.HI,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	handshakeSecret := agent.reconHandshakeSecret(options)
	transport := options.Upstream
	if strings.TrimSpace(transport) == "" {
		transport = agent.upstreamTransport()
	}
	if handshakeDebugEnabled() {
		base := ""
		if options != nil {
			base = options.BaseSecret()
		}
		logger.Infof("handshake-debug reconnectOnce uuid=%s connect=%s tls=%t transport=%s hs(len=%d fp=%s) base(len=%d fp=%s) opt(len=%d fp=%s)",
			activeUUID,
			strings.TrimSpace(options.Connect),
			agent.tlsEnabled(),
			strings.TrimSpace(transport),
			len(handshakeSecret),
			secretFingerprint(handshakeSecret),
			len(base),
			secretFingerprint(base),
			len(options.Secret),
			secretFingerprint(options.Secret),
		)
	}
	sMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	protocol.SetMessageMeta(sMessage, version, flags)
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	sMessage.SendMessage()

	_ = conn.SetReadDeadline(time.Now().Add(defaults.HandshakeReadTimeout))
	rMessage := protocol.NewUpMsgWithTransport(conn, handshakeSecret, protocol.TEMP_UUID, transport)
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}

	if fHeader.MessageType == protocol.HI {
		if mmess, ok := fMessage.(*protocol.HIMess); ok {
			if handshake.ValidGreeting(handshake.RoleAdmin, mmess.Greeting) && mmess.IsAdmin == 1 {
				negotiation := protocol.Negotiate(version, flags, mmess.ProtoVersion, mmess.ProtoFlags)
				if !negotiation.IsV1() {
					conn.Close()
					return nil, fmt.Errorf("upstream negotiation returned unsupported version %d", negotiation.Version)
				}
				if err := applyNegotiationResult(options, negotiation); err != nil {
					conn.Close()
					return nil, err
				}
				if agent.store != nil {
					agent.store.UpdateProtocol(activeUUID, negotiation.Version, negotiation.Flags)
				}
				return conn, nil
			}
		}
	}

	conn.Close()
	return nil, fmt.Errorf("unexpected handshake response")
}

func (agent *Agent) activeUUID() string {
	if agent == nil {
		return ""
	}
	if sess := agent.currentSession(); sess != nil {
		return sess.UUID()
	}
	return ""
}

func (agent *Agent) tlsEnabled() bool {
	if agent == nil || agent.store == nil {
		return false
	}
	return agent.store.TLSEnabled()
}

func (agent *Agent) setActiveConnection(conn net.Conn) {
	if agent == nil || conn == nil {
		return
	}
	// Capture the old upstream before we update the session/store. If we are switching
	// to a new upstream (repair/rescue/failover), we must force the main read loop
	// (handleDataFromUpstream) to exit the old DestructMessage() blocking read. The
	// simplest and most reliable way is to close the previous active connection.
	var oldConn net.Conn
	if sess := agent.currentSession(); sess != nil {
		oldConn = sess.Conn()
	}
	if agent.store != nil {
		agent.store.UpdateActiveConn(conn)
	}
	agent.updateSessionConn(conn)
	// Best-effort signal: allow reconnect loops to exit early when an external
	// repair/rescue connection has already updated the active session.
	if agent.connChanged != nil {
		select {
		case agent.connChanged <- struct{}{}:
		default:
		}
	}
	// If we are switching to a different underlying connection, close the old one so
	// the upstream read loop can rebind to the new session immediately.
	if oldConn != nil && oldConn != conn {
		// Avoid closing the new connection when callers pass a SafeConn wrapper or its
		// underlying raw connection interchangeably.
		sameUnderlying := false
		if sc, ok := oldConn.(*utils.SafeConn); ok && sc != nil && sc.Conn == conn {
			sameUnderlying = true
		} else if sc, ok := conn.(*utils.SafeConn); ok && sc != nil && sc.Conn == oldConn {
			sameUnderlying = true
		} else if osc, ok := oldConn.(*utils.SafeConn); ok && osc != nil {
			if nsc, ok := conn.(*utils.SafeConn); ok && nsc != nil && osc.Conn == nsc.Conn {
				sameUnderlying = true
			}
		}
		if !sameUnderlying {
			_ = oldConn.Close()
		}
	}
}

func (agent *Agent) normalReconnActiveReconn(ctx context.Context, options *initial.Options, proxy share.Proxy) net.Conn {
	strategy := reconnectStrategy(options)
	sched := reconn.NewScheduler(strategy)
	initialConn := func() net.Conn {
		if sess := agent.currentSession(); sess != nil {
			return sess.Conn()
		}
		return nil
	}()
	var lastErr error
	attemptsCh := sched.Attempts(ctx)
	failoverCh := agent.failoverConnChan
	for {
		select {
		case <-ctx.Done():
			goto done
		case <-agent.connChanged:
			// A repair/rescue connection has already replaced the active upstream session.
			if sess := agent.currentSession(); sess != nil {
				if conn := sess.Conn(); conn != nil && conn != initialConn {
					return conn
				}
			}
		case candidate := <-failoverCh:
			if agent.adoptFailoverCandidate(candidate) {
				return agent.currentSession().Conn()
			}
		case attempt, ok := <-attemptsCh:
			if !ok {
				goto done
			}
			conn, err := agent.reconnectOnce(ctx, options, proxy)
			if err == nil {
				return conn
			}
			lastErr = err
			WarnRuntime(agent.mgr, "AGENT_RECONNECT_ATTEMPT", true, err, "reconnect attempt %d failed", attempt.Index)
		}
	}
done:
	if lastErr != nil {
		WarnRuntime(agent.mgr, "AGENT_RECONNECT_EXHAUSTED", true, lastErr, "reconnect attempts exhausted")
	}
	return nil
}

func broadcastOfflineMess(ctx context.Context, mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	secret := sess.Secret()
	uuid := sess.UUID()

	children := mgr.ChildrenManager.AllChildren()
	for _, childUUID := range children {
		conn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || conn == nil {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		sMessage := protocol.NewDownMsg(conn, secret, uuid)
		protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())

		header := &protocol.Header{
			Sender:      uuid,
			Accepter:    childUUID,
			MessageType: protocol.UPSTREAMOFFLINE,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		offlineMess := &protocol.UpstreamOffline{
			OK: 1,
		}

		protocol.ConstructMessage(sMessage, header, offlineMess, false)
		// Apply a short write deadline to avoid blocking on hung children
		_ = conn.SetWriteDeadline(time.Now().Add(defaults.BroadcastWriteDeadline))
		sMessage.SendMessage()
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func broadcastReonlineMess(ctx context.Context, mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	secret := sess.Secret()
	uuid := sess.UUID()

	children := mgr.ChildrenManager.AllChildren()
	for _, childUUID := range children {
		conn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || conn == nil {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		sMessage := protocol.NewDownMsg(conn, secret, uuid)
		protocol.SetMessageMeta(sMessage, sess.ProtocolVersion(), sess.ProtocolFlags())

		header := &protocol.Header{
			Sender:      uuid,
			Accepter:    childUUID,
			MessageType: protocol.UPSTREAMREONLINE,
			RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
			Route:       protocol.TEMP_ROUTE,
		}

		reOnlineMess := &protocol.UpstreamReonline{
			OK: 1,
		}

		protocol.ConstructMessage(sMessage, header, reOnlineMess, false)
		_ = conn.SetWriteDeadline(time.Now().Add(defaults.BroadcastWriteDeadline))
		sMessage.SendMessage()
		_ = conn.SetWriteDeadline(time.Time{})
	}
}

func downStreamOffline(agent *Agent, uuid string, expected *childDispatcher, expectedConn net.Conn) {
	if agent == nil {
		return
	}
	// Child reconnect can race with stale reader teardown: if a newer dispatcher is
	// already installed for the same uuid, this offline callback belongs to an old
	// connection and must not clear the fresh child route.
	if expected != nil {
		current := agent.currentDispatcher(uuid)
		if current != expected {
			logger.Infof("[diag][child_offline] stage=skip_stale_dispatcher child=%s expected=%p current=%p conn=%s", uuid, expected, current, connEndpoints(expectedConn))
			return
		}
	}

	mgr := agent.mgr
	if mgr == nil {
		return
	}
	if expectedConn != nil && mgr.ChildrenManager != nil {
		if currentConn, ok := mgr.ChildrenManager.GetConn(uuid); ok && currentConn != nil && currentConn != expectedConn {
			if !sameBaseConn(currentConn, expectedConn) {
				logger.Infof("[diag][child_offline] stage=skip_stale_conn child=%s expected=%s current=%s", uuid, connEndpoints(expectedConn), connEndpoints(currentConn))
				return
			}
		}
	}

	logger.Infof("[diag][child_offline] stage=remove child=%s conn=%s", uuid, connEndpoints(expectedConn))
	mgr.ChildrenManager.RemoveChild(uuid)

	agent.removeNeighbor(uuid)

	sMessage, sess, ok := agent.newUpMsg()
	if !ok {
		return
	}
	senderUUID := ""
	if sess != nil {
		senderUUID = sess.UUID()
	}

	header := &protocol.Header{
		Sender:      senderUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.NODEOFFLINE,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	offlineMess := &protocol.NodeOffline{
		UUIDLen: uint16(len(uuid)),
		UUID:    uuid,
	}

	protocol.ConstructMessage(sMessage, header, offlineMess, false)
	sMessage.SendMessage()
}

func connEndpoints(conn net.Conn) string {
	if conn == nil {
		return "-"
	}
	local := "?"
	remote := "?"
	if addr := conn.LocalAddr(); addr != nil {
		local = addr.String()
	}
	if addr := conn.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	return local + "->" + remote
}

func sameBaseConn(a, b net.Conn) bool {
	ua := unwrapSafeConn(a)
	ub := unwrapSafeConn(b)
	if ua == nil || ub == nil {
		return false
	}
	return ua == ub
}

func unwrapSafeConn(conn net.Conn) net.Conn {
	if conn == nil {
		return nil
	}
	if safe, ok := conn.(*utils.SafeConn); ok && safe != nil {
		return safe.Conn
	}
	return conn
}

func tellAdminReonline(mgr *manager.Manager) {
	if mgr == nil {
		return
	}
	sess := mgr.ActiveSession()
	if sess == nil {
		return
	}
	conn := sess.Conn()
	if conn == nil {
		return
	}
	activeUUID := sess.UUID()
	sMessage := protocol.NewUpMsg(conn, sess.Secret(), activeUUID)

	reheader := &protocol.Header{
		Sender:      activeUUID,
		Accepter:    protocol.ADMIN_UUID,
		MessageType: protocol.NODEREONLINE,
		RouteLen:    uint32(len([]byte(protocol.TEMP_ROUTE))),
		Route:       protocol.TEMP_ROUTE,
	}

	for _, childUUID := range mgr.ChildrenManager.AllChildren() {
		childConn, ok := mgr.ChildrenManager.GetConn(childUUID)
		if !ok || childConn == nil {
			continue
		}

		reMess := &protocol.NodeReonline{
			ParentUUIDLen: uint16(len(activeUUID)),
			ParentUUID:    activeUUID,
			UUIDLen:       uint16(len(childUUID)),
			UUID:          childUUID,
			IPLen:         uint16(len(childConn.RemoteAddr().String())),
			IP:            childConn.RemoteAddr().String(),
		}

		protocol.ConstructMessage(sMessage, reheader, reMess, false)
		sMessage.SendMessage()
	}
}

func DispatchOfflineMess(agent *Agent) {
	ctx := agent.context()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-agent.mgr.OfflineManager.OfflineMessChan:
			switch message.(type) {
			case *protocol.UpstreamOffline:
				broadcastOfflineMess(ctx, agent.mgr)
			case *protocol.UpstreamReonline:
				agent.sendMyInfo()
				tellAdminReonline(agent.mgr)
				broadcastReonlineMess(ctx, agent.mgr)
			}
		}
	}
}
